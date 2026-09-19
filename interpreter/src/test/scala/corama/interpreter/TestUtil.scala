package corama.interpreter

import cats.syntax.all._
import fs2.Stream
import scala.concurrent.duration._
import cats.effect.IO
import corama.InstanceId
import scala.concurrent.duration.FiniteDuration
import net.gfxmonk.auditspec.Audit
import org.scalacheck.Gen
import corama.DepotId
import corama.PStateId
import cats.effect.Ref
import cats.effect.testkit.TestControl
import weaver.Expectations
import corama.Bytes
import flogger._
import corama.interpreter.Fixtures._

private implicit val log: Log[IO] = Log.empty
private implicit val logOutput: LogOutput[IO] = LogOutput("TestUtil")

def expectSeq(expectations: Seq[Expectations]): Expectations = expectations.reduce(_ and _)
def expectAll(expectations: Expectations*): Expectations = expectSeq(expectations)

class Hint[A](value: A) {
	override def toString(): String = value.toString()
	def as[B](b: B) = b
}

// usage: expect(clue(hint("unrelated")).as(items).length == 1)
def hint[A](value: A) = new Hint(value)


sealed trait Event {
	val timestamp: FiniteDuration
}
sealed trait OpEvent extends Event {
	val op: Op
}

sealed trait TxEvent extends Event {
	val id: TxId
	val follower: InstanceId
}

object Event {
	case class ReplicationSent(timestamp: FiniteDuration, id: TxId, follower: InstanceId) extends TxEvent
	
	case class OpSent(timestamp: FiniteDuration, op: Op) extends OpEvent
	
	// placed in pending buffer
	case class OpAccepted(timestamp: FiniteDuration, op: Op) extends OpEvent

	case class OpCompleted(timestamp: FiniteDuration, op: Op) extends OpEvent

	case class ReplicationAcknowledged(timestamp: FiniteDuration, id: TxId, follower: InstanceId) extends TxEvent
	// case class NewIsrs(timestamp: FiniteDuration, isrs: Set[InstanceId]) extends Event
}

sealed trait ReplicaState
object ReplicaState {
	case class Online(lag: FiniteDuration) extends ReplicaState
	case object Offline extends ReplicaState

	private val maxLagMillis = leaderConfig.maxLag.toMillis

	def healthy: Gen[ReplicaState] = for {
		lag <- Gen.chooseNum(0L, maxLagMillis - 10).map(_.millis)
	} yield Online(lag)

	def unhealthy = Gen.oneOf(lagged, Gen.const(Offline))

	private def lagged: Gen[ReplicaState] = for {
		lag <- Gen.chooseNum(maxLagMillis + 1, maxLagMillis * 10).map(_.millis)
	} yield Online(lag)
}

sealed trait Op
object Op {
	case class Append(depot: DepotId, appends: Vector[DepotAppend]) extends Op {
		override def toString(): String = s"Append(${depot.value})"
	}
	case class Update(pstate: PStateId, updates: Vector[PStateUpdate]) extends Op {
		override def toString(): String = s"Update(${pstate.value})"
	}

	def gen: Gen[Op] = Gen.oneOf(genAppends, genUpdates)
	
	private def genName = Gen.stringOfN(5, Gen.hexChar)

	private def genDepotAppend: Gen[DepotAppend] = for {
		bytes <- genBytes
	} yield DepotAppend(bytes)

	private def genAppends = for {
		name <- genName
		updates <- Gen.listOfN(2, genDepotAppend)
	} yield Append(DepotId(name), updates.toVector)

	private def genPstateUpdate: Gen[PStateUpdate] = for {
		key <- genBytes
		value <- Gen.option(genBytes)
	} yield PStateUpdate(key, value)

	private def genUpdates = for {
		name <- genName
		updates <- Gen.listOfN(2, genPstateUpdate)
	} yield Update(PStateId(name), updates.toVector)

	private def genBytes: Gen[Bytes] = Gen.listOfN(4, Gen.asciiPrintableChar.map(_.toByte)).map(_.toArray)
}

case class FollowerStates(
	default: ReplicaState,
	overrides: Map[InstanceId, ReplicaState]
) {
	def get(id: InstanceId) = overrides.getOrElse(id, default)
}

val aFollower: Gen[InstanceId] = Gen.oneOf(followerId1, followerId2)

object FollowerStates {
	private def all(state: Gen[ReplicaState]) = state.map { state => FollowerStates(state, Map.empty) }
	private def allHealthy = all(ReplicaState.healthy)

	def healthy: Gen[FollowerStates] = {
		val some = for {
			unhealthyInstance <- aFollower
			unhealthyState <- ReplicaState.unhealthy
			base <- allHealthy
		} yield base.copy(overrides = Map(unhealthyInstance -> unhealthyState))
		Gen.oneOf(allHealthy, some)
	}

	def unhealthy: Gen[FollowerStates] = {
		val allUnhealthy = all(ReplicaState.unhealthy)
		val most = for {
			base <- allHealthy
			unhealthy1 <- ReplicaState.unhealthy
			unhealthy2 <- ReplicaState.unhealthy
		} yield base.copy(overrides = Map(followerId1 -> unhealthy1, followerId2 -> unhealthy2))
		Gen.oneOf(allUnhealthy, most)
	}
}

// things that happen prior to test execution
case class Warmup(
	followers: FollowerStates,
	acknowledgedOps: List[Op],
	inFlightOps: List[Op],
) {
	// TODO: this will be hard to calculate if we move to more granular size information
	def batchSizeRemaining = leaderConfig.maxBatchSize - inFlightOps.length
}

object Warmup {
	def MAX_WARMUP_OPS = 3 // 100
	def gen: Gen[Warmup] = for {
		states <- FollowerStates.healthy
		numAcknowledged <- Gen.choose(0, MAX_WARMUP_OPS)
		acknowledgedOps <- Gen.listOfN(numAcknowledged, Op.gen)

		numInFlight <- Gen.choose(0, leaderConfig.maxBatchSize - 1)
		inFlightOps <- Gen.listOfN(numInFlight, Op.gen)
	} yield Warmup(states, acknowledgedOps, inFlightOps)
}

class ReplicationCtx(
	val audit: Audit[IO, Event],
	val states: Ref[IO, FollowerStates],
	val leader: TaskLeader,
) {
	def runWarmup(warmup: Warmup): IO[Unit] = {
		run(warmup.acknowledgedOps)
	}

	def run(ops: List[Op]): IO[Unit] = {
		enqueueAll(ops).flatMap(awaitAll)
	}

	def events = audit.get.map(Events.apply)

	private def enqueueAll(ops: List[Op]): IO[List[IO[Op]]] = {
		ops.traverse { op =>
			for {
				ts <- IO.monotonic
				_ <- log.debug(s"Sending op: $op")
				_ <- audit.record(Event.OpSent(ts, op))
				result <- op match {
					case Op.Append(depot, appends) => leader.append(depot, appends)
					case Op.Update(pstate, updates) => leader.update(pstate, updates)
				}
				ts2 <- IO.monotonic
				_ <- log.debug(s"Accepted op: $op")
				_ <- audit.record(Event.OpAccepted(ts2, op))
			} yield result.get.as(op)
		}
	}

	private def awaitAll(deferreds: List[IO[Op]]): IO[Unit] = {
		deferreds.traverse_ { acked =>
			for {
				op <- acked
				ts <- IO.monotonic
				_ <- log.debug(s"Completed op: $op")
				_ <- audit.record(Event.OpCompleted(ts, op))
			} yield ()
		}
	}
}

// implement the replication follower API by implementing the current follower states (at time of update)
def followerImpls(
	ref: Ref[IO, FollowerStates],
	audit: Audit[IO, Event],
): Map[InstanceId, Follower] = {

	def follower(id: InstanceId) = {
		val log = ReplicationSuite.log.addContext("follower" -> id.id)
		val impl = new Follower {
			override def follow(updates: Stream[IO, ReplicationUpdate]): Stream[IO, ReplicationAck] = {
				Stream.exec(log.info(s"Follow stream start: $id")) ++
				updates.evalMap { update =>
					for {
						states <- ref.get
						myState = states.get(id)
						ts <- IO.monotonic
						_ <- audit.record(Event.ReplicationSent(ts, update.id, id))
						_ <- log.debug(s"Replication sent[$id]: ${update.id}, state $myState")
						result <- myState match {
							case ReplicaState.Online(lag) => IO.sleep(lag).as(ReplicationAck(update.id))
							case ReplicaState.Offline => IO.raiseError(new RuntimeException(s"Follower $id is offline"))
						}
						_ <- audit.record(Event.ReplicationAcknowledged(ts, update.id, id))
						_ <- log.debug(s"Replication acknowledged[$id]: ${update.id}")
					} yield result
				}
			}
		}
		(id, impl)
	}
	Map(follower(followerId1), follower(followerId2))
}

object ReplicationCtx {
	def run(warmup: Warmup)(body: ReplicationCtx => IO[Expectations]): Expectations = {
		import cats.effect.unsafe.implicits.global
		val makeResource = for {
			_ <- log.info(s"-- starting test case: $warmup")
			states <- Ref[IO].of(warmup.followers)
			audit <- Audit[IO, Event]
			leaderResouce = TaskLeader.resource(taskId, leaderId, leaderConfig, followerImpls(states, audit))
		} yield leaderResouce.map(leader => new ReplicationCtx(audit, states, leader))

		val fullTest = makeResource.flatMap(_.use { ctx =>
			for {
				_ <- log.info("Running warmup")
				_ <- ctx.runWarmup(warmup)
				_ <- log.info("Warmup complete; running test")
				_ <- ctx.audit.reset
				result <- body(ctx)
				_ <- log.info("Test body complete")
			} yield result
		})

		TestControl.executeEmbed(fullTest).unsafeRunSync()
	}
}

case class Events(e: List[Event]) {
	def allOps = e.mapFilter {
		case ev: OpEvent => Some(ev)
		case _ => None
	}
	def ops = allOps.groupBy(_.op)

	def allTransactions = e.mapFilter {
		case ev: TxEvent => Some(ev)
		case _ => None
	}
	def transactions = allTransactions.groupBy(_.id).view.mapValues(_.groupBy(_.follower)).toMap
}
