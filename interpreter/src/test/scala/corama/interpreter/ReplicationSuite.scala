package corama.interpreter

import cats.syntax.all._
import fs2.Stream
import scala.concurrent.duration._
import weaver.SimpleIOSuite
import cats.effect.IO
import corama.InstanceId
import scala.concurrent.duration.FiniteDuration
import net.gfxmonk.auditspec.Audit
import org.scalacheck.Gen
import corama.DepotId
import corama.PStateId
import cats.effect.Ref
import weaver.scalacheck.Checkers
import weaver.Expect
import cats.effect.testkit.TestControl
import weaver.Expectations
import cats.Show
import corama.Bytes
import flogger._
import weaver.scalacheck.CheckConfig

object ReplicationSuite extends SimpleIOSuite with Checkers with IOLogging {
	implicit val log: Log[IO] = Log.empty
	implicit def show[A]: Show[A] = Show.fromToString
	private val leaderConfig = LeaderConfig.default
	private val leaderId = InstanceId("leader")
	private val taskId = TaskId(1)
	private val followerId1 = InstanceId("follower-1")
	private val followerId2 = InstanceId("follower-2")

	/*
	Properties:

	If a follower dies and then returns, further writes should be propagated.
	(after some catch-up duration)

	Modeling network partitions:
	 - stream emits zero responses
	 - stream errors (transport error)
	
	TODO: How does a replica return to healthy? Perhaps we keep invoking the stream function until it works...
	*/

	val TODO = IO.pure(success)
	
	test("single batch in healthy state") {
		val sim = for {
			warmup <- Warmup.gen
			eventsToSend <- Gen.choose(1, warmup.batchSizeRemaining)
			nextBatchEvents <- Gen.listOfN(eventsToSend, Op.gen)
		} yield (warmup, nextBatchEvents)
		forall.withConfig(CheckConfig.default.copy(
			perPropertyParallelism = 1 // TODO remove
		))(sim) { case (warmup, nextEvents) =>
			ReplicationCtx.run(warmup) { ctx =>
				for {
					_ <- log.info("running events...")
					result <- ctx.run(nextEvents).attempt.timed
					audit <- ctx.audit.get
				} yield {
					expect.all(
						1 == 2
					)
				}
			}
		}
	}

	test("all appends should either be acknowledged or rejected within max.lag + max.replication.lag") {
		TODO
	}

	test("No messages should be rejected if all replicas are functioning") {
		TODO
	}

	test("if a message is rejected, there should be >1 out-of-sync replica") {
		TODO
	}

	test("if a message is rejected, there should be >1 out-of-sync replica") {
		TODO
	}

	test("when there are insufficient ISRs, messages should be rejected immediately and not sent to any replicas") {
		TODO
	}

	sealed trait ReplicationChange
	// object ReplicationChange {
	// 	case class ReplicaOffline(id: InstanceId)
	// 	case class ReplicaOnline(id: InstanceId)
	// 	case class ReplicaLag(id: InstanceId, duration: FiniteDuration)
	// }

	sealed trait Event {
		val timestamp: FiniteDuration
	}

	object Event {
		case class ReplicationSent(timestamp: FiniteDuration, id: TxId, follower: InstanceId) extends Event
		
		case class OpSent(timestamp: FiniteDuration, op: Op) extends Event
		
		// placed in pending buffer
		case class OpAccepted(timestamp: FiniteDuration, op: Op) extends Event

		case class OpCompleted(timestamp: FiniteDuration, op: Op) extends Event

		case class ReplicationAcknowledged(timestamp: FiniteDuration, id: TxId, follower: InstanceId) extends Event
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
}
