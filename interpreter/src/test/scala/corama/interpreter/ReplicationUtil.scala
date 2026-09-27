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
import weaver.Expectations
import corama.Bytes
import flogger._
import corama.interpreter.Fixtures._

object Replication extends IOLogging {
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
		case class OpAccepted(timestamp: FiniteDuration, op: Op, batch: TxId) extends OpEvent

		case class OpCompleted(timestamp: FiniteDuration, op: Op, batch: TxId) extends OpEvent

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

		private def MAX_EMIT_DELAY = 1.second
		def emitDelayGen: Gen[FiniteDuration] = Gen.choose(0L, MAX_EMIT_DELAY.toMillis).map(_.millis)
		
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
		def allHealthy = all(ReplicaState.healthy)

		def degraded: Gen[FollowerStates] = for {
			unhealthyInstance <- aFollower
			unhealthyState <- ReplicaState.unhealthy
			base <- allHealthy
		} yield base.copy(overrides = Map(unhealthyInstance -> unhealthyState))

		def healthy: Gen[FollowerStates] = Gen.oneOf(allHealthy, degraded)

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
		emitDelay: FiniteDuration,
	)

	object Warmup {
		def MAX_WARMUP_OPS = 3 // 100
		def MAX_INFLIGHT_OPS = 3 // 100
		def gen(
			statesGen: Gen[FollowerStates] = FollowerStates.healthy,
		): Gen[Warmup] = for {
			states <- statesGen
			numAcknowledged <- Gen.choose(0, MAX_WARMUP_OPS)
			acknowledgedOps <- Gen.listOfN(numAcknowledged, Op.gen)
			emitDelay <- Op.emitDelayGen
			numInFlight <- Gen.choose(0, MAX_INFLIGHT_OPS)
			inFlightOps <- Gen.listOfN(numInFlight, Op.gen)
		} yield Warmup(states, acknowledgedOps, inFlightOps, emitDelay)
	}

	private def logAndRecord(audit: Audit[IO, Event], event: Event)(implicit log: Log[IO]) = {
		log.debug(event.toString()) *> audit.record(event)
	}

	class ReplicationCtx(
		val audit: Audit[IO, Event],
		val states: Ref[IO, FollowerStates],
		val leader: TaskLeader,
		val log: Log[IO],
	) {
		given Log[IO] = log
		def runWarmup(warmup: Warmup): IO[Unit] = {
			import scala.math.Ordering.Implicits._
			for {
				_ <- run(warmup.acknowledgedOps, delay = warmup.emitDelay)
				enqueues <- enqueueAll(warmup.inFlightOps, delay = Duration.Zero)
				_ <- log.info(s"Enqueued into batches: ${enqueues.map(_._2.txId).distinct}")

				// wait for batches that have definitely been sent, either because
				// they're full, or they're prior to the latest batch
				lastTx = enqueues.lastOption.map(_._2.txId)
				_ <- enqueues.traverse { case (op, batch) =>
					IO.whenA(lastTx.exists(_ > batch.txId) || batch.writes.size >= leaderConfig.maxBatchSize) {
						log.info(s"Awaiting acknowledgement for closed batch: ${batch.txId}") *>
						batch.acknowledged.get
					}
				}
			} yield ()
		}

		def run(ops: List[Op], delay: FiniteDuration = Duration.Zero): IO[List[TxId]] = {
			enqueueAll(ops, delay).flatMap(awaitAll)
		}
		
		def setStates(newStates: FollowerStates) = states.set(newStates)

		def events = audit.get.map(Events.apply)
		
		def record(event: Event): IO[Unit] = logAndRecord(audit, event)

		def enqueueAll(ops: List[Op], delay: FiniteDuration = Duration.Zero): IO[List[(Op, OpenBatch)]] = {
			ops.traverse { op =>
				for {
					ts <- IO.monotonic
					_ <- record(Event.OpSent(ts, op))
					batch <- op match {
						case Op.Append(depot, appends) => leader.append(depot, appends)
						case Op.Update(pstate, updates) => leader.update(pstate, updates)
					}
					_ <- IO.sleep(delay)
					ts2 <- IO.monotonic
					_ <- record(Event.OpAccepted(ts2, op, batch.txId))
				} yield (op, batch)
			}
		}

		def awaitAll(batches: List[(Op, OpenBatch)]): IO[List[TxId]] = {
			batches.traverse { case (op, batch) =>
				for {
					_ <- log.info(s"Awaiting batch ack: ${batch.txId}")
					_ <- batch.acknowledged.get
					ts <- IO.monotonic
					_ <- record(Event.OpCompleted(ts, op, batch.txId))
				} yield batch.txId
			}
		}
	}

	object ReplicationCtx {
		def run(warmup: Warmup)(body: ReplicationCtx => IO[Expectations])(using log: Log[IO]): IO[Expectations] = {
			val makeResource = for {
				_ <- log.info(s"Running warmup: $warmup")
				states <- Ref[IO].of(warmup.followers)
				audit <- Audit[IO, Event]
				leaderResouce = TaskLeader.resource(taskId, leaderId, leaderConfig, followerImpls(states, audit))
			} yield leaderResouce.map(leader => new ReplicationCtx(audit, states, leader, log))

			makeResource.flatMap(_.use { ctx =>
				for {
					_ <- log.info("Running warmup")
					_ <- ctx.runWarmup(warmup)
					_ <- log.info("Warmup complete; running test")
					_ <- ctx.audit.reset
					result <- body(ctx)
					_ <- log.info("Test body complete")
				} yield result
			})
		}

		// implement the replication follower API by implementing the current follower states (at time of update)
		def followerImpls(
			ref: Ref[IO, FollowerStates],
			audit: Audit[IO, Event],
		)(implicit baseLog: Log[IO]): Map[InstanceId, Follower] = {

			def follower(id: InstanceId) = {
				val log = baseLog.addContext("follower" -> id.id)
				val impl = new Follower {
					override def follow(updates: Stream[IO, ReplicationUpdate]): Stream[IO, ReplicationAck] = {
						Stream.exec(log.info(s"Follow stream start: $id")) ++
						updates.evalMap { update =>
							for {
								states <- ref.get
								myState = states.get(id)
								ts <- IO.monotonic
								_ <- logAndRecord(audit, Event.ReplicationSent(ts, update.id, id))
								result <- myState match {
									case ReplicaState.Online(lag) => IO.sleep(lag).as(ReplicationAck(update.id))
									case ReplicaState.Offline => IO.raiseError(new RuntimeException(s"Follower $id is offline"))
								}
								// TODO: suppress if this replica is out of sync?
								_ <- logAndRecord(audit, Event.ReplicationAcknowledged(ts, update.id, id))
							} yield result
						}
					}
				}
				(id, impl)
			}
			Map(follower(followerId1), follower(followerId2))
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

		// there are some events from transactions during warmup, in the case of lag (where the leader
		// doesn't wait for a response). This is a list of the ones which _are_ for an operation
		// sent after warmup
		def transactionIdsForAcceptedOps: Set[TxId] = allOps.mapFilter {
			case op: Event.OpAccepted => Some(op.batch)
			case _ => None
		}.toSet

	}
}
