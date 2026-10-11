package corama.interpreter

import fs2.Stream
import cats.effect.{IO, Concurrent}
import cats.effect.{Deferred, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import corama.error.Util.neverReached
import scala.concurrent.duration.*
import corama._
import cats.effect.Async
import flogger.Log
import flogger.IOLogging
import cats.Eq
import corama.error.Util.err

case class TxId(value: Long)
object TxId {
	implicit val ord: Ordering[TxId] = Ordering.by(_.value)
	implicit val eq: Eq[TxId] = Eq.by(_.value)
}

// There are `n` tasks in an `n` node setup.
// Each task is also replicated to 3 nodes (leader + 2)
case class TaskId(id: Int)
object TaskId {
	implicit val eq: Eq[TaskId] = Eq.fromUniversalEquals
}

case class PStateUpdate(key: Bytes, value: Option[Bytes])
case class PStateUpdates(id: PStateId, updates: Vector[PStateUpdate])

case class DepotAppend(value: Bytes)
case class DepotAppends(id: DepotId, updates: Vector[DepotAppend])

case class ReplicationUpdate(
	id: TxId,
	depotAppends: List[DepotAppends],
	pstateUpdates: List[PStateUpdates],
)

case class ReplicationAck(
	id: TxId,
)

class ReplicationChannel(
	queue: Queue[IO, ReplicationUpdate],
	waiters: Ref[IO, Map[TxId, Deferred[IO, ReplicationAck]]],
	follower: Follower,
)(using log: Log[IO]) extends IOLogging {
	def send(update: ReplicationUpdate): IO[ReplicationAck] = Deferred[IO, ReplicationAck].flatMap { d =>
		waiters.update(_.updated(update.id, d)) *>
			queue.offer(update) *>
			d.get.guarantee(waiters.update(_.removed(update.id)).void)
	}

	def ackLoop: IO[Nothing] = {
		// TODO: handle reconnection
		singleAckLoop.handleErrorWith { e =>
			log.warn("Follower failed; TODO reconnect", e) *> IO.never
		}
	}

	private def singleAckLoop: IO[Nothing] = {
		val ackStream = Stream.exec(log.info("Opening replication channel")) ++
			follower.follow(Stream.fromQueueUnterminated(queue)).evalTap { ack =>
				// route each ack to the send() waiting on its txId
				waiters.get.flatMap(_.get(ack.id).fold(IO.unit)(_.complete(ack).void))
			}
		ackStream.compile.drain >> neverReached
	}
}

object ReplicationChannel extends IOLogging {
	def open(follower: Follower, config: LeaderConfig)(using Concurrent[IO])(using log: Log[IO]): ActiveResource[IO, ReplicationChannel] =
		ActiveResource(for {
			queue <- Resource.eval(Queue.bounded[IO, ReplicationUpdate](config.maxInFlight))
			waiters <- Resource.eval(Ref.of[IO, Map[TxId, Deferred[IO, ReplicationAck]]](Map.empty))
			// run the ack stream for the channel's lifetime; cancel on release
			channel = new ReplicationChannel(queue, waiters, follower)
		} yield (channel, channel.ackLoop))
}

// A Resource[IO,A] with a background IO[Nothing]. When used, the background
// task is run concurrently with the foreground, and errors / cancelation propagate.
case class ActiveResource[F[_], A](resource: Resource[F, (A, F[Nothing])])(implicit F: Async[F]) {
	def use[B](action: A => F[B]): F[B] = {
		resource.use { case (res, task) =>
			F.race(action(res), task).map(_.merge)
		}
	}

	def map[B](f: A => B): ActiveResource[F, B] = ActiveResource(resource.map { case (a, task) => (f(a), task) })
}
// TODO: build algebras for activeresource

case class LeaderConfig(
	maxBatchSize: Int, // currently an update count, but maybe a byte size eventually
	maxBatchDelay: FiniteDuration, // delay from bach creation
	maxLag: FiniteDuration, // when to consider a replica in-sync
	maxInFlight: Int, // in flight batches to each replica, only matters when we implement pipelining
	minISR: Int, // min in-sync replicas (including leader)
)
object LeaderConfig {
	val default = LeaderConfig(
		maxBatchSize = 10,
		maxBatchDelay = 50.millis,
		maxLag = 2.seconds,
		maxInFlight = 1,
		minISR = 2,
	)
}

case class TaskStatus(
	isf: Set[InstanceId],
	followers: Set[InstanceId],
	highWatermark: TxId,
)

class ReplicationStalled(val txId: TxId)
	extends RuntimeException(s"replication stalled (below minISR) at tx $txId")

// /** Commands into the single batcher fiber. One fiber owns the current batch, so
// 	*  there are no races between appends joining a batch and the batch being
// 	*  sealed — the Kafka-producer accumulator model. */
sealed trait BatchCommand
private object BatchCommand {
	sealed trait Op extends BatchCommand {
		val accepted: Deferred[IO, OpenBatch]
	}
	case class Append(depot: DepotId, appends: Vector[DepotAppend], accepted: Deferred[IO, OpenBatch]) extends Op
	case class Update(pstate: PStateId, updates: Vector[PStateUpdate], accepted: Deferred[IO, OpenBatch]) extends Op

	case class Flush(batch: OpenBatch) extends BatchCommand
}

/** The leader for one task. Appends and pstate updates go into a single
	*  in-flight batch; a dedicated batcher fiber flushes on count-OR-linger,
	*  replicates to followers, and completes waiters.
	*/
final class TaskLeader private (
	val id: InstanceId,
	config: LeaderConfig,
	allocator: TransactionAllocator,
	cmdQueue: Queue[IO, BatchCommand],
	channels: Map[InstanceId, ReplicationChannel],
	val taskStatus: Ref[IO, TaskStatus],
)(using log: Log[IO]) extends IOLogging {
	// TODO: we could reuse a single deferred per batch instead of per op, but that might increase contention
	def append(depot: DepotId, appends: Vector[DepotAppend]): IO[OpenBatch] =
		Deferred[IO, OpenBatch].flatMap { w => cmdQueue.offer(BatchCommand.Append(depot, appends, w)) *> w.get }

	def update(pstate: PStateId, updates: Vector[PStateUpdate]): IO[OpenBatch] =
		Deferred[IO, OpenBatch].flatMap { w => cmdQueue.offer(BatchCommand.Update(pstate, updates, w)) *> w.get }

	def replicationLoop: IO[Nothing] = {
		def newBatchLoop: IO[Nothing] = {
			cmdQueue.take.flatMap {
				case cmd: BatchCommand.Op => {
					// only allocate once we have the first command in the batch
					// (batch.openedAt should reflect the time of first append)
					allocator.createBatch.flatMap { batch =>
						extendBatchLoop(batch, cmd)
					}
				}

				// nothing to flush; ignore
				case _: BatchCommand.Flush => newBatchLoop
			}
		}
		
		def flushAndContinue(batch: OpenBatch) = flush(batch) >> newBatchLoop

		def extendBatchLoop(prevBatch: OpenBatch, cmd: BatchCommand.Op): IO[Nothing] = {
			// Add a command to a batch and either flush immediately or keep accumulating
			val newBatch = prevBatch.update(cmd)
			cmd.accepted.complete(newBatch) *> (
				if (newBatch.writes.size >= config.maxBatchSize) {
					flushAndContinue(newBatch)
				} else {
					waitingBatchLoop(newBatch)
				}
			)
		}

		def waitingBatchLoop(batch: OpenBatch): IO[Nothing] = {
			// We have a nonempty batch. Wait until timeout or new command
			IO.monotonic.flatMap { now =>
				val elapsed = now - batch.openedAt
				if (elapsed >= config.maxBatchDelay) then {
					flushAndContinue(batch)
				} else {
					IO.race(
						cmdQueue.take,

						// Note: if the timeout cancels a `take`, the taken item can be lost.
						// (https://github.com/typelevel/cats-effect/issues/4571)
						// We avoid this by explicitly pushing a flush command after timeout, so the `take` is never canceled.
						// Note: the batch may be published prematurely in this case, if the offer succeeds but it appears on the
						// queue after another item.
						IO.sleep(config.maxBatchDelay - elapsed) *> cmdQueue.offer(BatchCommand.Flush(batch)) *> IO.never

					).flatMap {
						case Left(BatchCommand.Flush(batchToFlush)) => {
							if (batch `eq` batchToFlush) {
								// strict reference equality, if we've extended the batch we don't want to have
								// to compare full contents to see it differs.
								flushAndContinue(batch)
							} else {
								// Flush command for some other tx; ignore and resume
								// TODO: does this ever happen?
								waitingBatchLoop(batch)
							}
						}
						case Left(cmd: BatchCommand.Op) => extendBatchLoop(batch, cmd)
						case Right(_) => IO.raiseError(err("Impossible; race returned Right(...)"))
					}
				}
			}
		}

		newBatchLoop
	}

	private def flush(b: OpenBatch): IO[Unit] = {
		val writes = b.writes
		val update = ReplicationUpdate(
			b.txId,
			writes.depotAppends.toList.map { case (id, v) => DepotAppends(id, v) },
			writes.pstateUpdates.toList.map { case (id, v) => PStateUpdates(id, v) },
		)
		for {
			_ <- log.debug(s"Flushing batch ${b.txId} of size ${b.writes.size}")
			// (id, acked?) — maxLag-bounded; timeout or transport error => not acked
			results <- channels.toList.parTraverse { case (followerId, ch) =>
				ch.send(update).as(followerId -> true)
					.timeout(config.maxLag)
					.handleErrorWith { e =>
						log.warn(s"Follower $followerId didn't acknowledge: $e").as(followerId -> false)
					}
			}
			ackedFollowers = results.mapFilter { case (followerId, success) => Option.when(success)(followerId) }
			_ <- log.debug(s"batch ${b.txId} acknowledged by ${ackedFollowers.size} followers")
			enoughAcks = ackedFollowers.size >= config.minISR - 1
			_ <- taskStatus.update { status =>
						val newHw = if enoughAcks then b.txId else status.highWatermark
						status.copy(
							isf = ackedFollowers.toSet,
							highWatermark = newHw,
						)
					}
			_ <- if (enoughAcks) {
					log.debug(s"advanced HW to ${b.txId}") *>
					b.acknowledged.complete(())
				} else {
					val err = ReplicationStalled(b.txId) // TODO report OOS replicas
					IO.raiseError(err)
				}
		} yield ()
	}
}

object TaskLeader extends IOLogging {
	// A leader resource including the running replication loop for each follower
	def resource(
		taskId: TaskId,
		instanceId: InstanceId,
		config: LeaderConfig,
		followers: Map[InstanceId, Follower],
	)(using Concurrent[IO])(using rootLog: Log[IO]): ActiveResource[IO, TaskLeader] = {
		given log: Log[IO] = rootLog.addContext("task" -> taskId.id.toString)
		ActiveResource(for {
			allocator <- TransactionAllocator.resource
			cmdQueue <- Resource.eval(Queue.bounded[IO, BatchCommand](config.maxBatchSize))
			// res[io, (task, Map[...])
			channelPairs <- followers.toList.traverse { case (followerId, follower) =>
				given Log[IO] = log.addContext("follower" -> followerId.id)
				ReplicationChannel.open(follower, config).resource.map { case (channel, bg) =>
					followerId -> (channel, bg)
				}
			}.map(_.toMap)
			followerIds = followers.keys.toSet.incl(instanceId)
			taskStatusRef <- Resource.eval(Ref[IO].of(TaskStatus(isf = followerIds, followers = followerIds, highWatermark = TxId(0L))))
			hw <- Resource.eval(Ref[IO].of(TxId(0L)))

			// channels are given to TaskLeader so it can offer items, whereas
			// the individual channel background tasks (performing the actual GRPC streaming)
			// are not exposed outside of this resource
			channels = channelPairs.view.mapValues(_._1).toMap
			channelSendTask = channelPairs.values.map(_._2).toList.parTraverse_(identity) >> neverReached
			leader = new TaskLeader(instanceId, config, allocator, cmdQueue, channels, taskStatusRef)
			background = {
				log.info("Starting leader task") *>
				IO.race(leader.replicationLoop, channelSendTask).map(_.merge)
			}
		} yield (leader, background))
	}
}

case class BatchWrites(
	depotAppends: Map[DepotId, Vector[DepotAppend]],
	pstateUpdates: Map[PStateId, Vector[PStateUpdate]],
	size: Int,
) {
	def update(cmd: BatchCommand.Op): BatchWrites = cmd match {
		case BatchCommand.Append(depot, appends, w) => copy(
			depotAppends = depotAppends.updatedWith(depot)(v => Some(v.getOrElse(Vector.empty) ++ appends)),
			size = size + appends.size
		)

		case BatchCommand.Update(pstate, updates, w) => copy(
			pstateUpdates = pstateUpdates.updatedWith(pstate)(v => Some(v.getOrElse(Vector.empty) ++ updates)),
			size = size + updates.size
		)
	}
}

object BatchWrites {
	val empty = BatchWrites(Map.empty, Map.empty, 0)
}

case class OpenBatch(
	txId: TxId,
	openedAt: FiniteDuration,
	acknowledged: Deferred[IO, Unit],
	writes: BatchWrites,
) {

	def update(cmd: BatchCommand.Op): OpenBatch = copy(writes = writes.update(cmd))
}

class TransactionAllocator(nextId: Ref[IO, Long]):
	def createBatch: IO[OpenBatch] = for
		id <- nextId.updateAndGet(_ + 1).map(TxId.apply)
		openedAt <- IO.monotonic
		completion <- Deferred[IO, Unit]
	yield OpenBatch(id, openedAt, completion, BatchWrites.empty)

object TransactionAllocator {
	def resource: Resource[IO, TransactionAllocator] =
		Resource.eval(Ref.of[IO, Long](0L).map(new TransactionAllocator(_)))
}

