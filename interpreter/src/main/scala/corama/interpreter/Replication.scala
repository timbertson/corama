package corama.interpreter

import fs2.Stream
import cats.effect.{IO, Concurrent}
import cats.effect.{Deferred, Ref, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import scala.concurrent.duration.*
import corama._
import cats.effect.Async

case class TxId(value: Long)

// There are `n` tasks in an `n` node setup.
// Each task is also replicated to 3 nodes (leader + 2)
case class TaskId(value: Int)

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

trait Follower {
	val id: InstanceId
	def follow(updates: Stream[IO, ReplicationUpdate]): Stream[IO, ReplicationAck]
}

/** The leader's handle on one follower: a long-lived channel exposing a
	*  per-batch request/response.
	*/
trait ReplicationChannel {
	def send(update: ReplicationUpdate): IO[ReplicationAck]
}

private def neverReached: IO[Nothing] =
	IO.raiseError(new IllegalStateException("neverReached: this should be impossible"))

object ReplicationChannel {

	def open(follower: Follower, config: LeaderConfig)(using Concurrent[IO]): ActiveResource[IO, ReplicationChannel] =
		ActiveResource(for {
			queue <- Resource.eval(Queue.bounded[IO, ReplicationUpdate](config.maxInFlight))
			waiters <- Resource.eval(Ref.of[IO, Map[TxId, Deferred[IO, ReplicationAck]]](Map.empty))
			ackStream = follower.follow(Stream.fromQueueUnterminated(queue)).evalTap { ack =>
				// route each ack to the send() waiting on its txId
				waiters.get.flatMap(_.get(ack.id).fold(IO.unit)(_.complete(ack).void))
			}

			// run the ack stream for the channel's lifetime; cancel on release
			ackLoop = ackStream.compile.drain >> neverReached
			channel = new ReplicationChannel {
				def id: InstanceId = follower.id
				def send(u: ReplicationUpdate): IO[ReplicationAck] = Deferred[IO, ReplicationAck].flatMap { d =>
					waiters.update(_.updated(u.id, d)) *>
						queue.offer(u) *>
						d.get.guarantee(waiters.update(_.removed(u.id)).void)
				}
			}
		} yield (channel, ackLoop))
}

// A Resource[IO,A] with a background IO[Nothing]. When used, the background
// task is run concurrently with the foreground, and errors / cancelation propagate.
case class ActiveResource[F[_], A](resource: Resource[F, (A, F[Nothing])])(implicit F: Async[F]) {
	def use[B](action: A => F[B]): F[B] = {
		resource.use { case (res, task) =>
			F.race(action(res), task).map(_.merge)
		}
	}
}

case class LeaderConfig(
	maxBatchSize: Int, // currently an update count, but maybe a byte size eventually
	maxBatchDelay: FiniteDuration, // delay from bach creation
	maxLag: FiniteDuration, // when to consider a replica in-sync
	maxInFlight: Int, // in flight batches to each replica, only matters when we implement pipelining
	minISR: Int, // min in-sync replicas (including leader)
)

case class TaskStatus(
	taskId: TaskId,
	isr: List[InstanceId],
	replicas: List[InstanceId],
)

class ReplicationStalled(val txId: TxId)
	extends RuntimeException(s"replication stalled (below minISR) at tx $txId")

// /** Commands into the single batcher fiber. One fiber owns the current batch, so
// 	*  there are no races between appends joining a batch and the batch being
// 	*  sealed — the Kafka-producer accumulator model. */
private enum BatchCommand {
	case AppendCmd(depot: DepotId, appends: Vector[DepotAppend], waiter: Deferred[IO, Unit])
	case UpdateCmd(pstate: PStateId, updates: Vector[PStateUpdate], waiter: Deferred[IO, Unit])
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
	highWatermark: Ref[IO, TxId],
):
	// TODO: we could reuse a single deferred per batch instead of per op
	def append(depot: DepotId, appends: Vector[DepotAppend]): IO[Unit] =
	Deferred[IO, Unit].flatMap { w => cmdQueue.offer(BatchCommand.AppendCmd(depot, appends, w)) *> w.get }

	def update(pstate: PStateId, updates: Vector[PStateUpdate]): IO[Unit] =
	Deferred[IO, Unit].flatMap { w => cmdQueue.offer(BatchCommand.UpdateCmd(pstate, updates, w)) *> w.get }

	def replicationLoop: IO[Nothing] = {
		def newBatchLoop: IO[Nothing] = {
			cmdQueue.take.flatMap { cmd =>
				// only allocate once we have the first command in the batch
				// (batch.openedAt should reflect the time of first append)
				allocator.createBatch.flatMap { batch =>
					continueBatchLoop(batch, cmd)
				}
			}
		}

		def continueBatchLoop(batch: OpenBatch, cmd: BatchCommand): IO[Nothing] = {
			// Add a command to a batch and either flush immediately or keep accumulating
			batch.update(cmd).flatMap { newWrites =>
				if (newWrites.size >= config.maxBatchSize) {
					flush(batch) >> newBatchLoop
				} else {
					waitingBatchLoop(batch)
				}
			}
		}

		def waitingBatchLoop(batch: OpenBatch): IO[Nothing] = {
			// We have a nonempty batch. Wait until timeout or new command
			IO.monotonic.flatMap { now =>
				val elapsed = now - batch.openedAt
				if (elapsed >= config.maxBatchDelay) then {
					flush(batch) >> newBatchLoop
				} else {
					IO.race(
						cmdQueue.take,
						IO.sleep(config.maxBatchDelay - elapsed)
					).flatMap {
						case Left(cmd) => continueBatchLoop(batch, cmd)
						case Right(timeout) => flush(batch) >> newBatchLoop
					}
				}
			}
		}

		newBatchLoop
	}

	private def flush(b: OpenBatch): IO[Unit] = for {
		writes <- b.writes.get
		update = ReplicationUpdate(
			b.txId,
			writes.depotAppends.toList.map { case (id, v)	=> DepotAppends(id, v) },
			writes.pstateUpdates.toList.map { case (id, v) => PStateUpdates(id, v) },
		)

		// (id, acked?) — maxLag-bounded; timeout or transport error => not acked
		results <- channels.toList.parTraverse { case (followerId, ch) =>
			ch.send(update).as(followerId -> true)
				.timeout(config.maxLag)
				.handleError { e =>
					println(s"Follower $followerId didn't acknowledge: $e")
					followerId -> false
				}
		}
		ackedFollowers = results.mapFilter { case (followerId, success) => Option.when(success)(followerId) }
		_ <- taskStatus.update(ts => ts.copy(isr = ackedFollowers))
		_ <- if (ackedFollowers.size >= config.minISR - 1) {
				advanceHighWatermark(b.txId) *> writes.waiters.traverse_(_.complete(()).void)
			} else {
				IO.raiseError(ReplicationStalled(b.txId))
			}
	} yield ()

	private def advanceHighWatermark(to: TxId): IO[Unit] =
		highWatermark.update(h => if to.value > h.value then to else h)

object TaskLeader:
	// A leader resource including the running replication loop for each follower
	def resource(
		taskId: TaskId,
		instanceId: InstanceId,
		followers: Map[InstanceId, Follower],
		config: LeaderConfig,
	)(using Concurrent[IO]): ActiveResource[IO, TaskLeader] = {
		ActiveResource(for {
			allocator <- TransactionAllocator.resource
			cmdQueue <- Resource.eval(Queue.bounded[IO, BatchCommand](config.maxBatchSize))
			// res[io, (task, Map[...])
			channelPairs <- followers.toList.traverse { case (followerId, follower) =>
				ReplicationChannel.open(follower, config).resource.map { case (channel, bg) =>
					followerId -> (channel, bg)
				}
			}.map(_.toMap)
			replicas = instanceId :: followers.keys.toList
			taskStatus = TaskStatus(taskId = taskId, isr = replicas, replicas = replicas)
			taskStatusRef <- Resource.eval(Ref[IO].of(taskStatus))
			hw <- Resource.eval(Ref[IO].of(TxId(0L)))

			// channels are given to TaskLeader so it can offer items, whereas
			// the individual channel background tasks (performing the actual GRPC streaming)
			// are not exposed outside of this resource
			channels = channelPairs.view.mapValues(_._1).toMap
			channelSendTask = channelPairs.values.map(_._2).toList.parTraverse_(identity) >> neverReached
			leader = new TaskLeader(instanceId, config, allocator, cmdQueue, channels, taskStatusRef, hw)
		} yield (leader, IO.race(leader.replicationLoop, channelSendTask).map(_.merge)))
	}

case class BatchWrites(
	depotAppends: Map[DepotId, Vector[DepotAppend]],
	pstateUpdates: Map[PStateId, Vector[PStateUpdate]],
	waiters: List[Deferred[IO, Unit]],
	size: Int,
) {
	def update(cmd: BatchCommand): BatchWrites = cmd match {
		case BatchCommand.AppendCmd(depot, appends, w) => copy(
			depotAppends = depotAppends.updatedWith(depot)(v => Some(v.getOrElse(Vector.empty) ++ appends)),
			waiters = w :: waiters,
			size = size + appends.size
		)

		case BatchCommand.UpdateCmd(pstate, updates, w) => copy(
			pstateUpdates = pstateUpdates.updatedWith(pstate)(v => Some(v.getOrElse(Vector.empty) ++ updates)),
			waiters = w :: waiters,
			size = size + updates.size
		)
	}
}

object BatchWrites {
	val empty = BatchWrites(Map.empty, Map.empty, List.empty, 0)
}

case class OpenBatch(
	txId: TxId,
	openedAt: FiniteDuration,
	writes: Ref[IO, BatchWrites],
) {

	// returns the new size
	def update(cmd: BatchCommand): IO[BatchWrites] = writes.updateAndGet(_.update(cmd))
}

class TransactionAllocator(nextId: Ref[IO, Long]):
	def createBatch: IO[OpenBatch] = for
		id <- nextId.updateAndGet(_ + 1).map(TxId.apply)
		openedAt <- IO.monotonic
		writes <- Ref[IO].of(BatchWrites.empty)
	yield OpenBatch(id, openedAt, writes)

object TransactionAllocator {
	def resource: Resource[IO, TransactionAllocator] =
		Resource.eval(Ref.of[IO, Long](0L).map(new TransactionAllocator(_)))
}

