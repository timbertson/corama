package corama.interpreter

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Queue
import cats.syntax.all.*
import corama.InstanceId
import fs2.Stream
import flogger.{Log, IOLogging}


// interfaces used to swap out real server backend for an in-memory one in tests

case class LogIndex(value: Long)

class NotLeader(val leader: Option[InstanceId])
	extends RuntimeException(s"not the leader (hint: ${leader.fold("unknown")(_.id.toString)})")

trait RepLog {
	// Waits until proposal has been sent, caller must await proposal.committed
	// TODO: can/should we pipeline, to allow multiple
	def submit(update: ReplicationUpdate): IO[Proposal]

	// Note: not correct if the given TX's `propose` is still pending on this replog
	def isCommitted(txId: TxId): IO[Boolean]

	// current leader, if known
	def leader: IO[Option[InstanceId]]

	// consumed to drive the stream. TODO: is this necessary?
	def appliedOffsets: Stream[IO, LogIndex]
}

trait Proposal {
	def committed: IO[LogIndex]
}

object Proposal {
	def completedNow(index: LogIndex): Proposal = new Proposal { val committed = IO.pure(index) }
	def apply(whenCommitted: IO[LogIndex]): Proposal = new Proposal { def committed = whenCommitted }
}

/** The in-memory backend: propose appends to a `Ref`-held log and completes
	* once committed.
	* TODO: should onApply be a hook, or bake it into an in-memory version
	* of pstate/dpots?
	*/
final class InMemoryRepLog(
	instanceId: InstanceId,
	leaderRef: Ref[IO, InstanceId],
	onApply: (LogIndex, ReplicationUpdate) => IO[Unit],
	applyDelay: IO[Unit],
	pendingApply: Queue[IO, (LogIndex, ReplicationUpdate)],
	appliedOffsetsQueue: Queue[IO, LogIndex],
)(using Log[IO]) extends RepLog with IOLogging {
	private val log = Ref.unsafe[IO, Vector[(LogIndex, ReplicationUpdate)]](Vector.empty)
	private val appliedRef = Ref.unsafe[IO, LogIndex](LogIndex(-1L))
	private val committedTx = Ref.unsafe[IO, Set[TxId]](Set.empty)

	private def requireLeader: IO[Unit] = leaderRef.get.flatMap { leader =>
		IO.raiseUnless(leader === instanceId)(NotLeader(Some(leader)))
	}

	// TODO: reuse OpenBatch handling from ratis backend
	// TODO: the returned proposal is always already completed
	def submit(update: ReplicationUpdate): IO[Proposal] = {
		for {
			_ <- requireLeader
			entries <- log.get
			index = LogIndex(entries.size)
			_ <- log.update(_.appended(index -> update))
			_ <- committedTx.update(_ + update.id)
			_ <- pendingApply.offer(index -> update)
		} yield Proposal.completedNow(index)
	}

	def applyLoop: IO[Nothing] = pendingApply.take.flatMap { (index, update) =>
		for {
			_ <- applyDelay
			_ <- onApply(index, update) // expose to tests
			_ <- appliedRef.set(index)
			_ <- appliedOffsetsQueue.offer(index)
		} yield ()
	}.foreverM

	def isCommitted(txId: TxId): IO[Boolean] = committedTx.get.map(_.contains(txId))
	def leader: IO[Option[InstanceId]] = leaderRef.get.map(Some(_))

	def appliedOffsets: Stream[IO, LogIndex] = {
		Stream.fromQueueUnterminated(appliedOffsetsQueue)
	}
}

object InMemoryRepLog {
	def apply(
		instanceId: InstanceId,
		onApply: (LogIndex, ReplicationUpdate) => IO[Unit],
		applyDelay: IO[Unit] = IO.unit,
	)(using Log[IO]): IO[InMemoryRepLog] = {
		for {
			leaderRef <- Ref[IO].of(instanceId) // TODO?
			pendingApply <- Queue.unbounded[IO, (LogIndex, ReplicationUpdate)]
			offsets <- Queue.unbounded[IO, LogIndex]
		} yield new InMemoryRepLog(instanceId, leaderRef, onApply, applyDelay, pendingApply, offsets)
	}
}
