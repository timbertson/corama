package corama.interpreter

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Queue
import cats.syntax.all.*
import corama.InstanceId
import fs2.Stream
import flogger.{Log, IOLogging}
import corama.server.InMemoryGroup
import org.apache.ratis.server.protocol.TermIndex


class NotLeader(val leader: Option[InstanceId])
	extends RuntimeException(s"not the leader (hint: ${leader.fold("unknown")(_.id.toString)})")

trait RepLog {
	// Waits until proposal has been sent, caller must await proposal.committed
	// TODO: can/should we pipeline, to allow multiple
	def submit(update: ReplicationUpdate): IO[Proposal]

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
	group: InMemoryGroup,
	onApply: (LogIndex, ReplicationUpdate) => IO[Unit],
	applyDelay: IO[Unit],
	pendingApply: Queue[IO, (LogIndex, ReplicationUpdate)],
	appliedOffsetsQueue: Queue[IO, LogIndex],
)(using Log[IO]) extends RepLog with IOLogging {
	private val log = Ref.unsafe[IO, Vector[(LogIndex, ReplicationUpdate)]](Vector.empty)
	private val appliedRef = Ref.unsafe[IO, LogIndex](LogIndex(0))
	private val committedTx = Ref.unsafe[IO, Set[LogIndex]](Set.empty)
	private val taskLeadership = group.leadership(instanceId)

	private def requireLeader: IO[Unit] = taskLeadership.currentLeader.flatMap { leader =>
		IO.raiseUnless(leader.contains(instanceId))(NotLeader(leader))
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

	def isCommitted(txId: LogIndex): IO[Boolean] = committedTx.get.map(_.contains(txId))

	def appliedOffsets: Stream[IO, LogIndex] = {
		Stream.fromQueueUnterminated(appliedOffsetsQueue)
	}
}

object RepLog {
	def inMemory(
		instanceId: InstanceId,
		group: InMemoryGroup,
		onApply: (LogIndex, ReplicationUpdate) => IO[Unit] = (_, _) => IO.unit,
		applyDelay: IO[Unit] = IO.unit,
	)(using Log[IO]): IO[InMemoryRepLog] = {
		for {
			pendingApply <- Queue.unbounded[IO, (LogIndex, ReplicationUpdate)]
			offsets <- Queue.unbounded[IO, LogIndex]
		} yield new InMemoryRepLog(instanceId, group, onApply, applyDelay, pendingApply, offsets)
	}
}
