package corama.server

import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.syntax.all.*
import corama.*
import corama.AckLevel.AppendAck
import corama.error.Util
import corama.interpreter.*
import flogger.IOLogging
import flogger.Log
import fs2.Stream
import org.apache.ratis.client.RaftClient
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.exceptions.NotLeaderException

import java.nio.ByteBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import corama.interpreter.SpeculativeState

private[server] case class RatisCommand[A](op: RatisOp[A], reply: Deferred[IO, Try[A]])
private[server] sealed trait RatisOp[A]
private[server] object RatisCommand {
	case class Append(
		depot: DepotId,
		value: Bytes,
		level: AckLevel[?],
	) extends RatisOp[AckReturns]
	case class Invoke(
		q: QueryTopology[?, ?],
		value: Any,
	) extends RatisOp[Any]
}

/** Executes actions for one task's raft group. Proposals can be submitted
 * concurrently, but are serialized in the command queue.
 *
 * Three concurrent processes:
 * executorLoop:
 * - take command
 * - execute (on speculative view)
 * - submit (ratis propose)
 *
 * awaitLoop:
 * - awaits proposals in submission / commit order (including failed proposals)
 * - update speculative view with committed LogIndex
 * - resolve the Ack
 *
 * drainLoop:
 * - trim speculative view to remove now-applied LogIndexes
 */
final class RatisTask private[server] (
	val task: TaskId,
	replog: RepLog,
	speculative: SpeculativeState,
	topologies: Topologies,
	metadata: ClusterMetadata,
	commands: Queue[IO, RatisCommand[?]],
	submissions: Queue[IO, RatisTask.Submission],
	permits: Semaphore[IO],
	// partitions evaluated on this instance for this task
	pstateView: PStateView,
	partitionFor: PartitionId => IO[Partition],
)(using log: Log[IO]) extends IOLogging {
	import RatisCommand.*

	private val nextSeq = Ref.unsafe[IO, Long](0L)

	// Public entrypoint. Waits for the full ack, which is not
	// necessary for an append op within a partition
	def append[A, R](depot: corama.Depot[A], value: A, level: AckLevel[R]): IO[R] = {
		// TODO: encode `value` via the depot's codec
		val encoded: Bytes = ???
		submitCommand(Append(DepotId(depot.id.name), encoded, level)).map { returns =>
			level match {
				case AckLevel.Ack => returns.asInstanceOf[R]
				case AckLevel.NoAck | AckLevel.AppendAck => ().asInstanceOf[R] // R is always Unit for these ack levels
			}
		}
	}

	def invoke[S, R](q: QueryTopology[S, R], value: S): IO[R] = {
		submitCommand(Invoke(q, value)).map(_.asInstanceOf[R])
	}
	
	private def submitCommand[A](op: RatisOp[A]): IO[A] = {
		for {
			replyRef <- Deferred[IO, Try[A]]
			_ <- commands.offer(RatisCommand(op, replyRef))
			reply <- replyRef.get.flatMap(IO.fromTry)
		} yield reply
	}

	private[server] def executorLoop: IO[Nothing] = {
		// TODO: takeAll?
		commands.take.flatMap(handle).foreverM
	}

	private def handle(cmd: RatisCommand[?]): IO[Unit] = {
		cmd.op match {
			case Append(depot, value, level) =>
				// all appends are RatisCommand[AckReturns]
				val reply = cmd.asInstanceOf[RatisCommand[AckReturns]].reply
				// TODO: return early if the client isn't actually waiting for returns

				// TODO: allocate the TxId here, inside the serialized fiber so tx ids
				// and log indices stay consistent while leadership holds.
				// A bare counter is not enough — key on (instanceId, txId) or take
				// client-supplied ids, since a leader change resets a bare counter
				// and uncommitted old-leader txs collide.
				// A retry of a committed action must dedup by TxId at execution time.
				val txId = ??? : TxId
				val eval = for {
					evalState <- EvalState(pstateView, speculative)
					results <- EvalResults(subscribers(depot))
					// speculative execution: run the depot-driven topologies for this
					// record, capturing all writes into evalState
					_ <- executeTopologies(depot, value, evalState, results)
					raw <- results.await
					update = evalState.toUpdate(txId)
				} yield (raw, update)

				eval.attempt.flatMap {
					// evaluation failed: the ack fails, the record never reaches the log.
					// TODO: should this be allowed to retry? Definitely not forever...
					case Left(e) => reply.complete(Failure(e)).void

					case Right((raw, update)) =>
						// pipeline: block only when the in-flight bound is reached —
						// the slot is released by the await loop when this proposal resolves
						for {
							_ <- permits.acquire
							seq <- nextSeq.updateAndGet(_ + 1)
							proposal <- replog.submit(update)
							submission = RatisTask.Submission(seq, proposal, AckReturns(raw, topologies), reply)
							_ <- speculative.append(seq, update)
							_ <- submissions.offer(submission)
						} yield ()
				}

			case Invoke(q, value) => ???
		}
	}

	private[server] def awaitLoop: IO[Nothing] = {
		(for {
			submission <- submissions.take
			result <- submission.proposal.committed.attempt
			_ <- result match {
				case Right(index) =>
					speculative.markCommitted(submission.seq, index) *>
						// committed, release the ack
						// note: apply happens after ack, but that's OK because we already speculatively executed it.
						// On failover it's in the log, so it'll be available there too
						submission.reply.complete(Success(submission.raw)).void
				case Left(e) =>
					speculative.discard(submission.seq) *>
						submission.reply.complete(Failure(e)).void
			}
			_ <- permits.release
		} yield ()).foreverM
	}

	// The task's background fibers
	private[server] def background: IO[Nothing] = {
		val drainLoop = replog.appliedOffsets.evalMap(speculative.markApplied).compile.drain *> Util.neverReached
		Util.joinNothings(List(
			executorLoop,
			awaitLoop,
			drainLoop
		))
	}

	// matched by name to avoid depending on Identity's visibility across packages
	private def subscribers(depot: DepotId): List[Topology[?, ?]] = {
		topologies.topologies.filter(_.builder.source.name == depot.value)
	}

	// deprecated sketch wiring: run topologies with a capturing partition
	private def executeTopologies(
		depot: DepotId,
		value: Bytes,
		state: EvalState,
		results: EvalResults,
	): IO[Unit] = ???
	// TODO: hook up interpreter with eval state
}

object RatisTask {
	private[server] case class Submission(
		seq: Long,
		proposal: Proposal,
		raw: AckReturns,
		reply: Deferred[IO, Try[AckReturns]],
	)

	/** One executor per task; its fibers run for the resource's lifetime */
	def resource(
		task: TaskId,
		replog: RepLog,
		view: SpeculativeState,
		topologies: Topologies,
		metadata: ClusterMetadata,
		partitionFor: PartitionId => IO[Partition],
		maxInFlight: Int = 16,
	)(using log: Log[IO]): ActiveResource[IO, RatisTask] = {
		ActiveResource(for {
			commands <- Resource.eval(Queue.unbounded[IO, RatisCommand[?]])
			submissions <- Resource.eval(Queue.unbounded[IO, Submission])
			permits <- Resource.eval(Semaphore[IO](maxInFlight.toLong))
			rocksDbState = identity[PStateView](???)
			ratisTask = RatisTask(
				task,
				replog,
				view,
				topologies,
				metadata,
				commands,
				submissions,
				permits,
				rocksDbState,
				partitionFor
			)
		} yield (ratisTask, ratisTask.background))
	}
}

/** The ratis backend of [[RepLog]] for one task's group. */
final class RatisRepLog(raft: RaftClient)(using Log[IO]) extends RepLog with IOLogging {
	// TODO: use OpenBatch from interpreter to keep 1 open batch at all times.
	// Submit should only block once there's 1 in-flight batch and the next batch is full
	// TODO: can we start sending a ratis batch before the previous one completes? Should we?
	def submit(update: ReplicationUpdate): IO[Proposal] = {
		IO.pure {
			Proposal(IO.fromCompletableFuture {
				IO.delay {
					raft.async().send(Message.valueOf(
						ByteBuffer.wrap(ReplogCodecs.replicationUpdate.encode(update).array)
					))
				}
			}.map(reply => LogIndex(reply.getLogIndex))
			// TODO: leader hint
			.adaptError { case e: NotLeaderException => NotLeader(None) }
			)
		}
	}

	def isCommitted(txId: TxId): IO[Boolean] = ???
	// TODO: durable tx bookkeeping — the state machine indexes tx ids as it applies

	def leader: IO[Option[InstanceId]] = ???
	// TODO: from the client's leader knowledge

	def appliedOffsets: Stream[IO, LogIndex] = ???
	// TODO: broadcast from the StateMachine.applyTransaction hook
}

final class RatisInterpreter(
	val topologies: Topologies,
	metadata: ClusterMetadata,
	val tasks: Map[TaskId, RatisTask],
)(using Log[IO]) extends IOLogging {

	private def taskFor(partitionId: PartitionId): IO[TaskId] = {
		IO.pure(metadata.taskFor(partitionId))
	}

	// Note: this is rejected if not currently the leader.
	def append[A, R](depot: corama.Depot[A], value: A, level: AckLevel[R] = AckLevel.Ack): IO[R] = {
		taskFor(metadata.partitionKey(value)).flatMap { taskId =>
			tasks.get(taskId) match {
				case Some(task) => task.append(depot, value, level)
				case None => ??? // TODO: route to the hosting instance via custom gRPC
			}
		}
	}
}

object RatisInterpreter {
	def resource(
		topologies: Topologies,
		metadata: ClusterMetadata,
		groupClients: Map[TaskId, GroupClient],
		partitionFor: PartitionId => IO[Partition],
	)(using log: Log[IO]): ActiveResource[IO, RatisInterpreter] = {
		val clients = groupClients.toList.traverse { (taskId, groupClient) =>
			RatisTask.resource(
				taskId,
				RatisRepLog(groupClient.raft),
				SpeculativeState(),
				topologies,
				metadata,
				partitionFor,
			).resource
		}
		
		ActiveResource(clients.map { activeTasks =>
			val background = Util.joinNothings(activeTasks.map(_._2))
			val tasks = activeTasks.map { (t, _) => t.task -> t }.toMap
			(RatisInterpreter(topologies, metadata, tasks), background)
		})
	}
}
