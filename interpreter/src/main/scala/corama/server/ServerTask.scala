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
import corama.interpreter.SpeculativeState
import corama.interpreter.db.RocksDb
import flogger.IOLogging
import flogger.Log
import fs2.Stream

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.annotation.unused
import cats.effect.kernel.Resource.ExitCase
import cats.effect.SyncIO

private[server] case class RatisCommand[A](op: RatisOp[A], reply: Deferred[IO, Try[A]])
private[server] sealed trait RatisOp[A]
private[server] object RatisCommand {
	case class Append(
		depot: Depot[?],
		partition: PartitionId,
		bytes: Bytes,
		decoded: Any,
		level: AckLevel[?],
	) extends RatisOp[AckReturns]
	case class Invoke(
		q: QueryTopology[?, ?],
		value: Any,
	) extends RatisOp[Any]
}

// rocksDB storage view for a partition
// Note that DBs are shared between all partitions in a task,
// so this only performs reads
class PersistentDbView(db: RocksDb) extends PStateView {
	override def get(key: PStateKey) = {
		db.get(key.raw.raw)
	}

	override def select(key: PStateKey, options: SelectOptions.Raw) = ???
}


/** Executes actions for one task's raft group. See `background`
 * for details on processing concurrency.
 *
 * When invoking append / query / etc, the caller is responsible for first checking requireLeader.
 * Failure to do so will not break anything, it's just wasteful (the eventual proposal will fail)
 */

final class ServerTask private[server] (
	val task: TaskId,
	replog: RepLog,
	leadership: TaskLeadership,
	receivers: EvalReceivers,
	speculative: SpeculativeState,
	topologies: Topologies,
	@unused metadata: ClusterMetadata,

	commands: Queue[IO, RatisCommand[?]],
	submissions: Queue[IO, ServerTask.Submission],
	outputsQueue: Queue[IO, (EvalState.Outputs, ServerTask.PendingAck)],
	pendingAcks: Queue[IO, ServerTask.PendingAck],

	permits: Semaphore[IO],
	partitions: TaskPartitions,
	// partitions evaluated on this instance for this task
	pstateView: PStateView,
	@unused partitionFor: PartitionId => IO[TaskPartitions],
)(using log: Log[IO]) extends IOLogging {
	import RatisCommand.*
	import ServerTask.*

	private val nextSeq = Ref.unsafe[IO, Long](0L)

	// Run the task's background fibers. These are wired up with queues to reduce the scope
	// for fibers that must execute sequentially.
	private[server] def background: IO[Nothing] = {
		Util.joinNothings(List(
			executorLoop.to[IO], // synchronous + sequential. commands -> [execute] -> submissions
			proposeLoop, // sequential. submissions -> [ratis consensus] -> outputsQueue
			processOutputsLoop, // parallel. outputsQueue -> [send notifications + continuations] -> pendingAcks
			fulfilAcksLoop, // parallel. pendingAcks -> [await and return to client]
			drainLoop // synchronous, sequential. watch the replog and trim speculative execution state
		))
	}

	def requireLeader: IO[Unit] = leadership.isLeader.flatMap { isLeader =>
		IO.raiseUnless(isLeader)(NotLeader(None))
	}

	def append[A, R](depot: corama.Depot[A], partition: PartitionId, bytes: Bytes, level: AckLevel[R]): IO[R] = {
		// TODO: we could skip this, but it gives some protection from invalid bytes. Pass both through?
		IO.fromTry(depot.codec.decode(bytes)).flatMap { decoded =>
			submitCommand(Append(depot, partition, bytes, decoded, level)).map { returns =>
				level match {
					case AckLevel.Ack => returns.asInstanceOf[R]
					
					// TODO: can we wait only until replicated, instead of until fully executed?
					// Need to also trigger execution post-append, rather then at-append-time
					case AckLevel.NoAck | AckLevel.AppendAck => ().asInstanceOf[R] // R is always Unit for these ack levels
				}
			}
		}
	}

	def invoke[S, R](q: QueryTopology[S, R], value: S): IO[R] = {
		submitCommand(Invoke(q, value)).map(_.asInstanceOf[R])
	}
	
	private def submitCommand[A](op: RatisOp[A]): IO[A] = {
		for {
			replyRef <- Deferred[IO, Try[A]]
			_ <- commands.offer(RatisCommand(op, replyRef)).to[IO]
			reply <- replyRef.get.flatMap(IO.fromTry)
		} yield reply
	}

	private[server] def executorLoop: IO[Nothing] = {
		// TODO: takeAll?
		commands.take.flatMap(handle).foreverM
	}

	private def handle(cmd: RatisCommand[?]): IO[Unit] = {
		cmd.op match {
			case Append(depot, partition, bytes, decoded, level) =>
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
				val eval: SyncIO[(EvalState, PendingAck)] = for {
					// TODO create state per batch, not individual elem
					evalState <- EvalState(pstateView, speculative)
					_ <- evalState.append(depot.id, partition, bytes)

					// TODO: multiplex multiple commands into a single receiver, so we can just install one per batch
					results <- EvalResults(subscribers(depot.id))
					// speculative execution: run the depot-driven topologies for this
					// record, capturing all writes into evalState
					_ <- partitions.continue(
						???,
						???,
						???,
						???,
						???,
						???,
						???
					)
					update = evalState.toUpdate(txId)

					// raw <- results.await
					// notifications = evalState.notifications
				} yield (evalState, PendingAck(results, reply, None))

				eval.attempt.to[IO].flatMap {
					case Right((evalState, pendingAck)) =>
						val update = evalState.toUpdate(txId)
						// pipeline: block only when the in-flight bound is reached —
						// the slot is released by the await loop when this proposal resolves
						for {
							_ <- permits.acquire
							seq <- nextSeq.updateAndGet(_ + 1)
							proposal <- replog.submit(update)
							submission = ServerTask.Submission(seq, proposal, pendingAck, evalState.outputs)
							_ <- speculative.append(seq, update).to[IO]
							_ <- submissions.offer(submission)
						} yield ()
					case Left(ex) => ???
				}

			case Invoke(q, value) => ???
		}
	}

	private[server] def proposeLoop: IO[Nothing] = {
		(for {
			submission <- submissions.take
			result <- submission.proposal.committed.attempt
			_ <- result match {
				case Right(index) => for {
						_ <- speculative.markCommitted(submission.seq, index).to[IO]

						// Once the proposal is accepted, we can notify the origin of
						// results and additional edges, as well as sending continuations.
						// We can't do this before the proposal is accepted, as
						// - the origin has no visibility into our consensus state
						// - I don't think it's valid (TODO: verify?) for the second
						//   half of a topology to be comitted if the first half was rejected.
						_ <- outputsQueue.offer((submission.outputs, submission.pendingAck))
					} yield ()
				case Left(e) =>
					speculative.discard(submission.seq).to[IO] *>
						// immediately reject, don't go via pendingAcks
						submission.pendingAck.onCommit(topologies, Failure(e))
			}
			_ <- permits.release
		} yield ()).foreverM
	}

	private def notifyOriginOrLog(origin: Origin, notification: EvalNotification): IO[Unit] = {
		// notifications are downstream of evaluation. They are best-effort, we don't block
		// future evaluations on attempting to reach an unavailable origin.
		// Origins which are still waiting on an evaluation will timeout anyway
		// TODO: special-case shortcut when origin instance is self?
		??? // TODO
	}
	
	private def processOutputsLoop: IO[Nothing] = {
		// TODO: is this OK to do in parallel? I think it's valid for any `hashPartition` logic
		// to be reordered with concurrently-executing eval steps
		Stream.fromQueueUnterminated(outputsQueue)
			.parEvalMapUnordered(MAX_INFLIGHT_ACKS) { (outputs, pendingAck) =>
				// install all local eval receivers first, then perform remote ops (continuations and notifications)
				outputs.evalSinks.traverse { sink =>
					receivers.running(sink).allocated
				}.flatMap { sinksAndFinalizers =>
					IO.race(
						// TODO group by task, then by leader?
						outputs.continuations.traverse_ { cont =>
							???
						},
						
						// TODO group by origin
						outputs.notifications.traverse_ { (origin, notification) => notifyOriginOrLog(origin, notification) }
					).guaranteeCase {
						// TODO: cleanup if not successful, otherwise add to pendingAcks
						case ExitCase.Succeeded => pendingAcks.offer(pendingAck.copy(release = Some(
							sinksAndFinalizers.traverse_(_._2)
						)))
						case _ => ???
					}
				}
			}
			.compile.drain *> Util.neverReached
	}

	private def fulfilAcksLoop: IO[Nothing] = {
		// Note acks may complete out of order, as some topologies/events will require
		// repartitioning on different tasks.
		Stream.fromQueueUnterminated(pendingAcks)
			.parEvalMapUnordered(MAX_INFLIGHT_ACKS) { pendingAck =>
				// only committed acks make it to this queue, so this is always Success
				pendingAck.onCommit(topologies, Success(()))
			}.compile.drain *> Util.neverReached
	}

	private def drainLoop: IO[Nothing] = {
		replog.appliedOffsets
			.evalMap(applied => speculative.markApplied(applied).to[IO])
			.compile.drain *> Util.neverReached
	}
	

	// matched by name to avoid depending on Identity's visibility across packages
	private def subscribers(depot: DepotId): List[Topology[?, ?]] = {
		topologies.topologies.filter(_.builder.source == depot)
	}
}

object ServerTask {
	val MAX_INFLIGHT_ACKS = 50

	private[server] case class Submission(
		seq: Long,
		proposal: Proposal,
		pendingAck: PendingAck,
		outputs: EvalState.Outputs
	)

	// captures both the client-connected state (promise)
	// plus references to the EvalResults which will receive
	// return values from other partitions
	case class PendingAck(
		results: EvalResults,
		reply: Deferred[IO, Try[AckReturns]],
		release: Option[IO[Unit]]
	) {
		def onCommit(topologies: Topologies, committed: Try[Unit]) = {
			val respond = committed match {
				case Success(()) => for {
					map <- results.await
					acks = AckReturns(map, topologies)
					_ <- reply.complete(Success(acks))
				} yield ()
				case Failure(err) => reply.complete(Failure(err))
			}
			respond.guarantee(release.sequence_)
		}
	}


	/** One executor per task; its fibers run for the resource's lifetime */
	def resource(
		task: TaskId,
		replog: RepLog,
		view: SpeculativeState,
		topologies: Topologies,
		metadata: ClusterMetadata,
		partitionFor: PartitionId => IO[TaskPartitions],
		maxInFlight: Int = 16,
	)(using log: Log[IO]): ActiveResource[IO, ServerTask] = {
		ActiveResource(for {
			commands <- Resource.eval(Queue.bounded[IO, RatisCommand[?]](10))
			submissions <- Resource.eval(Queue.bounded[IO, Submission](10))
			outputs <- Resource.eval(Queue.bounded[IO, (EvalState.Outputs, PendingAck)](10))
			pendingAcks <- Resource.eval(Queue.bounded[IO, PendingAck](10))
			permits <- Resource.eval(Semaphore[IO](maxInFlight.toLong))
			rocksDbState = identity[PStateView](???)
			serverTask = ServerTask(
				task,
				replog,
				???,
				???,
				view,
				topologies,
				metadata,
				commands,
				submissions,
				outputs,
				???,
				permits,
				???,
				rocksDbState,
				partitionFor
			)
		} yield (serverTask, serverTask.background))
	}
}

