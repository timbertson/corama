package corama.server

import cats.effect.IO
import cats.effect.Outcome
import cats.effect.SyncIO
import cats.effect.kernel.Deferred
import cats.effect.kernel.Outcome.Canceled
import cats.effect.kernel.Outcome.Errored
import cats.effect.kernel.Outcome.Succeeded
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Queue
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
import corama.server.ratis.ClusterClient
import io.grpc.Metadata
import corama.server.TaskCommand.OpCommand

private[server] sealed trait TaskCommand[A]
private[server] sealed trait TaskOp[A]
private[server] object TaskCommand {
	case class Continue(
		origin: Origin,
		topologyId: TopologyId,
		continuation: Continuation,
		partitions: List[Partitioned[(EdgeId, List[Env])]],
	) extends TaskCommand[Unit]

	case class OpCommand[A](op: TaskOp[A], reply: Deferred[IO, Try[A]]) extends TaskCommand[A]
	case class Append(
		depot: Depot[?],
		partition: PartitionId,
		bytes: Bytes,
		decoded: Any,
		level: AckLevel[?],
	) extends TaskOp[AckReturns]

	case class Invoke(
		q: QueryTopology[?, ?],
		value: Any,
	) extends TaskOp[Any]
}

// rocksDB storage view for a task
// Note that DBs are shared between all partitions in a task,
// so keys embed the partition ID
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
	val instanceId: InstanceId,
	val task: TaskId,
	speculative: SpeculativeState,
	clusterClient: ClusterClient,
	replog: RepLog,
	leadership: TaskLeadership,
	receivers: IncomingEvalRouter,
	topologies: Topologies,
	evaluator: Eval,

	// queues
	commands: Queue[IO, TaskCommand[?]],
	submissions: Queue[IO, ServerTask.Submission],
	outputsQueue: Queue[IO, (EvalState.Outputs, Option[ServerTask.PendingAck])],
	pendingAcks: Queue[IO, ServerTask.PendingAck],

)(using log: Log[IO]) extends IOLogging {
	import TaskCommand.*
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

	def append[A, R](append: Append): IO[R] = {
		submitOp(append).map { returns =>
			append.level match {
				case AckLevel.Ack => returns.asInstanceOf[R]
				
				// TODO: can we wait only until replicated, instead of until fully executed?
				// Need to also trigger execution post-append, rather then at-append-time
				case AckLevel.NoAck | AckLevel.AppendAck => ().asInstanceOf[R] // R is always Unit for these ack levels
			}
		}
	}

	def continue[A, R](cont: Continue): IO[Unit] = {
		commands.offer(cont)
	}

	def invoke[S, R](q: QueryTopology[S, R], value: S): IO[R] = {
		submitOp(Invoke(q, value)).map(_.asInstanceOf[R])
	}
	
	private def submitOp[A](op: TaskOp[A]): IO[A] = {
		for {
			replyRef <- Deferred[IO, Try[A]]
			_ <- commands.offer(OpCommand(op, replyRef)).to[IO]
			reply <- replyRef.get.flatMap(IO.fromTry)
		} yield reply
	}

	private[server] def executorLoop: IO[Nothing] = {
		// TODO: takeAll?
		commands.take.flatMap(handle).foreverM
	}

	private def handle(cmd: TaskCommand[?]): IO[Unit] = {
		cmd match {
			case c: Continue => {
				val eval: SyncIO[EvalState] = for {
					// TODO create state per batch, not individual elem
					evalState <- EvalState(speculative, pendingAck = None)
					_ <- c.partitions.traverse_ { partitioned =>
						val (edgeId, envs) = partitioned.value
						evaluator.continue(
							c.origin,
							c.topologyId,
							partitioned.partition,
							evalState,
							edgeId,
							c.continuation,
							envs
						)
					}
					_ <- if (evalState.outputs.registeredEvalSinks.isEmpty) {
						SyncIO.unit
					} else {
						SyncIO.raiseError(new RuntimeException("Continuation registered eval sinks"))
					}
				} yield (evalState)
				handleEval(eval)
			}

			case OpCommand(op, reply) => op match {
				case Append(depot, partition, bytes, decoded, level) =>
					// TODO: return early if the client isn't actually waiting for returns
					val evalTopologies = subscribers(depot.id)
					val eval: SyncIO[EvalState] = for {
						// TODO create state per batch, not individual elem
						results <- EvalResults(instanceId, evalTopologies)
						evalState <- EvalState(speculative, Some(PendingAck(results, reply, None)))
						_ <- evalState.append(depot.id, partition, bytes)

						// TODO: multiplex multiple commands into a single receiver, so we can just install one per batch
						// speculative execution: run the depot-driven topologies for this
						// record, capturing all writes into evalState
						_ <- evalTopologies.traverse { topology =>
							SyncIO.fromTry(results.forTopology(topology)).flatMap { localResults =>
								evaluator.run(
									localResults,
									partition,
									evalState,
									topology,
									List(Map(topology.input.vid -> decoded))
								)
							}
						}
					} yield evalState
					handleEval(eval)

				case Invoke(q, value) => ???
			}
		}
	}

	private def handleEval(eval: SyncIO[EvalState]): IO[Unit] = {
		// common logic for append & continue branches in `handle`
		eval.attempt.to[IO].flatMap {
			case Right(evalState) =>
				val txId = ??? : TxId
				val update = evalState.toUpdate(txId)
				// pipeline: block only when the in-flight bound is reached —
				// the slot is released by the await loop when this proposal resolves
				for {
					seq <- nextSeq.updateAndGet(_ + 1)
					proposal <- replog.submit(update)
					submission = ServerTask.Submission(seq, proposal, evalState.pendingAck, evalState.outputs)
					_ <- speculative.append(seq, update).to[IO]
					_ <- submissions.offer(submission)
				} yield ()
			case Left(ex) => ???
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
						// - We don't want to start work on the new partition if the current
						//   partition's writes may later be rejected.
						_ <- outputsQueue.offer((submission.outputs, submission.pendingAck))
					} yield ()
				case Left(e) =>
					speculative.discard(submission.seq).to[IO] *>
						// immediately reject, don't go via pendingAcks
						submission.pendingAck.traverse_(_.onCommit(topologies, Failure(e)))
			}
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
				// complete any local results
				outputs.localResults.traverse_ { (local, edge, values) =>
					local.completeList(edge, values)
				} *>
				// install all eval receivers
				outputs.registeredEvalSinks.traverse { sink =>
					receivers.running(sink).allocated.map(_._2)
				}.flatMap { releaseEvalSinks =>
					val cleanup = releaseEvalSinks.sequence_
					// then perform remote ops in parallel (continuations and notifications)
					IO.race(
						// TODO group by task, then by leader?
						outputs.continuations.traverse_ { cont =>
							clusterClient.invokeGrpc(cont.origin.instanceId) { client =>
								client.continue(EvalContinuation.toProtobuf(cont), Metadata())
							}
						},
						
						// TODO group by origin
						outputs.notifications.traverse_ { (origin, notification) => notifyOriginOrLog(origin, notification) }
					).guaranteeCase { outcome =>
						pendingAck match {
							// TODO: when pendingAck is None, it's a continuation. We could notify the origin so it doesn't have to rely on a timeout
							case None => outcome match {
									case Outcome.Succeeded(_) => IO.unit
									case _ => cleanup
							}
							case Some(pendingAck) => outcome match {
								case Outcome.Succeeded(_) => pendingAcks.offer(pendingAck.copy(release = Some(cleanup)))
								case Outcome.Errored(t) => cleanup *> pendingAck.reply.complete(Failure(t)).void
								case Outcome.Canceled() => cleanup *> pendingAck.reply.complete(Failure(new RuntimeException("Canceled"))).void
							}
						}
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
		pendingAck: Option[PendingAck],
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
		instanceId: InstanceId,
		task: TaskId,
		topologies: Topologies,
		clusterClient: ClusterClient,
		
		// state
		replog: RepLog,
		speculative: SpeculativeState,
		
		metadata: ClusterConfig,
		taskLeadership: TaskLeadership,
		evalRouter: IncomingEvalRouter,

		maxInFlight: Int = 16,
	)(using log: Log[IO]): ActiveResource[IO, ServerTask] = {
		ActiveResource(for {
			commands <- Resource.eval(Queue.bounded[IO, TaskCommand[?]](10))
			submissions <- Resource.eval(Queue.bounded[IO, Submission](10))
			outputs <- Resource.eval(Queue.bounded[IO, (EvalState.Outputs, Option[PendingAck])](10))
			pendingAcks <- Resource.eval(Queue.bounded[IO, PendingAck](10))
			serverTask = ServerTask(
				instanceId,
				task,
				speculative,
				clusterClient,
				replog,
				taskLeadership,
				evalRouter,
				topologies,
				new Eval(metadata, log),
				commands,
				submissions,
				outputs,
				pendingAcks
			)
		} yield (serverTask, serverTask.background))
	}
}

