package corama.interpreter

import cats.syntax.all.*
import cats.effect.IO
import corama.*
import corama.internal.*
import corama.interpreter.Env
import org.tpolecat.sourcepos.SourcePos

import flogger.*
import cats.effect.SyncIO
import scala.util.control.NoStackTrace
import corama.server.ClusterConfig
import cats.data.NonEmptyList

trait SyncIOLogging {
	protected implicit val logOutputSyncIO: LogOutput[SyncIO] = LogOutput[SyncIO](this.getClass)
}

class EvalError(msg: String, pos: SourcePos, cause: Throwable) extends RuntimeException(s"$msg defined at $pos", cause) with NoStackTrace

// stateless logic to eval a set of
class Eval(
	metadata: ClusterConfig,
	log: Log[IO],
) extends IOLogging with SyncIOLogging {
	import Storage._
	given Log[IO] = log
	given syncLog: Log[SyncIO] = Log.from(log)

	// Run a topology over some values. This is invoked
	// only on the owning partition.
	def run[R](
		results: LocalEvalSink[R],
		partitionId: PartitionId,
		evalState: EvalState,
		topology: Topology[?, R],
		envs: NonEmptyList[Env],
	): SyncIO[Unit] = {
			this.continue(
				results,
				topology.id,
				partitionId,
				evalState,
				EdgeId.startingEdge(topology.id),
				Continuation.Basic(topology, topology.compiled._1),
				envs
			)
	}

	// Continue a topology. This is invoked on the destination
	// task after a partition step.
	private [corama] def continue[R](
		source: EvalSource,
		topologyId: TopologyId,
		partitionId: PartitionId,
		evalState: EvalState,
		edgeId: EdgeId,
		continuation: Continuation,
		envs: NonEmptyList[Env],
	): SyncIO[Unit] = {
		// TODO: skip if not debug?
		val origin = source.origin
		val continueLog = syncLog.addContext(
			"topology" -> topologyId.value.toString(),
			"edge" -> edgeId.hex,
			"origin" -> origin.instanceId.id,
			"evalId" -> origin.evalId.hex,
		)
		val continue = continueLog.debug("Continuing computation") *>
			this.continueInner(source, topologyId, partitionId, evalState, edgeId, continuation, envs, continueLog)

		// continue could throw (from user functions), so wrap it in IO.defer
		SyncIO.defer(continue)
	}

	/** Runs a list of nodes with the given envs. Results are returned to `results`.
		*/
	private def continueInner[R](
		source: EvalSource,
		topologyId: TopologyId,
		partitionId: PartitionId,
		evalState: EvalState,
		edgeId: EdgeId,
		continuation: Continuation,
		envs: NonEmptyList[Env],
		log: Log[SyncIO],
	): SyncIO[Unit] = {
		def continueTopology(nodes: List[Node], envs: NonEmptyList[Env], nextEdgeId: EdgeId = edgeId) =
			// supplies args that don't change
			continueInner(source, topologyId, partitionId, evalState, nextEdgeId, continuation, envs, log)

		import Node.DynamicOp
		import Node.PStateOp

		continuation.nodes match {
			case Nil =>
				evalState.outputs.addNotification(source, edgeId, Some(envs.map(_(continuation.output))), continuation.agg)

			case node :: tail => {
				log.debug(s"Executing node: ${node}") *>
				(node.op match {
					case Node.Dynamic(pos, ins, fn, op) => {
						val results: Option[NonEmptyList[Env]] = try {
							op match {
								case DynamicOp.Map(out) =>
									Some(envs.map(env => env.updated(out, fn(ins.map(env(_))))))
								case DynamicOp.Explode(out) =>
									// in each env, `in` holds an `A`
									// `fn` takes A and yields a Seq of values.
									// we expand each of these into `out` in a
									// copy of the parent env
									envs.flatTraverse { env =>
										val seq = fn(ins.map(env(_))).asInstanceOf[Seq[?]]
										NonEmptyList.fromList(seq.toList).map { nel =>
											nel.map { value =>
												env.updated(out, value)
											}
										}
									}
								case DynamicOp.Filter(_) =>
									NonEmptyList.fromList(envs.filter(env => fn(ins.map(env(_))).asInstanceOf[Boolean]))
							}
						} catch {
							case e => throw new EvalError(s"Error evaluating node", pos, e)
						}
						results.traverse_(continueTopology(tail, _))
					}

					case Node.Assign(in, out) =>
						continueTopology(tail, envs.map { env =>
							env.updated(out, env(in))
						})

					case Node.If(cond, ifBranch, elseBranch, out) =>
						// TODO: I don't think we can break up envs like this! Need to statically extend execution tree.
						// TODO: is single-exec batching even useful? We should batch multiple appends instead
						envs.traverse_ { env =>
							if (env(cond).asInstanceOf[Boolean]) {
								continueTopology(ifBranch ++ tail, NonEmptyList.one(env))
							} else {
								continueTopology(elseBranch ++ tail, NonEmptyList.one(env))
							}
						}

					case Node.Partition(in) => {
						/*
						A partition node fans out to multiple partitions. These child edges are
						started in parallel. The result notification of this edge being complete is
						combined (via xor) with the opening of all fanout edges at once.
						*/
						val taskPartitions = metadata.partitionBatches(envs, env => env(in))
						for {
							origin <- source match {
								case remote: Origin => SyncIO.pure(remote)
								
								// if the evaluation started locally and we're repartitioning, register this origin
								case local: LocalEvalSink[?] =>
									evalState.outputs.registerEvalSink(local).as(local.origin)
							}
							// TODO: short-circuit partitions on this task
							evalContinuations <- taskPartitions.traverse { case (taskId, partitionedEnvs) =>
								partitionedEnvs.traverse { partitionedEnv =>
									EdgeId.generate.map(edge => Partitioned(partitionedEnv.partition, (edge, partitionedEnv.value)))
								}.map { partitionedEnvs =>
									EvalContinuation(taskId, origin, topologyId, node.id, partitionedEnvs)
								}
							}
							
							spawnedEdges = evalContinuations.flatMap(cont => cont.partitions.map(_.value._1))
							_ <- evalContinuations.traverse_(evalState.outputs.addContinuation)

							closeOriginAndOpenChildren = spawnedEdges.foldLeft(edgeId)(_.xor(_))
							_ <- evalState.outputs.addNotification(origin, closeOriginAndOpenChildren, None, continuation.agg)
						} yield ()
					}

					case Node.Combine(block, agg, collectFrom, out) => {
						???
						// envs.parTraverse { env =>
						// 	// TODO: skip all the bookkeeping if there aren't any
						// 	// partition jumps in `block`?
						// 	// Note: reusing parent `edgeId` here, but there's no confusion because this is a new EvalId
						// 	EvalSink.local(out, agg, edgeId).flatMap { results =>
						// 		runTopologyEvalRaw(results, evalState, topologyId, edgeId, block, List(env)).map { result =>
						// 			env.updated(out, result)
						// 		}
						// 	}
						// }.flatMap { outputEnvs =>
						// 	continueTopology(tail, outputEnvs)
						// }
					}
					
					case Node.PStateNode(ps, path, op) =>
						envs.traverse { env =>
							op match {
								// adding / removing items of a set is just writing to the key of (path,value)
								case PStateOp.Add(v) => {
									evalState.put(PStateKey.forSetElem(partitionId, ps, path, env, v), Some(Bytes.empty)).as(env)
								}

								case PStateOp.Remove(v) => {
									evalState.put(PStateKey.forSetElem(partitionId, ps, path, env, v), None).as(env)
								}

								case PStateOp.Select(out, options) => {
									val key = PStateKey(partitionId, ps, path, env)

									val after = options.after.flatMap { after =>
										env(after.vid)
											.asInstanceOf[Option[Any]]
											.map(after.vid.codec.encode)
									}
									val rawOpts = SelectOptions.Raw(after, options.maxItems)
									evalState.select(key, rawOpts).map { result =>
										env.updated(out, result.map(out.codec.decodeEx))
									}
								}

								case PStateOp.Get(out: VarId) =>
									evalState.get(PStateKey(partitionId, ps, path, env)).map { stored =>
										env.updated(out, stored.map(out.codec.decodeEx))
									}

								case PStateOp.Put(v: VarId) => {
									evalState.put(PStateKey(partitionId, ps, path, env), Some(v.codec.encode(env(v)))).as(env)
								}
							}
						}.flatMap { updatedEnvs =>
							continueTopology(tail, updatedEnvs)
						}

					case Node.PStateUpdate(_pos, ps, path, ins, fn) =>
						val f = fn // TODO is this necessary?
						envs.traverse_ { env =>
							val key = PStateKey(partitionId, ps, path, env)
							evalState.get(key).flatMap { currentBytes =>
								val current = currentBytes.map(path.codec.decodeEx)
								val args = ins.map(env(_))
								val updated = f(current, args).map(path.codec.encode)
								evalState.put(key, updated)
							}
						}.flatMap { _ =>
							continueTopology(tail, envs)
						}

					case Node.LocalAppend(dep, value) =>
						envs.traverse_ { env =>
							val v = env(value)
							evalState.append(dep, partitionId, value.codec.encode(v))
						} *> continueTopology(tail, envs)
				})
			}
		}
	}

	def invoke[S, A, R](q: QueryTopology[S, R], value: S): IO[R] = {
		???
		// val t = q.topology
		// val env: Env = Map(t.builder.input.vid -> value)
		// EvalSink.local(t.output.vid, t.agg, EdgeId.startingEdge(t.id)).flatMap { results =>
		// 	runTopologyEval(results, t, List(env))
		// }
	}

}
