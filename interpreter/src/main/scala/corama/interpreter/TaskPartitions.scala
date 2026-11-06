package corama.interpreter

import cats.syntax.all.*
import cats.effect.IO
import corama.*
import corama.internal.*
import corama.interpreter.Env
import org.tpolecat.sourcepos.SourcePos

import flogger.*
import cats.effect.SyncIO

trait SyncIOLogging {
	protected implicit val logOutputSyncIO: LogOutput[SyncIO] = LogOutput[SyncIO](this.getClass)
}

// the stateless execution logic for a single task's partitions
class TaskPartitions(
	receivers: EvalReceivers,
	metadata: ClusterMetadata,
	topologies: Topologies,
	taskRouter: TaskRouter,
	log: Log[IO],
) extends IOLogging with SyncIOLogging {
	import Storage._
	given Log[IO] = log
	given syncLog: Log[SyncIO] = Log.from(log)

	// Run a topology over some values by installing its results into the global GRPC service until completion
	private def runTopologyEval[R](
		results: LocalEvalSink[R],
		evalState: EvalState,
		topology: Topology[?, R],
		envs: List[Env],
	): SyncIO[Unit] = {
		// TODO: only addEvalSink when there's a repartitioning that leaves this node
		evalState.outputs.addEvalSink(results) *>
			this.continue(
				???,
				topology.id,
				evalState,
				EdgeId.startingEdge(topology.id),
				topology.compiled._1,
				topology.output.vid,
				envs
			)
	}

	private [corama] def continue[R](
		origin: Origin,
		topologyId: TopologyId,
		evalState: EvalState,
		edgeId: EdgeId,
		nodes: List[Node],
		outputVar: VarId,
		envs: List[Env],
	): SyncIO[Unit] = {
		// TODO: skip if not debug?
		val continueLog = syncLog.addContext(
			"origin" -> origin.instanceId.id,
			"evalId" -> origin.evalId.hex,
			"topology" -> topologyId.value.toString(),
			"edge" -> edgeId.hex,
		)
		val continue = continueLog.debug("Continuing computation") *>
			this.continueInner(origin, ???, topologyId, evalState, edgeId, nodes, outputVar, envs, continueLog)

		// continue could throw (from user functions), so wrap it in IO.defer
		SyncIO.defer(continue)
	}

	/** Runs a list of nodes with the given envs. Results are returned to `results`.
		*/
	private def continueInner[R](
		origin: Origin,
		partitionId: PartitionId,
		topologyId: TopologyId,
		evalState: EvalState,
		edgeId: EdgeId,
		nodes: List[Node],
		outputVar: VarId,
		envs: List[Env],
		log: Log[SyncIO],
	): SyncIO[Unit] = {
		def continueTopology(nodes: List[Node], envs: List[Env], nextEdgeId: EdgeId = edgeId) =
			// supplies args that don't change
			continueInner(origin, partitionId, topologyId, evalState, nextEdgeId, nodes, outputVar, envs, log)

		import Node.DynamicOp
		import Node.PStateOp

		nodes match {
			case Nil =>
				evalState.outputs.addNotification(origin, edgeId, envs.map(_(outputVar)), ???)

			case node :: tail => {
				log.debug(s"Executing node: ${node}") *>
				(node.op match {
					case Node.Dynamic(pos, ins, fn, op) => {
						val results = try {
							op match {
								case DynamicOp.Map(out) =>
									envs.map(env => env.updated(out, fn(ins.map(env(_)))))
								case DynamicOp.Explode(out) =>
									// in each env, `in` holds an `A`
									// `fn` takes A and yields a Seq of values.
									// we expand each of these into `out` in a
									// copy of the parent env
									envs.flatMap { env =>
										fn(ins.map(env(_))).asInstanceOf[Seq[?]].toList.map { value =>
											env.updated(out, value)
										}
									}
								case DynamicOp.Filter(_) =>
									envs.filter(env => fn(ins.map(env(_))).asInstanceOf[Boolean])
							}
						} catch {
							case e => throw new EvalError(s"Error evaluating node", pos, e)
						}
						continueTopology(tail, results)
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
								continueTopology(ifBranch ++ tail, List(env))
							} else {
								continueTopology(elseBranch ++ tail, List(env))
							}
						}

					case Node.Partition(in) => {
						/*
						A partition node fans out to multiple partitions. These child edges are
						started in parallel. The result notification of this edge being complete is
						combined (via xor) with the opening of all fanout edges at once.
						*/
						val taskPartitions = metadata.partitionBatches(envs, env => env(in)).toList
						for {
							continuationTasks <- taskPartitions.traverse { case (taskId, partitionedEnvs) =>
								EdgeId.generate.map { childEdge =>
									EvalContinuationTask(taskId, childEdge, partitionedEnvs)
								}
							}
							
							spawnedEdges = continuationTasks.map(_.edge)
							closeOriginAndOpenChildren = spawnedEdges.foldLeft(edgeId)(_.xor(_))
							continuation = EvalContinuation(origin, topologyId, node.id, continuationTasks)
							_ <- evalState.outputs.addContinuation(continuation)
							_ <- evalState.outputs.addNotification(origin, closeOriginAndOpenChildren, Nil, ???)
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
