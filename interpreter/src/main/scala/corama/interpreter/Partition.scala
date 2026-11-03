package corama.interpreter

import cats.syntax.all.*
import cats.data.NonEmptyList
import cats.effect.IO
import corama.*
import corama.internal.*
import corama.interpreter.Env
import org.tpolecat.sourcepos.SourcePos

import scala.util.*
import flogger.*
import izumi.reflect.Tag

class Partition(
	id: PartitionId,
	state: SpeculativeState,
	receivers: EvalReceivers,
	metadata: ClusterMetadata,
	topologies: Topologies,
	taskRouter: TaskRouter,
	log: Log[IO],
) extends IOLogging {
	import Storage._
	given Log[IO] = log

	private val idList = topologies.ids.toList

	// Run a topology over some values by installing its results into the global GRPC service until completion
	private def runTopologyEval[R](
		results: LocalEvalSink[R],
		evalState: EvalState,
		topology: Topology[?, R],
		envs: List[Env],
	): IO[R] = {
		runTopologyEvalRaw(results, evalState, topology.id, EdgeId.startingEdge(topology.id), topology.compiled._1, envs)
	}

	// As above, but also used in sub-aggregations to collect the results of a block
	private def runTopologyEvalRaw[R](
		results: LocalEvalSink[R],
		evalState: EvalState,
		topologyId: TopologyId,
		start: EdgeId,
		nodes: List[Node],
		envs: List[Env],
	): IO[R] = {
		// install into GRPC listener so other partitions can send results
		receivers.addRunning(results) { origin =>
			// continue could throw (from user functions), so wrap it in IO.defer
			this.continue(origin, topologyId, results, evalState, start, nodes, envs).attempt.flatMap {
				// TODO: how to apply backpressure if clients are not waiting?
				case Right(_) => IO.unit

				// One bad record must not stop the topology: Rama retries the record and
				// keeps consuming. There is no retry here, so the failure is reported to
				// whoever appended it and consumption continues.
				case Left(e) => results.fail(e)
			} *> results.await.flatMap(r => log.debug(s"runTopologyEval got results: $r").as(r))
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

	private [corama] def continue[R](
		origin: Origin,
		topologyId: TopologyId,
		evalSink: EvalSink[R],
		evalState: EvalState,
		edgeId: EdgeId,
		nodes: List[Node],
		envs: List[Env],
	): IO[Unit] = {
		// TODO: skip if not debug?
		val continueLog = log.addContext(
			"origin" -> origin.instanceId.id,
			"evalId" -> origin.evalId.hex,
			"topology" -> topologyId.value.toString(),
			"edge" -> edgeId.hex,
		)
		val continue = continueLog.debug("Continuing computation") *>
			this.continueInner(origin, topologyId, evalSink, evalState, edgeId, nodes, envs, continueLog)

		IO.defer(continue)
	}

	/** Runs a list of nodes with the given envs. Results are returned to `results`.
	 *  TODO: should this be IO?
		*/
	private def continueInner[R](
		origin: Origin,
		topologyId: TopologyId,
		evalSink: EvalSink[R],
		evalState: EvalState,
		edgeId: EdgeId,
		nodes: List[Node],
		envs: List[Env],
		log: Log[IO],
	): IO[Unit] = {
		def continueTopology(nodes: List[Node], envs: List[Env], nextEdgeId: EdgeId = edgeId) =
			// supplies origin / topology / evalSink since those don't change
			continueInner(origin, topologyId, evalSink, evalState, nextEdgeId, nodes, envs, log)

		import Node.DynamicOp
		import Node.PStateOp

		nodes match {
			case Nil =>
				evalSink.completeList(edgeId, envs.map(_(evalSink.varId).asInstanceOf[R]))

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
						taskPartitions.parTraverse { case (taskId, partitionedEnvs) =>
							EdgeId.generate.flatMap { childEdge =>
								taskRouter.continue(taskId, topologyId, node.id, childEdge, partitionedEnvs, origin).as(childEdge)
							}
						}.flatMap { spawnedEdges =>
							val closeOriginAndOpenChildren = spawnedEdges.foldLeft(edgeId)(_.xor(_))
							evalSink.notify(closeOriginAndOpenChildren, None)
						}
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
							IO.pure(op match {
								// adding / removing items of a set is just writing to the key of (key,value)
								case PStateOp.Add(v) => {
									evalState.put(PStateKey.forSetElem(id, ps, path, env, v), Some(Bytes.empty))
									env
								}

								case PStateOp.Remove(v) => {
									evalState.put(PStateKey.forSetElem(id, ps, path, env, v), None)
									env
								}

								case PStateOp.Select(out, options) => {
									val key = PStateKey(id, ps, path, env)

									val after = options.after.flatMap { after =>
										env(after.vid)
											.asInstanceOf[Option[Any]]
											.map(after.vid.codec.encode)
									}
									val rawOpts = SelectOptions.Raw(after, options.maxItems)
									env.updated(out, evalState.select(key, rawOpts))
								}

								case PStateOp.Get(out: VarId) =>
									env.updated(out, evalState.get(PStateKey(id, ps, path, env)).map(out.codec.decodeEx))

								case PStateOp.Put(v: VarId) => {
									evalState.put(PStateKey(id, ps, path, env), Some(v.codec.encode(env(v))))
									env
								}
							})
						}.flatMap { updatedEnvs =>
							continueTopology(tail, updatedEnvs)
						}

					case Node.PStateUpdate(_pos, ps, path, ins, fn) =>
						val f = fn // TODO is this necessary?
						envs.traverse_ { env =>
							val key = PStateKey(id, ps, path, env)
							val current = evalState.get(key).map(path.codec.decodeEx)
							val args = ins.map(env(_))
							val updated = f(current, args).map(path.codec.encode)
							evalState.put(key, updated)
							IO.unit
						}.flatMap { _ =>
							continueTopology(tail, envs)
						}

					case Node.LocalAppend(dep, value) =>
						envs.traverse_ { env =>
							val v = env(value)
							evalState.append(dep, value.codec.encode(v))
							IO.unit
						} *> continueTopology(tail, envs)
				})
			}
		}
	}
}
