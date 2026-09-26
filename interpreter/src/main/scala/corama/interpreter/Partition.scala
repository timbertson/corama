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

class Partition(
	receivers: EvalReceivers,
	metadata: ClusterMetadata,
	topologies: Topologies,
	taskRouter: TaskRouter
) {
	import Storage._
	private val idList = topologies.ids.toList

	private val pstates: Map[String, PState] = {
		idList.mapFilter { id =>
			if (id.namespace == IdentityNamespace.PState) {
				Some(id.name -> new PState())
			} else {
				None
			}
		}.toMap
	}
	
	private val depots: Map[String, Depot] = idList.mapFilter { id =>
		if (id.namespace == IdentityNamespace.Depot) {
			Some(id.name -> new Depot())
		} else {
			None
		}
	}.toMap

	// Run a topology over some values by installing its results into the global GRPC service until completion
	private def runTopologyEval[R](
		topologyId: TopologyId,
		output: VarId,
		agg: Agg[R],
		start: EdgeId,
		nodes: List[Node],
		envs: List[Env],
	)(using Log[IO]): IO[R] = {
		EvalSink.local(output, agg, start).flatMap { results =>
			// install into GRPC listener so other partitions can send results
			receivers.addRunning(results) { origin =>
				// continue could throw (from user functions), so wrap it in IO.defer
				val continue = this.continue(origin, topologyId, results, start, nodes, envs)
				IO.defer(continue).attempt.flatMap {
					// TODO: how to apply backpressure if clients are not waiting?
					case Right(_) => IO.unit

					// One bad record must not stop the topology: Rama retries the record and
					// keeps consuming. There is no retry here, so the failure is reported to
					// whoever appended it and consumption continues.
					case Left(e) => results.fail(e)
				} *> results.await
			}
		}
	}

	def invoke[S, A, R](q: QueryTopology[S, R], value: S)(using Log[IO]): IO[R] = {
		val t = q.topology
		val env: Env = Map(t.builder.input.vid -> value)
		runTopologyEval(t.id, t.output.vid, t.agg, EdgeId.startingEdge(t.id), t.compiled._1, List(env))
	}

	// TODO: cancel running when this node no longer owns this partition
	def run(using Log[IO]): IO[Nothing] = {
		topologies.topologies.filter(_.builder.source.namespace == IdentityNamespace.Depot).parTraverse_ { topology =>
			// TODO: parallel? / chunks?
			this.depots(topology.builder.source.name).stream.foreach { record =>
				val env = Map(topology.input.vid -> record.value)
				for {
					resultsOpt <- record.tracker.traverse(tracker => IO.fromTry(tracker.forTopology(topology)))
					// TODO: skip all this when results are not tracked
					results = resultsOpt.getOrElse(???)
					startingEdge = EdgeId.startingEdge(topology.id)
					_ <- runTopologyEval(topology.id, topology.output.vid, topology.agg, startingEdge, topology.compiled._1, List(env))
				} yield ()
			}.compile.drain *> IO.raiseError(new RuntimeException("depot consume terminated"))
		} *> IO.never
	}
	
	// TODO: returning an IO of a lens is a nuisance
	def access[V](pstate: corama.PState, path: NonEmptyList[Any]): Storage.Lens[V] =
		Storage.Lens[V](this.pstate(pstate.id.name), path)

	def getKeys[K, V](pstate: corama.PStateKV[K, V]): IO[Set[K]] = {
		this.pstate(pstate.id.name).inner.get.map(_.keySet.map(_.asInstanceOf[K]))
	}

	def emit[A](depot: corama.Depot[A], value: A): IO[Unit] = {
		this.depot(depot.id.name).inner.update(vec => vec.appended(Record(value, None)))
	}

	/** As `emit`, but with ack tracking */
	def spawn[A](depot: corama.Depot[A], value: A, tracker: EvalResults): IO[Unit] = {
		this.depot(depot.id.name).inner.update { vec =>
			vec.appended(Record(value, Some(tracker)))
		}
	}

	def readDepot[A](depot: corama.Depot[A]): IO[Vector[A]] = {
		this.depot(depot.id.name).inner.get.map(vec => vec.map(_.value.asInstanceOf[A]))
	}

	private def pstate(name: String) = pstates.getOrElse(name, throw new RuntimeException(s"Unknown pstate: ${name}"))
	private def depot(name: String) = depots.getOrElse(name, throw new RuntimeException(s"Unknown depot: ${name}"))

	private def compilePath(path: CompiledPath, env: Env): NonEmptyList[Any] = {
		import CompiledPath._
		path.path.map {
			case Literal(inner) => inner
			case Variable(id) => env(id)
		}
	}

	/** Runs a list of nodes with the given envs. Results are returned to `results`.
		*/
	private [corama] def continue[R](
		origin: Origin,
		topologyId: TopologyId,
		evalSink: EvalSink[R],
		edgeId: EdgeId,
		nodes: List[Node],
		envs: List[Env],
	)(using Log[IO]): IO[Unit] = {
		def continueTopology(nodes: List[Node], envs: List[Env], nextEdgeId: EdgeId = edgeId) =
			// supplies evalSink since those don't change
			continue(origin, topologyId, evalSink, nextEdgeId, nodes, envs)

		import Node.DynamicOp
		import Node.PStateOp

		nodes match {
			case Nil =>
				evalSink.completeList(edgeId, envs.map(_(evalSink.varId).asInstanceOf[R]))
			
			case node :: tail => {
				node.op match {
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
						envs.parTraverse { env =>
							// TODO: skip all the bookkeeping if there aren't any
							// partition jumps in `block`?
							// Note: reusing parent `edgeId` here, but there's no confusion because this is a new EvalId
							runTopologyEval(topologyId, out, agg, edgeId, block, List(env)).map { result =>
								env.updated(out, result)
							}
						}.flatMap { outputEnvs =>
							continueTopology(tail, outputEnvs)
						}
					}
					
					case Node.PStateNode(ps, path, op) =>
						val state = pstate(ps.name)
						envs.traverse { env =>
							val rawPath = compilePath(path, env)
							def applyOp(current: Option[Any]): (Option[Any], Env) = {
								def currentSet: Set[Any] = {
									println(s"accessing current set @ $rawPath: ${current}")
									current.fold(Set.empty[Any])(_.asInstanceOf[Set[Any]])
								}
								op match {
									case PStateOp.Add(v) => (Some(currentSet.incl(env(v))), env)
									case PStateOp.Remove(v) => (Some(currentSet.excl(env(v))), env)
									case PStateOp.Select(_intermediate, out, options) => {
										// TODO: implement limit
										val ord: Ordering[Any] = options.ord.asInstanceOf[Ordering[Any]]
										val fullList = currentSet.toList.sorted(using ord)
										val result = options match {
											case SelectOptions(afterVar, maxItems, _) =>
												val afterList = afterVar.fold(fullList){ afterVar =>
													val afterVal = env(afterVar.vid)
													fullList.dropWhile(x => ord.lteq(x, afterVal))
												}
												val afterLimit = maxItems.fold(afterList) { max =>
													afterList.take(max)
												}
												afterLimit
										}
										println(s"Select returned $result into var $out")
										(current, env.updated(out, result))
									}

									case PStateOp.Get(_intermediate, out: VarId) => (current, env.updated(out, current))
									case PStateOp.Put(v: VarId) => (Some(env(v)), env)
									// case PStateOp.Delete(v: VarId) => (None, env)
								}
							}
							state.modify(rawPath)(applyOp)
						}.flatMap { updatedEnvs =>
							continueTopology(tail, updatedEnvs)
						}

					case Node.PStateUpdate(_pos, ps, path, ins, fn) =>
						val f = fn // TODO is this necessary?
						val state = pstate(ps.name)
						envs.traverse_ { env =>
							val rawPath = compilePath(path, env)
							def applyOp(current: Option[Any]): (Option[Any], Unit) = {
								val args = ins.map(env(_))
								(f(current, args), ())
							}
							state.modify(rawPath)(applyOp)
						}.flatMap { _ =>
							continueTopology(tail, envs)
						}

					case Node.LocalAppend(dep, value) =>
						val state = depot(dep.name)
						envs.traverse_ { env =>
							val v = env(value)
							// `None`: the new record starts its own event tree rather than extending
							// the appender's. An assumption, not a verified fact — FLOW-NOTES.md.
							state.inner.update(_.appended(Record(v, None)))
						} *> continueTopology(tail, envs)
				}
			}
		}
	}
}
