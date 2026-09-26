package corama.interpreter

import cats.syntax.all.*
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe
import corama.*
import corama.error.Util.*
import corama.internal.*
import corama.interpreter.Env
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.SignallingRef
import izumi.reflect.Tag
import org.tpolecat.sourcepos.SourcePos

import scala.util.*
import scala.util.control.NoStackTrace
import flogger.*
import cats.effect.Resource

/**
	* An environment is one *instance* of a topology execution, mapping vars to values.
	* A node maps one environment to 0..n environments — `Each` gives
	* one, `Filter` gives zero or one, `Explode` gives n.
	*
	* Continuing execution on a different node does not require any waiting on the source.
	* The evaluation is handed off, and any acknowledgement is routed to the original
	* requestor, not any of the intermediate nodes which played a part.
	*/
type Env = Map[VarId, Any]
class EvalError(msg: String, pos: SourcePos, cause: Throwable) extends RuntimeException(s"$msg defined at $pos", cause) with NoStackTrace

object Storage {
	class PState() {
		val inner = Ref.unsafe[IO, Map[Any,Any]](Map.empty)
		def modify[R](fullPath: NonEmptyList[Any])(fn: (Option[Any]) => (Option[Any], R)): IO[R] = {
			inner.modify { map =>
				def recurse(path: NonEmptyList[Any], map: Map[Any, Any]): (Map[Any, Any], R) = {
					NonEmptyList.fromList(path.tail) match {
						case Some(tail) => {
							val child = map.get(path.head).fold(Map.empty[Any, Any])(_.asInstanceOf[Map[Any, Any]])
							val (newMap, result) = recurse(tail, child)
							(map.updated(path.head, newMap), result)
						}
						case None => {
							val current = map.get(path.head)
							println(s"modifying @ $fullPath: ${current}")
							val (newValue, result) = fn(current)
							println(s"-> $newValue")
							val newMap = newValue match {
								case Some(x) => map.updated(path.head, x)
								case None => map.removed(path.head)
							}
							(newMap, result)
						}
					}
				}
				recurse(fullPath, map)
			}
		}
	}
	class Depot() {
		val inner = SignallingRef.of[IO, Vector[Record]](Vector.empty).unsafeRunSync()(using unsafe.IORuntime.global)
		def stream: Stream[IO, Record] = inner.discrete.mapAccumulate(0) { (seen, values) =>
			val additions = values.drop(seen)
			(seen + additions.length, additions)
		}.mapChunks { ch => ch.flatMap(tup => Chunk.from(tup._2)) }
	}

	class Lens[V](pstate: PState, path: NonEmptyList[Any]) {
		def put(v: V): IO[Unit] = pstate.modify(path) { _ => (Some(v), ()) }
		def get: IO[Option[V]] = pstate.modify(path) { x => (x, x.asInstanceOf[Option[V]]) }
	}
}

/** An appended value, plus a slot for acks to accumulate.
	*
	* `tracker` is `None` for `emit`, where nobody is waiting.
	*/
private[corama] final case class Record(value: Any, tracker: Option[TopologiesResults])

// Tracker for every topology spawned by a given depot append (or query)
private[corama] final class TopologiesResults(results: Map[TopologyId, LocalTopologyResults[?]]) {
	def forTopology[R](topology: Topology[?, R]): Try[LocalTopologyResults[R]] =
		results.get(topology.id)
			.toRight(err(s"TopologyResults(${topology.id}) not found")).toTry
			.map(_.asInstanceOf[LocalTopologyResults[R]])

	def await: IO[Map[TopologyId, Any]] = results.toList.traverse { (id, results) =>
		results.await.map(id -> _)
	}.map(_.toMap)
}

private[corama] object TopologiesResults {
	def apply(topologies: List[Topology[?, ?]])(using Log[IO]): IO[TopologiesResults] = {
		topologies.traverse { topology =>
			TopologyResults.local[topology.Result](topology.output.vid, topology.agg, EdgeId.union(topologies.map(_.startingEdge)))
				.map(topology.id -> _)
		}.map { topologyResults =>
			new TopologiesResults(topologyResults.toMap)
		}
	}
}

class Interpreter(
	val topologies: Topologies,
	metadata: ClusterMetadata,
	val instances: Vector[InstancePartitions],
)(using Log[IO]) {
	import Interpreter._
	val run: IO[Nothing] = {
		instances.parTraverse_(_.run(this)) *> neverReached
	}
	
	// def running[A](block: IO[A])(using Log[IO]): IO[A] = {
	// 	IO.race(block, run).map(_.merge)
	// }

	private[interpreter] def leader(taskId: TaskId): IO[InstancePartitions] = {
		IO.pure(instances(taskId.id))
	}
	
	private[interpreter] def partition(partitionId: PartitionId): IO[Partition] = {
		val taskId = metadata.taskFor(partitionId)
		leader(taskId).flatMap(_.get(taskId, partitionId))
	}

	private[interpreter] def instance(instanceId: InstanceId): IO[InstancePartitions] = {
		IO.fromEither(instances.find(_.instance === instanceId).toRight(err(s"Unknown instance: $instanceId")))
	}
	
	private def accessPath[V](pstate: corama.PState, path: NonEmptyList[Any]): IO[Storage.Lens[V]] =
		partition(metadata.partitionKey(path.head)).map(_.access(pstate, path))

	def access[K, V](pstate: corama.PStateKV[K, V], key: K): IO[Storage.Lens[V]] =
		accessPath[V](pstate.inner, NonEmptyList.one(key))

	def access[K, V](pstate: corama.PStateKVSet[K, V], key: K): IO[Storage.Lens[Set[V]]] =
		accessPath[Set[V]](pstate.inner, NonEmptyList.one(key))

	def access[V: Tag](pstate: corama.PState, root: Any, path: Any*): IO[Storage.Lens[V]] =
		accessPath[V](pstate, NonEmptyList(root, path.toList))

	// def getKeys[K, V](pstate: corama.PStateKV[K, V]): IO[Set[K]] =
	// 	this.partitions.all.flatTraverse { partition =>
	// 		partition.getKeys(pstate).map(_.toList)
	// 	}.map(_.toSet)

	def emit[A](depot: corama.Depot[A], value: A): IO[Unit] =
		// TODO use the depot's partitioning strategy
		partition(metadata.partitionKey(value)).flatMap(_.emit(depot, value))

	/** Topologies driven by this depot — used to populate [[TopologiesResults]] */
	private[corama] def subscribers(depot: Identity): List[Topology[?, ?]] =
		topologies.topologies.filter(_.builder.source == depot)

	def append[A, R](depot: corama.Depot[A], value: A, level: AckLevel[R] = AckLevel.Ack)(using Log[IO]): IO[R] =
		level match {
			// TODO: these branches should differ when we model replication
			case AckLevel.NoAck => emit(depot, value)
			case AckLevel.AppendAck => emit(depot, value)

			case AckLevel.Ack =>
				for {
					results <- TopologiesResults(subscribers(depot.id))
					// TODO use the depot's partitioning strategy
					partition <- partition(metadata.partitionKey(value))
					_ <- partition.spawn(depot, value, results)
					raw <- results.await
				} yield new AckReturns(raw, topologies)
		}

	// def readDepot[A](depot: corama.Depot[A]): IO[Vector[A]] =
	// 	this.partitions.all.toVector.flatTraverse { partition =>
	// 		partition.readDepot(depot)
	// 	}

	/** Invoke a query topology, as a client would.
		*/
	def invoke[S, A, R](q: QueryTopology[S, R], value: S)(using Log[IO]): IO[R] = {
		for {
			partitionId <- metadata.randomPartition
			partition <- partition(partitionId)
			result <- partition.invoke(this, q, value)
		} yield result
	}

	val taskExecutor: TaskExecutor = {
		val interpreter = this
		new TaskExecutor {
			override def continue(
				task: TaskId,
				topologyId: TopologyId,
				afterNode: NodeId,
				edge: EdgeId,
				envs: PartitionBatch[Env],
				origin: Origin,
			): IO[Unit] = {
				// TODO seems inefficient
				leader(task).flatMap(_.taskExecutor(interpreter).continue(task, topologyId, afterNode, edge, envs, origin))
			}
		}
	}

	val router: ResultsRouter = new ResultsRouter {
		def remoteResults[R](topology: Topology[?, ?], continuation: Continuation, origin: Origin): TopologyResults[R] = {
			val (outVarId, agg) = continuation match {
				case _: Continuation.Basic => (topology.output.vid, topology.agg)
				case c: Continuation.WithinAggregation => (c.out, c.agg)
			}

			new TopologyResults[R] {
				override val varId: VarId = outVarId

				override def notify(edge: EdgeId, value: Option[R]): IO[Unit] = {
					instance(origin.instanceId).flatMap(_.receivers.invoke(origin, edge, value))
				}

				override def completeList(edge: EdgeId, values: List[R]): IO[Unit] =
					notify(edge, Option.when(values.nonEmpty) {
						(agg.asInstanceOf[Agg[R]]).merge(values)
					})
			}
		}
	}
}

object Interpreter {
	def createCluster(
		topologies: Topologies,
		instances: Vector[InstanceId],
		numPartitions: Int,
	)(using Log[IO]): ActiveResource[IO, Interpreter] =
		ActiveResource(
			Resource.eval(ClusterMetadata(instances, numPartitions))
				.flatMap(metadata => apply(topologies, metadata).resource)
		)

	def apply(topologies: Topologies, metadata: ClusterMetadata)(using Log[IO]): ActiveResource[IO, Interpreter] = {
		ActiveResource(metadata.instances.traverse { instance =>
			EvalReceivers(instance).flatMap { receivers =>
				Resource.eval(InstancePartitions(instance, topologies, metadata, receivers))
			}
		}.map { instancePartitions =>
			val interpreter = new Interpreter(topologies, metadata, instancePartitions)
			(interpreter, interpreter.run)
		})
	}
}

// The partitions this instance is leader for
class InstancePartitions(
	val instance: InstanceId,
	topologies: Topologies,
	metadata: ClusterMetadata,
	val receivers: EvalReceivers,
	partitions: Ref[IO, Map[PartitionId, Partition]]
) {

	def taskExecutor(interpreter: Interpreter)(using Log[IO]): TaskExecutor = new TaskExecutor {
		override def continue(
			task: TaskId,
			topologyId: TopologyId,
			afterNode: NodeId,
			edge: EdgeId,
			envs: PartitionBatch[Env],
			origin: Origin,
		): IO[Unit] = {
			for {
				topology <- IO.fromTry(topologies.get(topologyId))
				continuation <- IO.fromTry(topology.continuationAfter(afterNode))
				results = interpreter.router.remoteResults(topology, continuation, origin)
				newEdges <- envs.value.parTraverse { case (partitionId, envs) =>
					for {
						newEdge <- EdgeId.generate
						partition <- get(task, partitionId)
						_ <- partition.continue(origin, interpreter, topologyId, results, newEdge, continuation.nodes, envs)
					} yield newEdge
				}
				_ <- results.notify(EdgeId.union(edge :: newEdges), None)
			} yield ()
		}
	}

	// TODO: only if we are currently leader for task
	def get(task: TaskId, partition: PartitionId): IO[Partition] = {
		if (metadata.taskFor(partition) === task) {
			val newPartition = new Partition(receivers, metadata, topologies)
			partitions.modify { map =>
				map.get(partition) match {
					case None => {
						(map.updated(partition, newPartition), newPartition)
					}
					case Some(p) => (map, p)
				}
			}
		} else {
			IO.raiseError(new RuntimeException(s"$partition is not owned by $task"))
		}
	}
	
	// TODO: respond /guard against changes in leadership
	def run(interpreter: Interpreter)(using Log[IO]) = {
		metadata.leaders.get.flatMap { leaders =>
			leaders.toList.mapFilter { case (task, leadership) =>
				Option.when(leadership.instance === instance)(task)
			}.parTraverse_ { taskId =>
				metadata.partitionsIn(taskId).parTraverse_ { partitionId =>
					this.get(taskId, partitionId).flatMap(_.run(interpreter))
				}
			}
		}
	}
}

object InstancePartitions {
	def apply(
		instance: InstanceId,
		topologies: Topologies,
		metadata: ClusterMetadata,
		receivers: EvalReceivers,
	) = Ref[IO].of(Map.empty[PartitionId, Partition]).map { partitionMap =>
		new InstancePartitions(instance, topologies, metadata, receivers, partitionMap)
	}
}

class Partition(receivers: EvalReceivers, metadata: ClusterMetadata, topologies: Topologies) {
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
		interpreter: Interpreter,
		topologyId: TopologyId,
		output: VarId,
		agg: Agg[R],
		start: EdgeId,
		nodes: List[Node],
		envs: List[Env],
	)(using Log[IO]): IO[R] = {
		TopologyResults.local(output, agg, start).flatMap { results =>
			// install into GRPC listener so other partitions can send results
			receivers.addRunning(results) { origin =>
				// continue could throw (from user functions), so wrap it in IO.defer
				val continue = this.continue(origin, interpreter, topologyId, results, start, nodes, envs)
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

	def invoke[S, A, R](interpreter: Interpreter, q: QueryTopology[S, R], value: S)(using Log[IO]): IO[R] = {
		val t = q.topology
		val env: Env = Map(t.builder.input.vid -> value)
		runTopologyEval(interpreter, t.id, t.output.vid, t.agg, t.startingEdge, t.compiled._1, List(env))
	}

	// TODO: cancel running when this node no longer owns this partition
	def run(interpreter: Interpreter)(using Log[IO]): IO[Nothing] = {
		interpreter.topologies.topologies.filter(_.builder.source.namespace == IdentityNamespace.Depot).parTraverse_ { topology =>
			// TODO: parallel? / chunks?
			this.depots(topology.builder.source.name).stream.foreach { record =>
				val env = Map(topology.input.vid -> record.value)
				for {
					resultsOpt <- record.tracker.traverse(tracker => IO.fromTry(tracker.forTopology(topology)))
					// TODO: skip all this when results are not tracked
					results = resultsOpt.getOrElse(???)
					_ <- runTopologyEval(interpreter, topology.id, topology.output.vid, topology.agg, topology.startingEdge, topology.compiled._1, List(env))
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
	def spawn[A](depot: corama.Depot[A], value: A, tracker: TopologiesResults): IO[Unit] = {
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
		interpreter: Interpreter,
		topologyId: TopologyId,
		results: TopologyResults[R],
		edgeId: EdgeId,
		nodes: List[Node],
		envs: List[Env],
	)(using Log[IO]): IO[Unit] = {
		def continueTopology(nodes: List[Node], envs: List[Env], nextEdgeId: EdgeId = edgeId) =
			// supplies interpreter & results since those don't change
			continue(origin, interpreter, topologyId, results, nextEdgeId, nodes, envs)

		import Node.DynamicOp
		import Node.PStateOp

		nodes match {
			case Nil =>
				results.completeList(edgeId, envs.map(_(results.varId).asInstanceOf[R]))
			
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
								interpreter.taskExecutor.continue(taskId, topologyId, node.id, childEdge, partitionedEnvs, origin).as(childEdge)
							}
						}.flatMap { spawnedEdges =>
							val closeOriginAndOpenChildren = spawnedEdges.foldLeft(edgeId)(_.xor(_))
							results.notify(closeOriginAndOpenChildren, None)
						}
					}

					case Node.Combine(block, agg, collectFrom, out) => {
						envs.parTraverse { env =>
							// TODO: skip all the bookkeeping if there aren't any
							// partition jumps in `block`?
							// Note: reusing parent `edgeId` here, but there's no confusion because this is a new EvalId
							runTopologyEval(interpreter, topologyId, out, agg, edgeId, block, List(env)).map { result =>
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
