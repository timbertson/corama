package corama.interpreter

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.unsafe
import cats.syntax.all.*
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

case class EvalId(id: Long)
case class EvalSink(origin: InstanceId, id: EvalId)

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
private[corama] final case class Record(value: Any, tracker: Option[AckTracker])

/** Watches one append's event tree, and collects what its topologies ack return.
	*
	* The tree is tracked by counting: the append registers one pending slot per topology
	* subscribed to the depot, each reports when its flow has run to completion, and the
	* last one out completes the append.
	*
	* The count is fixed up front rather than incremented as topologies pick the record up.
	*
	* `PartitionAppend` deliberately does *not* extend the tree; TODO verify this matches
	* rama's behaviour.
	*/
private[corama] final class AckTracker(
	pending: Ref[IO, Int],
	returns: Ref[IO, Map[TopologyId, List[Any]]],
	done: Deferred[IO, Either[Throwable, Map[TopologyId, List[Any]]]]) {

	/** Add a value to the return set for a specific topology
		*/
	def ackReturn(topology: TopologyId, value: Any): IO[Unit] =
		returns.update(m => m.updated(topology, m.getOrElse(topology, Nil) :+ value))

	/** One topology finished; the last one out completes the append. */
	def finish: IO[Unit] =
		pending.updateAndGet(_ - 1).flatMap { remaining =>
			if (remaining <= 0) returns.get.flatMap(r => done.complete(Right(r)).void) else IO.unit
		}

	/** A topology failed. Rama retries the record; here the append fails.
	 * TODO reconsider this.
	 */
	def fail(err: Throwable): IO[Unit] = done.complete(Left(err)).void

	def await: IO[Map[TopologyId, List[Any]]] = done.get.rethrow
}

/** An [[AckTracker]] with the identity of the topology reporting into it. */
private[corama] final case class AckSink(topology: TopologyId, outVar: VarId, tracker: AckTracker) {
	def addResults(envs: List[Env]) = {
		envs.traverse_ { env =>
			tracker.ackReturn(topology, env(outVar))
		}
	}
	def fail(err: Throwable): IO[Unit] = tracker.fail(err)
	def finish: IO[Unit] = tracker.finish
}

private[corama] object AckTracker {

	/** `subscribers` is how many topologies must report before the append completes.
		* Zero means a depot nothing consumes, which completes immediately — the append is
		* done once it is durable, since there is no event tree to wait for.
		*/
	def create(subscribers: Int): IO[AckTracker] =
		for {
			pending <- Ref.of[IO, Int](subscribers)
			returns <- Ref.of[IO, Map[TopologyId, List[Any]]](Map.empty)
			done		<- Deferred[IO, Either[Throwable, Map[TopologyId, List[Any]]]]
			_				<- IO.whenA(subscribers == 0)(done.complete(Right(Map.empty)).void)
		} yield new AckTracker(pending, returns, done)
}

class Interpreter(
	val topologies: Topologies,
	metadata: ClusterMetadata,
	val instances: Vector[InstancePartitions],
	val router: TaskExecutor
) {
	import Interpreter._
	def run: IO[Nothing] = {
		instances.parTraverse_(_.run(this)) *> neverReached
	}
	
	def running[A](block: IO[A]): IO[A] = {
		IO.race(block, run).map(_.merge)
	}
	
	private[interpreter] def partition(partitionId: PartitionId): IO[Partition] = {
		val taskId = metadata.taskFor(partitionId)
		instances(taskId.id).get(taskId, partitionId)
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

	/** Topologies driven by this depot — the count an [[AckTracker]] waits for. */
	private[corama] def subscribers(depot: Identity): List[Topology[?, ?]] =
		topologies.topologies.filter(_.builder.source == depot)

	def append[A, R](depot: corama.Depot[A], value: A, level: AckLevel[R] = AckLevel.Ack): IO[R] =
		level match {
			// TODO: these branches should differ when we model replication
			case AckLevel.NoAck			=> emit(depot, value)
			case AckLevel.AppendAck => emit(depot, value)

			case AckLevel.Ack =>
				for {
					tracker <- AckTracker.create(subscribers(depot.id).length)
					// TODO use the depot's partitioning strategy
					partition <- partition(metadata.partitionKey(value))
					_				<- partition.append(depot, value, tracker)
					raw			<- tracker.await
				} yield new AckReturns(raw, topologies)
		}

	// def readDepot[A](depot: corama.Depot[A]): IO[Vector[A]] =
	// 	this.partitions.all.toVector.flatTraverse { partition =>
	// 		partition.readDepot(depot)
	// 	}

	/** Invoke a query topology, as a client would.
		*/
	def invoke[S, A, R](q: QueryTopology[S, R], value: S): IO[R] = {
		val topology = q.topology
		val env: Env = Map(topology.builder.input.vid -> value)
		val agg = topology.agg
		partition(metadata.randomPartition).flatMap { partition =>
			partition.continue(this, topology.id, topology.compiled._1, List(env)).map { resultEnvs =>
				resultEnvs.foldLeft(agg.zero) { (acc, resultEnv) =>
					agg.combineAny(acc, resultEnv.getOrElse(topology.compiled._2.vid, throw new RuntimeException("output var not present in query result")))
				}
			}
		}
	}
}

object Interpreter {
	def createCluster(
		topologies: Topologies,
		instances: Vector[InstanceId],
		numPartitions: Int,
	) = ClusterMetadata(instances, numPartitions).flatMap(metadata => apply(topologies, metadata))

	def apply(topologies: Topologies, metadata: ClusterMetadata) = {
		metadata.instances.traverse { instance =>
			InstancePartitions(instance, topologies, metadata)
		}.map { instancePartitions =>
			new Interpreter(topologies, metadata, instancePartitions, ???)
		}
	}
}

// The partitions this instance is leader for
class InstancePartitions(
	instance: InstanceId,
	topologies: Topologies,
	metadata: ClusterMetadata,
	partitions: Ref[IO, Map[PartitionId, Partition]]
) {
	def taskExecutor(interpreter: Interpreter): TaskExecutor = new TaskExecutor {
		def continue(task: TaskId, topology: TopologyId, afterNode: NodeId, envs: PartitionBatch[Env], sink: EvalSink): IO[Unit] = {
			// TODO: capture node continuation
			IO.fromTry(topologies.continuationAfter(topology, afterNode)).flatMap { nodes =>
				envs.value.parTraverse_ { case (partitionId, envs) =>
					get(task, partitionId).flatMap { partition =>
						partition.continue(interpreter, topology, nodes, envs, None /* TODO: sink */)
					}
				}
			}
		}
	}

	// TODO: only if we are currently leader for task
	def get(task: TaskId, partition: PartitionId): IO[Partition] = {
		if (metadata.taskFor(partition) === task) {
			val newPartition = new Partition(metadata, topologies)
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
	def run(interpreter: Interpreter) = {
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
	) = Ref[IO].of(Map.empty[PartitionId, Partition]).map { partitionMap =>
		new InstancePartitions(instance, topologies, metadata, partitionMap)
	}
}

class Partition(metadata: ClusterMetadata, topologies: Topologies) {
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
	
	// TODO: cancel running when this node no longer owns this partition
	def run(interpreter: Interpreter): IO[Nothing] = {
		interpreter.topologies.topologies.filter(_.builder.source.namespace == IdentityNamespace.Depot).parTraverse_ { top =>
			this.depots(top.builder.source.name).stream.foreach { record =>
				// TODO: chunks?
				val env = Map(top.input.vid -> record.value)
				val ack = record.tracker.map(tracker => AckSink(top.builder.id, top.compiled._2.vid, tracker))

				// `continue` returning means the whole event tree for this record is
				// exhausted — every fanned-out branch, on every partition it reached.
				// `IO.defer` ensures exceptions in user functions (like each) don't escape.
				IO.defer(continue(interpreter, top.id, top.compiled._1, List(env), ack)).void.attempt.flatMap {
					case Right(_) => record.tracker.traverse_(_.finish)

					// One bad record must not stop the topology: Rama retries the record and
					// keeps consuming. There is no retry here, so the failure is reported to
					// whoever appended it and consumption continues.
					case Left(e) =>
						record.tracker match {
							case Some(tracker) => tracker.fail(e)
							// Nobody is waiting, so swallowing would lose the error entirely.
							// Failing `run` is better than silently discarding
							case None => IO.raiseError(e)
						}
				}
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

	/** As `emit`, but the record carries a tracker for the topologies it will trigger. */
	def append[A](depot: corama.Depot[A], value: A, tracker: AckTracker): IO[Unit] = {
		this.depot(depot.id.name).inner.update(vec => vec.appended(Record(value, Some(tracker))))
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


	/** Runs a list of nodes with the given envs. Returns the resulting list of envs.
		*
		* `ack` is where `Node.AckReturn` reports, and is `None` for a query or for a record
		* appended with `emit` — nobody is waiting on either. It is passed along untouched
		* everywhere else, including across a partition hop, since Rama routes an ack return
		* to the *appender* rather than to the task that produced it.
		*/
	private [corama] def continue(
		interpreter: Interpreter,
		topologyId: TopologyId,
		nodes: List[Node],
		envs: List[Env],
		ack: Option[AckSink] = None
	): IO[List[Env]] = {
		def continueTopology(nodes: List[Node], envs: List[Env], ack: Option[AckSink]) =
			// supplies interpreter & topologyId since those don't change
			continue(interpreter, topologyId, nodes, envs, ack)

		import Node.DynamicOp
		import Node.PStateOp

		nodes match {
			case Nil => {
				ack.traverse_ { sink =>
					sink.addResults(envs)
				}.as(envs)
			}
			
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
						continueTopology(tail, results, ack)
					}

					case Node.Assign(in, out) =>
						continueTopology(tail, envs.map { env =>
							env.updated(out, env(in))
						}, ack)

					case Node.If(cond, ifBranch, elseBranch, out) =>
						envs.flatTraverse { env =>
							if (env(cond).asInstanceOf[Boolean]) {
								continueTopology(ifBranch ++ tail, List(env), ack)
							} else {
								continueTopology(elseBranch ++ tail, List(env), ack)
							}
						}

					case Node.Partition(in) => {
						// todo: parallel, but we shouldn't be returning the envs from here
						metadata.partitionBatches(envs, env => env(in)).toList.flatTraverse { case (taskId, partitionedEnvs) =>
							interpreter.router.continue(taskId, topologyId, node.id, partitionedEnvs, ???).void.as(Nil /* TODO */)
						}
					}

					case Node.Combine(block, agg, collectFrom, out) => {
						envs.traverse { env =>
							// TODO: this doesn't work across instances
							continueTopology(block, List(env), ack).map { innerEnvs =>
								env.updated(out, innerEnvs.foldLeft(agg.zero) { (acc, innerEnv) =>
									agg.combineAny(acc, innerEnv(collectFrom))
								})
							}
						}.flatMap { outputEnvs =>
							continueTopology(tail, outputEnvs, ack)
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
							continueTopology(tail, updatedEnvs, ack)
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
							continueTopology(tail, envs, ack)
						}

					case Node.LocalAppend(dep, value) =>
						val state = depot(dep.name)
						envs.traverse_ { env =>
							val v = env(value)
							// `None`: the new record starts its own event tree rather than extending
							// the appender's. An assumption, not a verified fact — FLOW-NOTES.md.
							state.inner.update(_.appended(Record(v, None)))
						} *> continueTopology(tail, envs, ack)
				}
				}
			}

	}
}
