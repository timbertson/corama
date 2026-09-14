package corama.interpreter

import corama._
import cats.data.NonEmptyList
import fs2.Stream
import cats.syntax.all._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try
import cats.effect.IO
import cats.effect.kernel.{Deferred, Outcome, Ref}
import cats.effect.unsafe
import corama.interpreter.Interpreter.Env
import scala.util.Random
import fs2.concurrent.SignallingRef
import fs2.Chunk
import izumi.reflect.Tag

import corama.internal._
import scala.util.control.NoStackTrace
import org.tpolecat.sourcepos.SourcePos
import corama.interpreter.Interpreter.EvalError

/** Simple interpreter: runs a [[Flow]] as plain Scala over in-memory state.
  *
  * An environment is one *branch* of execution, mapping vars to values.
  * A step maps one environment to 0..n environments — `Each` gives
  * one, `Filter` gives zero or one, `Explode` gives n.
  */
object Interpreter {
	type Env = Map[VarId, Any]
	class EvalError(msg: String, pos: SourcePos, cause: Throwable) extends RuntimeException(s"$msg defined at $pos", cause) with NoStackTrace
}

class Partitions(interpreter: Interpreter, numPartitions: Int) {
	private val partitions: Array[Partition] = Array.fill(numPartitions)(new Partition(interpreter))
	val all: List[Partition] = partitions.toList
	def get(partition: Int): Try[Partition] = {
		Try(partitions(partition))
	}
	def partitionKey(value: Any) = partitions(rawPartitionKey(value))

	def random: Partition = {
		partitions(Random.nextInt(numPartitions))
	}

	def groupByPartitions[A](values: List[A], keyFn: A => Any): List[(Partition, List[A])] =
		values.groupBy(v => rawPartitionKey(keyFn(v))).toList.map { case (p, items) =>
			(partitions(p) -> items)
		}

	// TODO make much better
	private def rawPartitionKey(value: Any) = {
		val mod = value.hashCode() % numPartitions
		if (mod < 0) {
			mod + numPartitions
		} else {
			mod
		}
	}

}

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
			done    <- Deferred[IO, Either[Throwable, Map[TopologyId, List[Any]]]]
			_       <- IO.whenA(subscribers == 0)(done.complete(Right(Map.empty)).void)
		} yield new AckTracker(pending, returns, done)
}

class Interpreter(val topologies: Topologies, numPartitions: Int = 10) {
	import Interpreter._
	val partitions = new Partitions(this, numPartitions)

	def run: IO[Nothing] = {
		partitions.all.parTraverse_(p => p.run) *> IO.raiseError(new RuntimeException("IO[Nothing] completed"))
	}
	
	def running[A](block: IO[A]): IO[A] = {
		IO.race(block, run).map(_.merge)
	}
	
	private def accessPath[V](pstate: corama.PState, path: NonEmptyList[Any]): Storage.Lens[V] =
		this.partitions.partitionKey(path.head).access(pstate, path)

	def access[K, V](pstate: corama.PStateKV[K, V], key: K): Storage.Lens[V] =
		accessPath[V](pstate.inner, NonEmptyList.one(key))

	def access[K, V](pstate: corama.PStateKVSet[K, V], key: K): Storage.Lens[Set[V]] =
		accessPath[Set[V]](pstate.inner, NonEmptyList.one(key))

	def access[V: Tag](pstate: corama.PState, root: Any, path: Any*): Storage.Lens[V] =
		accessPath[V](pstate, NonEmptyList(root, path.toList))

	def getKeys[K, V](pstate: corama.PStateKV[K, V]): IO[Set[K]] =
		this.partitions.all.flatTraverse { partition =>
			partition.getKeys(pstate).map(_.toList)
		}.map(_.toSet)

	def emit[A](depot: corama.Depot[A], value: A): IO[Unit] =
		// TODO use the depot's partitioning strategy
		this.partitions.partitionKey(value).emit(depot, value)

	/** Topologies driven by this depot — the count an [[AckTracker]] waits for. */
	private[corama] def subscribers(depot: Identity): List[Topology[?, ?]] =
		topologies.topologies.filter(_.builder.source == depot)

	def append[A, R](depot: corama.Depot[A], value: A, level: AckLevel[R] = AckLevel.Ack): IO[R] =
		level match {
			// TODO: these branches should differ when we model replication
			case AckLevel.NoAck     => emit(depot, value)
			case AckLevel.AppendAck => emit(depot, value)

			case AckLevel.Ack =>
				for {
					tracker <- AckTracker.create(subscribers(depot.id).length)
					// TODO use the depot's partitioning strategy
					_       <- partitions.partitionKey(value).append(depot, value, tracker)
					raw     <- tracker.await
				} yield new AckReturns(raw, topologies)
		}

	def readDepot[A](depot: corama.Depot[A]): IO[Vector[A]] =
		this.partitions.all.toVector.flatTraverse { partition =>
			partition.readDepot(depot)
		}

	/** Invoke a query topology, as a client would.
	  */
	def invoke[S, A, R](q: QueryTopology[S, R], value: S): IO[R] = {
		val topology = q.topology
		val env: Env = Map(topology.builder.input.vid -> value)
		val agg = topology.agg
		partitions.random.continue(topology.compiled._1, List(env)).map { resultEnvs =>
			resultEnvs.foldLeft(agg.zero) { (acc, resultEnv) =>
				agg.combineAny(acc, resultEnv.getOrElse(topology.compiled._2.vid, throw new RuntimeException("output var not present in query result")))
			}
		}
	}
}

// TODO this could possibly take a pair of (ids, partitioned) - it doesn't require the full interpreter
class Partition(interpreter: Interpreter) {
	import Storage._

	private val idList = interpreter.topologies.ids.toList

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
	
	val run: IO[Nothing] = {
		interpreter.topologies.topologies.filter(_.builder.source.namespace == IdentityNamespace.Depot).parTraverse_ { top =>
			this.depots(top.builder.source.name).stream.foreach { record =>
				// TODO: chunks?
				val env = Map(top.input.vid -> record.value)
				val ack = record.tracker.map(tracker => AckSink(top.builder.id, top.compiled._2.vid, tracker))

				// `continue` returning means the whole event tree for this record is
				// exhausted — every fanned-out branch, on every partition it reached.
				// `IO.defer` ensures exceptions in user functions (like each) don't escape.
				IO.defer(continue(top.compiled._1, List(env), ack)).void.attempt.flatMap {
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
	private [corama] def continue(nodes: List[Node], envs: List[Env], ack: Option[AckSink] = None): IO[List[Env]] =
		import Node.DynamicOp
		import Node.PStateOp

		nodes match {
			case Nil => {
				ack.traverse_ { sink =>
					sink.addResults(envs)
				}.as(envs)
			}

			case Node.Dynamic(pos, ins, fn, op) :: tail => {
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
				continue(tail, results, ack)
			}

			case Node.Assign(in, out) :: tail =>
				continue(tail, envs.map { env =>
					env.updated(out, env(in))
				})

			case Node.If(cond, ifBranch, elseBranch, out) :: tail =>
				envs.flatTraverse { env =>
					if (env(cond).asInstanceOf[Boolean]) {
						continue(ifBranch ++ tail, List(env))
					} else {
						continue(elseBranch ++ tail, List(env))
					}
				}

			case Node.Partition(in) :: tail => {
				interpreter.partitions.groupByPartitions(envs, env => env(in)).toList.flatTraverse { case (partition, penvs) =>
					partition.continue(tail, penvs, ack)
				}
			}

			case Node.Combine(block, agg, collectFrom, out) :: tail => {
				envs.traverse { env =>
					continue(block, List(env), ack).map { innerEnvs =>
						env.updated(out, innerEnvs.foldLeft(agg.zero) { (acc, innerEnv) =>
							agg.combineAny(acc, innerEnv(collectFrom))
						})
					}
				}.flatMap { outputEnvs =>
					continue(tail, outputEnvs, ack)
				}
			}
			
			case Node.PStateNode(ps, path, op) :: tail =>
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
					continue(tail, updatedEnvs, ack)
				}

			case Node.PStateUpdate(_pos, ps, path, ins, fn) :: tail =>
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
					continue(tail, envs, ack)
				}

			case Node.LocalAppend(dep, value) :: tail =>
				val state = depot(dep.name)
				envs.traverse_ { env =>
					val v = env(value)
					// `None`: the new record starts its own event tree rather than extending
					// the appender's. An assumption, not a verified fact — FLOW-NOTES.md.
					state.inner.update(_.appended(Record(v, None)))
				} *> continue(tail, envs, ack)
		}
}
