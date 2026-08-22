package flow

import fs2.Stream
import cats.syntax.all._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try
import cats.effect.IO
import cats.effect.kernel.{Deferred, Outcome, Ref}
import cats.effect.unsafe
import flow.Interpreter.Env
import scala.util.Random
import fs2.concurrent.SignallingRef
import fs2.Chunk

import flow.internal._

/** Simple interpreter: runs a [[Flow]] as plain Scala over in-memory state.
  *
  * An environment is one *branch* of execution, mapping vars to values.
  * A step maps one environment to 0..n environments — `Each` gives
  * one, `Filter` gives zero or one, `Explode` gives n.
  */
object Interpreter {
	type Env = Map[VarId, Any]
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
	}
	class Depot() {
		val inner = SignallingRef.of[IO, Vector[Record]](Vector.empty).unsafeRunSync()(using unsafe.IORuntime.global)
		def stream: Stream[IO, Record] = inner.discrete.mapAccumulate(0) { (seen, values) =>
			val additions = values.drop(seen)
			(seen + additions.length, additions)
		}.mapChunks { ch => ch.flatMap(tup => Chunk.from(tup._2)) }
	}
}

/** An appended value, plus a slot for acks to accumulate.
  *
  * `tracker` is `None` for `emit`, where nobody is waiting.
  */
private[flow] final case class Record(value: Any, tracker: Option[AckTracker])

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
private[flow] final class AckTracker(
	pending: Ref[IO, Int],
	returns: Ref[IO, Map[TopologyId, List[Any]]],
	done: Deferred[IO, Either[Throwable, Map[TopologyId, List[Any]]]]) {

	/** Add a value to the return set for a specifi topology
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

/** An [[AckTracker]] with the identity of the topology reporting into it.
  *
  * `continue` executes nodes without knowing which topology it is running, and the node
  * for `ackReturn` carries only a var. Pairing the two here keeps that so, and is what
  * lets the sink cross a partition hop unchanged.
  */
private[flow] final case class AckSink(topology: TopologyId, tracker: AckTracker) {
	def ackReturn(value: Any): IO[Unit] = tracker.ackReturn(topology, value)
	def fail(err: Throwable): IO[Unit] = tracker.fail(err)
	def finish: IO[Unit] = tracker.finish
}

private[flow] object AckTracker {

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

	def put[K, V](pstate: flow.PState[K, V], key: K, value: V): IO[Unit] =
		this.partitions.partitionKey(key).put(pstate, key, value)

	def delete[K, V](pstate: flow.PState[K, V], key: K): IO[Unit] =
		this.partitions.partitionKey(key).delete(pstate, key)

	def get[K, V](pstate: flow.PState[K, V], key: K): IO[Option[V]] =
		this.partitions.partitionKey(key).get(pstate, key)

	def getKeys[K, V](pstate: flow.PState[K, V]): IO[Set[K]] =
		this.partitions.all.flatTraverse { partition =>
			partition.getKeys(pstate).map(_.toList)
		}.map(_.toSet)

	def emit[A](depot: flow.Depot[A], value: A): IO[Unit] =
		// TODO use the depot's partitioning strategy
		this.partitions.partitionKey(value).emit(depot, value)

	/** Topologies driven by this depot — the count an [[AckTracker]] waits for. */
	private[flow] def subscribers(depot: Identity): List[Topology[?, ?]] =
		topologies.topologies.filter(_.builder.source == depot)

	def append[A, R](depot: flow.Depot[A], value: A, level: AckLevel[R] = AckLevel.Ack): IO[R] =
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

	def readDepot[A](depot: flow.Depot[A]): IO[Vector[A]] =
		this.partitions.all.toVector.flatTraverse { partition =>
			partition.readDepot(depot)
		}

	/** Invoke a query topology, as a client would.
	  */
	def invoke[S, R](q: QueryTopology[S, R], value: S): IO[R] = {
		val topology = q.topology
		val env: Env = Map(topology.builder.input.vid -> value)
		partitions.random.continue(topology.compiled._1, List(env)).map { resultEnvs =>
			resultEnvs.foldLeft(topology.agg.zero) { (acc, resultEnv) =>
				topology.agg.combineAny(acc, resultEnv.getOrElse(topology.compiled._2.vid, throw new RuntimeException("output var not present in query result")))
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
				val ack = record.tracker.map(AckSink(top.builder.id, _))

				// `continue` returning means the whole event tree for this record is
				// exhausted — every fanned-out branch, on every partition it reached. So
				// this is exactly where a colocated topology reports completion.
				// `IO.defer`, because `continue` evaluates the pure parts of a flow — an
				// `each`'s function, say — while *building* its `IO`. A user function that
				// throws therefore throws here rather than inside the returned `IO`, and
				// without this the raise escapes to the consuming fiber instead of reaching
				// the tracker below.
				IO.defer(continue(top.compiled._1, List(env), ack)).void.attempt.flatMap {
					case Right(_) => record.tracker.traverse_(_.finish)

					// One bad record must not stop the topology: Rama retries the record and
					// keeps consuming. There is no retry here, so the failure is reported to
					// whoever appended it and consumption continues.
					case Left(e) =>
						record.tracker match {
							case Some(tracker) => tracker.fail(e)
							// Nobody is waiting, so swallowing would lose the error entirely.
							// Failing `run` is loud, which beats a silent no-op.
							case None => IO.raiseError(e)
						}
				}
			}.compile.drain *> IO.raiseError(new RuntimeException("depot consume terminated"))
		} *> IO.never
	}
	
	def put[K, V](pstate: flow.PState[K, V], key: K, value: V): IO[Unit] =
		this.pstate(pstate.id.name).inner.update(_.updated(key, value))

	def delete[K, V](pstate: flow.PState[K, V], key: K): IO[Unit] =
		this.pstate(pstate.id.name).inner.update(_.removed(key))

	def get[K, V](pstate: flow.PState[K, V], key: K): IO[Option[V]] = {
		this.pstate(pstate.id.name).inner.get.map(_.get(key).map(_.asInstanceOf[V]))
	}

	def getKeys[K, V](pstate: flow.PState[K, V]): IO[Set[K]] = {
		this.pstate(pstate.id.name).inner.get.map(_.keySet.map(_.asInstanceOf[K]))
	}

	def emit[A](depot: flow.Depot[A], value: A): IO[Unit] = {
		this.depot(depot.id.name).inner.update(vec => vec.appended(Record(value, None)))
	}

	/** As `emit`, but the record carries a tracker for the topologies it will trigger. */
	def append[A](depot: flow.Depot[A], value: A, tracker: AckTracker): IO[Unit] = {
		this.depot(depot.id.name).inner.update(vec => vec.appended(Record(value, Some(tracker))))
	}

	def readDepot[A](depot: flow.Depot[A]): IO[Vector[A]] = {
		this.depot(depot.id.name).inner.get.map(vec => vec.map(_.value.asInstanceOf[A]))
	}

	private def pstate(name: String) = pstates.getOrElse(name, throw new RuntimeException(s"Unknown pstate: ${name}"))
	private def depot(name: String) = depots.getOrElse(name, throw new RuntimeException(s"Unknown depot: ${name}"))

	/** Runs a list of nodes with the given envs. Returns the resulting list of envs.
	  *
	  * `ack` is where `Node.AckReturn` reports, and is `None` for a query or for a record
	  * appended with `emit` — nobody is waiting on either. It is passed along untouched
	  * everywhere else, including across a partition hop, since Rama routes an ack return
	  * to the *appender* rather than to the task that produced it.
	  */
	private [flow] def continue(nodes: List[Node], envs: List[Env], ack: Option[AckSink] = None): IO[List[Env]] =
		nodes match {
			case Nil => IO.pure(envs)

			case Node.MapN(pos, fn, ins, out) :: tail => {
				val results = envs.map(env => env.updated(out, fn(ins.map(env(_)))))
				continue(tail, results, ack)
			}

			case Node.Explode(_pos, fn, in, out) :: tail =>
				val exploded = envs.flatMap { env =>
					// in each env, `in` holds an `A`
					// `fn` takes A and yields a Seq of values.
					// we expand each of these into `out` in a
					// copy of the parent env
					fn(env(in)).toList.map { value =>
						env.updated(out, value)
					}
				}
				continue(tail, exploded, ack)

			case Node.Assign(in, out) :: tail =>
				continue(tail, envs.map { env =>
					env.updated(out, env(in))
				})

			case Node.Filter(_pos, fn, in, _) :: tail =>
				// The flag var exists for Rama's `keepTrue`; here the predicate prunes
				// directly, so nothing needs binding.
				continue(tail, envs.filter(env => fn(env(in))), ack)

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

			case Node.AggList(block, collectFrom, out) :: tail => {
				envs.traverse { env =>
					// for each env, we execute the sub-block and add its result to `out` in the
					// single resulting env
					continue(block, List(env), ack).map { innerEnvs =>
						env.updated(out, innerEnvs.map(block => block(collectFrom)))
					}
				}.flatMap { outputEnvs =>
					continue(tail, outputEnvs, ack)
				}
			}

			case Node.Agg(block, agg, collectFrom, out) :: tail => {
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

			case Node.Select(ps, key, out) :: tail =>
				val state = pstate(ps.name)
				envs.traverse { env =>
					val k = env(key)
					state.inner.get.map(_.get(k)).map { value =>
						env.updated(out, value)
					}
				}.flatMap { updatedEnvs =>
					continue(tail, updatedEnvs, ack)
				}

			case Node.Update(_pos, ps, key, fn) :: tail =>
				val state = pstate(ps.name)
				envs.traverse_ { env =>
					val k = env(key)
					state.inner.update { map =>
						map.updatedWith(k)(fn)
					}
				} *> continue(tail, envs, ack)

			case Node.Put(ps, key, value) :: tail =>
				val state = pstate(ps.name)
				envs.traverse_ { env =>
					val k = env(key)
					state.inner.update { map =>
						map.updated(k, env(value))
					}
				} *> continue(tail, envs, ack)

			case Node.UpdateWith(_pos, ps, key, arg, fn) :: tail =>
				val state = pstate(ps.name)
				envs.traverse_ { env =>
					val k = env(key)
					val a = env(arg)
					state.inner.update { map =>
						map.updatedWith(k)(v => fn(v, a))
					}
				} *> continue(tail, envs, ack)

			case Node.PartitionAppend(dep, value) :: tail =>
				val state = depot(dep.name)
				envs.traverse_ { env =>
					val v = env(value)
					// `None`: the new record starts its own event tree rather than extending
					// the appender's. An assumption, not a verified fact — FLOW-NOTES.md.
					state.inner.update(_.appended(Record(v, None)))
				} *> continue(tail, envs, ack)

			case Node.AckReturn(value) :: tail =>
				// Once per branch. A flow that fans out reaches this many times, and it is
				// the declared `AckAgg` that decides what the appender sees.
				envs.traverse_ { env =>
					ack.traverse_(_.ackReturn(env(value)))
				} *> continue(tail, envs, ack)
		}
}
