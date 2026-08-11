package flow

import fs2.Stream
import cats.syntax.all._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe
import flow.Interpreter.Env
import scala.util.Random
import fs2.concurrent.SignallingRef
import fs2.Chunk

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
		val inner = SignallingRef.of[IO, Vector[Any]](Vector.empty).unsafeRunSync()(using unsafe.IORuntime.global)
		def stream: Stream[IO, Any] = inner.discrete.mapAccumulate(0) { (seen, values) =>
			val additions = values.drop(seen)
			(seen + additions.length, additions)
		}.mapChunks { ch => ch.flatMap(tup => Chunk.from(tup._2)) }
	}
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

	def append[A, R](depot: flow.Depot[A], value: A, level: AckLevel[R] = AckLevel.Ack): IO[R] =
		level match {
			// TODO: these branches should differ when we model replication
			case AckLevel.NoAck     => emit(depot, value)
			case AckLevel.AppendAck => emit(depot, value)

			// Needs the append to wait for every colocated stream topology's event tree
			// to be exhausted, and needs `ackReturn` to exist as a Flow op to collect
			// from. Failing is the honest answer: returning `AckReturns.empty` would be
			// indistinguishable from "every topology returned nothing" while in fact
			// having waited for nothing at all.
			case AckLevel.Ack =>
				IO.raiseError(new NotImplementedError(
					"AckLevel.Ack needs completion tracking and a Flow-level ackReturn; " +
					"until then use AppendAck and poll, as the tests do"))
		}

	def readDepot[A](depot: flow.Depot[A]): IO[Vector[A]] =
		this.partitions.all.toVector.flatTraverse { partition =>
			partition.readDepot(depot)
		}

	/** Invoke a query topology, as a client would.
	  */
	def invoke[S, R](q: Topology[S, R], value: S): IO[R] = {
		// TODO: wrap a QueryTopology(topology) instead of asserting
		val source = q.builder.source
		require(source.namespace == IdentityNamespace.Query, s"${source} is not a query")
		
		val env: Env = Map(q.builder.input.vid -> value)
		partitions.random.continue(q.compiled._1, List(env)).map {
			case List(resultEnv: Env) => {
				resultEnv.getOrElse(q.compiled._2.vid, throw new RuntimeException("output var not present in query result")).asInstanceOf[R]
			}
			case other => throw new IllegalStateException(s"query '${q.builder.source}' emitted ${other.length} values, expected exactly 1: $other")
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
			this.depots(top.builder.source.name).stream.foreach { input =>
				// TODO: chunks?
				val env = Map(top.input.vid -> input)
				continue(top.compiled._1, List(env)).void
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
		this.depot(depot.id.name).inner.update(vec => vec.appended(value))
	}

	def readDepot[A](depot: flow.Depot[A]): IO[Vector[A]] = {
		this.depot(depot.id.name).inner.get.map(vec => vec.map(_.asInstanceOf[A]))
	}

	private def pstate(name: String) = pstates.getOrElse(name, throw new RuntimeException(s"Unknown pstate: ${name}"))
	private def depot(name: String) = depots.getOrElse(name, throw new RuntimeException(s"Unknown depot: ${name}"))

	// runs a list of nodes with the given envs. Returns the resulting list of envs
	private [flow] def continue(nodes: List[Node], envs: List[Env]): IO[List[Env]] =
		nodes match {
			case Nil => IO.pure(envs)

			case Node.Each(fn, ins, out) :: tail =>
				val results = envs.map(env => env.updated(out, fn(ins.map(env(_)))))
				continue(tail, results)

			case Node.Explode(fn, in, out) :: tail =>
				val exploded = envs.flatMap { env =>
					// in each env, `in` holds an `A`
					// `fn` takes A and yields a Seq of values.
					// we expand each of these into `out` in a
					// copy of the parent env
					fn(env(in)).toList.map { value =>
						env.updated(out, value)
					}
				}
				continue(tail, exploded)
				

			case Node.Filter(fn, in, _) :: tail =>
				// The flag var exists for Rama's `keepTrue`; here the predicate prunes
				// directly, so nothing needs binding.
				continue(tail, envs.filter(env => fn(env(in))))

			case Node.Partition(in) :: tail => {
				interpreter.partitions.groupByPartitions(envs, env => env(in)).toList.flatTraverse { case (partition, penvs) =>
					partition.continue(tail, penvs)
				}
			}

			case Node.Scope(inner, collectFrom, out) :: tail => {
				envs.traverse { env =>
					// for each env, we execute the sub-block and add its result to `out` in the
					// single resulting env
					continue(inner, List(env)).map { innerEnvs =>
						env.updated(out, innerEnvs.map(inner => inner(collectFrom)))
					}
				}.flatMap { outputEnvs =>
					continue(tail, outputEnvs)
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
					continue(tail, updatedEnvs)
				}

			case Node.Update(ps, key, fn) :: tail =>
				val state = pstate(ps.name)
				envs.traverse_ { env =>
					val k = env(key)
					state.inner.update { map =>
						map.updatedWith(k)(fn)
					}
				} *> continue(tail, envs)

			case Node.Put(ps, key, value) :: tail =>
				val state = pstate(ps.name)
				envs.traverse_ { env =>
					val k = env(key)
					state.inner.update { map =>
						map.updated(k, env(value))
					}
				} *> continue(tail, envs)

			case Node.UpdateWith(ps, key, arg, fn) :: tail =>
				val state = pstate(ps.name)
				envs.traverse_ { env =>
					val k = env(key)
					val a = env(arg)
					state.inner.update { map =>
						map.updatedWith(k)(v => fn(v, a))
					}
				} *> continue(tail, envs)

			case Node.PartitionAppend(dep, value) :: tail =>
				val state = depot(dep.name)
				envs.traverse_ { env =>
					val v = env(value)
					state.inner.update(_.appended(v))
				} *> continue(tail, envs)
		}
}
