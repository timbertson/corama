package flow

import cats.syntax.all._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try
import cats.effect.IO
import cats.effect.kernel.Ref
import flow.Interpreter.Env

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

	def groupByPartitions[A](values: List[A], keyFn: A => Any): List[(Partition, List[A])] =
		values.groupBy(v => rawPartitionKey(keyFn(v))).toList.map { case (p, items) =>
			(partitions(p) -> items)
		}

	// TODO make much better
	private def rawPartitionKey(value: Any) = value.hashCode() % numPartitions

}

object Storage {
	class PState() {
		val inner = Ref.unsafe[IO, Map[Any,Any]](Map.empty)
	}
	class Depot() {
		val inner = Ref.unsafe[IO, Vector[Any]](Vector.empty)
	}
}

class Interpreter(val topologies: Topologies, numPartitions: Int = 10) {
	import Interpreter._
	val partitions = new Partitions(this, numPartitions)
	// def partition(i: Int): Try[Partition] = partitions.get(i)
	// def hashPartition(value: Any): Partition = ???

	def run: IO[Nothing] = {
		partitions.all.parTraverse_(p => p.run) *> IO.raiseError(new RuntimeException("IO[Nothing] completed"))
	}
	
	def running[A](block: IO[A]): IO[A] = {
		IO.race(block, run).map(_.merge)
	}
}

// TODO this could possibly take a pair of (ids, partitioned) - it doesn't require the full interpreter
class Partition(interpreter: Interpreter) {
	import Storage._

	private val idList = interpreter.topologies.ids.toList

	private val pstates: Map[String, PState] = idList.mapFilter { id =>
		if (id.namespace == IdentityNamespace.PState) {
			Some(id.name -> new PState())
		} else {
			None
		}
	}.toMap
	
	private val depots: Map[String, Depot] = idList.mapFilter { id =>
		if (id.namespace == IdentityNamespace.Depot) {
			Some(id.name -> new Depot())
		} else {
			None
		}
	}.toMap
	
	val run: IO[Nothing] = {
		IO.never
	}

	private def pstate(name: String) = pstates.getOrElse(name, throw new RuntimeException(s"Unknown pstate: ${name}"))
	private def depot(name: String) = depots.getOrElse(name, throw new RuntimeException(s"Unknown depot: ${name}"))

	private def continue(nodes: List[Node], envs: List[Env]): IO[Unit] =
		nodes match {
			case Nil => IO.unit
			case Node.Each(fn, ins, out) :: tail =>
				val results = envs.map(env => env.updated(out, fn(ins.map(env(_)))))
				continue(tail, results)

			case Node.Explode(fn, ins, out) :: tail =>
				// TODO
				???

			case Node.Filter(fn, ins, _) :: tail =>
				// The flag var exists for Rama's `keepTrue`; here the predicate prunes
				// directly, so nothing needs binding.
				continue(tail, envs.filter(env => fn(ins.map(env(_)))))

			case Node.Partition(in) :: tail => {
				interpreter.partitions.groupByPartitions(envs, env => env(in)).traverse_ { case (partition, penvs) =>
					partition.continue(tail, penvs)
				}
			}

			case Node.Scope(inner, collectFrom, out) :: tail =>
				// TODO
				???

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
						map.updatedWith(k)(v => fn(v, v))
					}
				} *> continue(tail, envs)

			case Node.PartitionAppend(dep, value) :: tail =>
				val state = depot(dep.name)
				envs.traverse_ { env =>
					val v = env(value)
					state.inner.update(_.appended(v))
				} *> continue(tail, envs)

		}

	// /** Run one source value through an already-lowered flow.
	//   */
	// private def runLowered[S, A](
	// 	nodes: List[Node],
	// 	out: Var[A],
	// 	source: Var[S],
	// 	value: S,
	// 	store: Store): Trace = {
	// 	val hops = List.newBuilder[Any]
	// 	val start: Env = Map(source.vid -> value, Var.unit.vid -> (()))
	// 	val finalEnvs = run(nodes, List(start), store, hops)
	// 	Trace(finalEnvs.map(_(out.vid)), hops.result())
	// }

	// /** Feed one input value through a flow, returning what it emitted.
	//   */
	// def runOne[S, A](t: TopologyBuilder[S], value: S, flow: Flow[A], store: Store = new Store): Trace = {
	// 	val (nodes, out) = Lower(t, flow)
	// 	runLowered(nodes, out, t.input, value, store)
	// }

	// /** Feed a sequence of input values through, sharing one store — the useful
	//   * shape for testing stateful topologies. Lowers once, then runs per value.
	//   */
	// def runAll[S, A](t: TopologyBuilder[S], values: Seq[S], flow: Flow[A]): (List[Trace], Store) = {
	// 	val store = new Store
	// 	val (nodes, out) = Lower(t, flow)
	// 	(values.map(v => runLowered(nodes, out, t.input, v, store)).toList, store)
	// }

	// /** Run a declared topology over a sequence of inputs, sharing one store. */
	// def run[S, R](t: Topology[S, R], values: Seq[S], store: Store = new Store): List[Trace] = {
	// 	val (nodes, out) = Lower(t)
	// 	values.map(v => runLowered(nodes, out, t.input, v, store)).toList
	// }

	// /** Invoke a query topology, as a client would.
	//   */
	// def invoke[S, R](q: Topology[S, R], value: S): IO[R] = {
	// 	// TODO: wrap a QueryTopology(topology) instead of asserting
	// 	val source = q.builder.source
	// 	require(source.namespace == IdentityNamespace.Query, s"${source} is not a query")
	// 	???
	// 	val trace = run(q, List(value), store).head
	// 	trace.values match {
	// 		case List(one) => one.asInstanceOf[R]
	// 		case other =>
	// 			throw new IllegalStateException(
	// 				s"query '${q.name}' emitted ${other.length} values, expected exactly 1: $other")
	// 	}
	// }
}
