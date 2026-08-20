package flow

import cats.syntax.all._
import com.rpl.rama.{Agg, Block, Path}
import com.rpl.rama
import com.rpl.rama.module.StreamSourceOptions
import com.rpl.rama.ops.{RamaAccumulatorAgg1, RamaFunction1, RamaFunction2, RamaOperation1, OutputCollector}
import com.rpl.rama.RamaModule.Setup
import com.rpl.rama.RamaModule
import flow.IdentityNamespace.Query
import com.rpl.rama.module.StreamTopology
import com.rpl.rama.ops.RamaAccumulatorAgg
import com.rpl.rama.ops.RamaFunction0

class RamaBackend(setup: RamaModule.Setup, ramaTopologies: RamaModule.Topologies) {
	import RamaBackend._
	def install(topologies: flow.Topologies): Unit = {
		// TODO: would you ever need multiple stream topologies?
		val streamTopology = ramaTopologies.stream("main")

		topologies.ids.foreach { id =>
			id.namespace match {
				// TODO schema + partitioning config
				case flow.IdentityNamespace.PState => streamTopology.pstate(compileIdentity(id), classOf[Object])
				case flow.IdentityNamespace.Depot => setup.declareDepot(compileIdentity(id), rama.Depot.random())
				case _ => ()
			}
		}

		topologies.topologies.foreach { t =>
			val id = t.builder.source
			val compiled = compileNodes(t.compiled._1)
			val block = t.builder.source.namespace match {
				case IdentityNamespace.Depot => Some(streamTopology.source(compileIdentity(id)))
				case IdentityNamespace.Query => Some(ramaTopologies.query(compileIdentity(id)))
				case IdentityNamespace.PState => None
			}
			block.foreach(_.out(compileVar(t.input)).`macro`(compiled))
		}
	}
}

object RamaBackend {
	def apply(setup: RamaModule.Setup, topologies: RamaModule.Topologies) = new RamaBackend(setup, topologies)

	private def fn1[A, B](f: A => B): RamaFunction1[A, B] = new RamaFunction1[A, B] {
		def invoke(a: A): B = f(a)
	}

	private def fn0[A](f: () => A): RamaFunction0[A] = new RamaFunction0[A] {
		def invoke(): A = f()
	}

	// generate a rama varname for the given identity
	private def compileIdentity(id: Identity) = {
		id.namespace match {
			case IdentityNamespace.PState => "$$" + id.name
			case IdentityNamespace.Depot => "*" + id.name
			case IdentityNamespace.Query => id.name
		}
	}

	// generate a rama var name
	private[flow] def compileVar(vid: VarId): String = {
		require(vid != Var.unit.vid, "Var.unit has no Rama representation: it stands for the absence of a value")
		// TODO assert hint doesn't have any special characters
		s"*${vid.hint}_${vid.id}"
	}

	def compileVar[A](v: Var[A]): String = compileVar(v.vid)

	private def eachStep(fn: List[Any] => Any, ins: List[String], out: String): Block.Impl =
		ins match {
			case Nil => {
				Block.each(fn0[Any](() => fn(Nil))).out(out)
			}
			case List(a) => {
				Block.each(fn1[Any, Any](x => fn(List(x))), a).out(out)
			}
			case List(a, b) =>
				val f2 = new RamaFunction2[Any, Any, Any] {
					def invoke(x: Any, y: Any): Any = fn(List(x, y))
				}
				Block.each(f2, a, b).out(out)
			case other =>
				throw new UnsupportedOperationException(
					s"eachStep: arity ${other.length} not wired up yet (inputs: $other)")
		}

	/** Fan-out as a Rama operation: emit once per element of the computed sequence.
	  *
	  * Note this uses `RamaOperation1` + `OutputCollector` rather than `Ops.EXPLODE`
	  * over a returned collection. Both work; emitting directly keeps the Scala
	  * `Seq` from having to be converted to a `java.util.List` first, and it makes
	  * the "0..n emissions" shape explicit at the call site.
	  */
	private def explodeStep(fn: List[Any] => Seq[Any], in: String, out: String): Block.Impl = {
		val op = new RamaOperation1[Any] {
			def invoke(x: Any, collector: OutputCollector): Unit =
				fn(List(x)).foreach(v => collector.emit(v))
		}
		Block.each(op, in).out(out)
	}

	private def compileNode(node: Node): Block.Impl = node match {
		case Node.MapN(fn, ins, out)    => eachStep(fn, ins.map(compileVar), compileVar(out))
		case Node.Explode(fn, in, out) => explodeStep(fn, compileVar(in), compileVar(out))

		case Node.Filter(fn, in, flag) =>
			val flagName = compileVar(flag)
			eachStep(arg => fn(arg).asInstanceOf[Any], List(compileVar(in)), flagName)
				.`macro`(Block.keepTrue(flagName))

		case Node.If(cond, ifBlock, elseBlock, _) => {
			Block.ifTrue(
				compileVar(cond),
				Block.create().`macro`(compileNodes(ifBlock)),
				Block.create().`macro`(compileNodes(elseBlock)),
			)
		}

		case Node.Partition(in) => Block.hashPartition(compileVar(in))

		case Node.Assign(in, out) =>
			println(s"Assigning ${in} -> ${out}")
			if (in === Var.unit.vid) {
				Block.each(() => ()).out(compileVar(out))
			} else {
				Block.each(identity, compileVar(in)).out(compileVar(out))
			}

		case Node.AggList(block, collectFrom, out) =>
			Block.create()
				.`macro`(compileNodes(block))
				.`macro`(Block.agg(Agg.list(compileVar(collectFrom))).out(compileVar(out)))

		case Node.Agg(block, agg, collectFrom, out) =>
			Block.create()
				.`macro`(compileNodes(block))
				.`macro`(Block.agg(Agg.accumulator(compileAgg(agg), compileVar(collectFrom))).out(compileVar(out)))

		case Node.Select(ps, key, out) =>
			Block.localSelect(compileIdentity(ps), Path.key(compileVar(key)))
				.out(compileVar(out))
				.each(Option.apply, compileVar(out)) // localselect returns null when not found
				.out(compileVar(out))

		case Node.Update(ps, key, fn) =>
			// Rama models read-modify-write as a `term` on the path: the function
			// receives the current value (null when absent) and returns the new one.
			val term = new RamaFunction1[Any, Any] {
				def invoke(current: Any): Any = fn(Option(current)).orNull
			}
			Block.localTransform(compileIdentity(ps), Path.key(compileVar(key)).term(term))

		case Node.Put(ps, key, value) =>
			Block.localTransform(compileIdentity(ps), Path.key(compileVar(key)).termVal(compileVar(value)))

		case Node.PartitionAppend(dep, value) =>
			Block.depotPartitionAppend(compileIdentity(dep), compileVar(value))

		case Node.AckReturn(value) =>
			Block.ackReturn(compileVar(value))

		case Node.UpdateWith(ps, key, arg, fn) =>
			// Rama's 2-arity `term`: the extra var is passed as a path argument, which
			// is how a runtime value reaches a read-modify-write.
			val term = new RamaFunction2[Any, Any, Any] {
				def invoke(current: Any, a: Any): Any = fn(Option(current), a).orNull
			}
			Block.localTransform(compileIdentity(ps), Path.key(compileVar(key)).term(term, compileVar(arg)))
	}

	private def compileNodes(nodes: List[Node]): Block.Impl =
		nodes.foldLeft(Block.create())((acc, n) => acc.`macro`(compileNode(n)))

	private def compileAgg[O](agg: flow.Agg[O]): RamaAccumulatorAgg1[O, O] = {
		new RamaAccumulatorAgg1[O, O] {
			override def initVal(): O = agg.zero
			override def accumulate(current: O, value: O): O = agg.combine(current, value)
		}
	}
}
