package flow

import com.rpl.rama.{Agg, Block, Path}
import com.rpl.rama.ops.{RamaFunction1, RamaFunction2, RamaOperation1, OutputCollector}

/** Compiles the [[Node]] AST to a `com.rpl.rama.Block`.
  * Both namespaces are mapped here, at the boundary: PState names get Rama's `$$`
  * prefix, and [[VarId]]s are rendered to `*hint_id` by [[varName]]. Nothing upstream
  * of this file knows Rama's naming convention.
  */
object RamaBackend {

	private def fn1[A, B](f: A => B): RamaFunction1[A, B] = new RamaFunction1[A, B] {
		def invoke(a: A): B = f(a)
	}

	private def pstateVar(name: String): String = "$$" + name

	/** Renders a [[VarId]] to its Rama var name.
	  *
	  * A direct function, not a stateful allocator: ids come from the topology's
	  * counter and are already unique and stable, so the name needs no bookkeeping.
	  * The hint rides along because var names are what Rama prints in compile errors
	  * and the cluster UI.
	  */
	private[flow] def varName(vid: VarId): String = {
		require(vid != Var.unit.vid, "Var.unit has no Rama representation: it stands for the absence of a value")
		// TODO assert hint doesn't have any special characters
		s"*${vid.hint}_${vid.id}"
	}

	/** The Rama name a var is bound to — what the caller passes to
	  * `topology.source(depot).out(...)` so the block's reads line up.
	  */
	def varName[A](v: Var[A]): String = varName(v.vid)

	private def eachStep(fn: List[Any] => Any, ins: List[String], out: String): Block.Impl =
		ins match {
			case List(a) =>
				Block.each(fn1[Any, Any](x => fn(List(x))), a).out(out)
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
	private def explodeStep(fn: List[Any] => Seq[Any], ins: List[String], out: String): Block.Impl =
		ins match {
			case List(a) =>
				val op = new RamaOperation1[Any] {
					def invoke(x: Any, collector: OutputCollector): Unit =
						fn(List(x)).foreach(v => collector.emit(v))
				}
				Block.each(op, a).out(out)
			case other =>
				throw new UnsupportedOperationException(s"explodeStep: arity ${other.length}")
		}

	private def compileNode(node: Node): Block.Impl = node match {
		case Node.Each(fn, ins, out)    => eachStep(fn, ins.map(varName), varName(out))
		case Node.Explode(fn, ins, out) => explodeStep(fn, ins.map(varName), varName(out))

		case Node.Filter(fn, ins, flag) =>
			// The flag var is minted by `Lower`, so the two filters over one input get
			// distinct names without the backend having to allocate anything.
			val flagName = varName(flag)
			eachStep(args => fn(args).asInstanceOf[Any], ins.map(varName), flagName)
				.`macro`(Block.keepTrue(flagName))

		case Node.Partition(in) => Block.hashPartition(varName(in))

		case Node.Scope(inner, collectFrom, out) =>
			// Rama's `agg` collapses an implicit region; here the region is exactly the
			// nested block, which is what makes `aggList`'s type change meaningful.
			Block.create()
				.`macro`(compile(inner))
				.`macro`(Block.agg(Agg.list(varName(collectFrom))).out(varName(out)))

		case Node.Select(ps, key, out) =>
			Block.localSelect(pstateVar(ps), Path.key(varName(key))).out(varName(out))

		case Node.Update(ps, key, fn) =>
			// Rama models read-modify-write as a `term` on the path: the function
			// receives the current value (null when absent) and returns the new one.
			val term = new RamaFunction1[Any, Any] {
				def invoke(current: Any): Any = fn(Option(current)).orNull
			}
			Block.localTransform(pstateVar(ps), Path.key(varName(key)).term(term))

		case Node.Put(ps, key, value) =>
			Block.localTransform(pstateVar(ps), Path.key(varName(key)).termVal(varName(value)))

		case Node.UpdateWith(ps, key, arg, fn) =>
			// Rama's 2-arity `term`: the extra var is passed as a path argument, which
			// is how a runtime value reaches a read-modify-write.
			val term = new RamaFunction2[Any, Any, Any] {
				def invoke(current: Any, a: Any): Any = fn(Option(current), a).orNull
			}
			Block.localTransform(pstateVar(ps), Path.key(varName(key)).term(term, varName(arg)))
	}

	/** Compile a lowered flow to a Rama block.
	  *
	  * No source name to pass: every var already has a canonical name, so the caller
	  * wires the source up with `topology.source(depot).out(RamaBackend.varName(v))`.
	  */
	def compile(nodes: List[Node]): Block.Impl =
		nodes.foldLeft(Block.create())((acc, n) => acc.`macro`(compileNode(n)))

	/** Lower and compile a flow in one step, against the builder that owns its vars. */
	def compileFlow[A](t: TopologyBuilder[?], flow: Flow[A]): Block.Impl =
		compile(Lower(t, flow)._1)

	/** What a declared topology compiles to, plus the names Rama needs positionally.
	  *
	  * `in` is where the input value must be bound: for a stream that's
	  * `topologies.stream(name).source(depot).out(in)`, for a query it is the `"*arg"`
	  * of `topologies.query(name, in)`. `out` is present only for a query — Rama's
	  * `.out("*result")` — because a stream has no result to name.
	  */
	final case class Compiled(kind: TopologyKind, name: String, in: String, out: Option[String], block: Block.Impl)

	/** Compile a declared topology, reporting the names its input and result are bound
	  * to. The caller wires those into Rama's declaration, so the block's reads and
	  * writes line up by construction rather than by matching string literals by hand.
	  */
	def compileTopology[S, R](t: Topology[S, R]): Compiled = {
		val (nodes, result) = Lower(t)
		val out = t.kind match {
			case TopologyKind.Query  => Some(varName(result))
			case TopologyKind.Stream => None
		}
		Compiled(t.kind, t.name, varName(t.input), out, compile(nodes))
	}

	/** Every Rama var name a lowered tree binds or reads, in traversal order.
	  *
	  * `Block` has no public inspection API, so this is how naming is asserted on.
	  */
	private[flow] def namesOf(nodes: List[Node]): List[String] = nodes.flatMap {
		case Node.Each(_, ins, out)       => ins.map(varName) :+ varName(out)
		case Node.Explode(_, ins, out)    => ins.map(varName) :+ varName(out)
		case Node.Filter(_, ins, flag)    => ins.map(varName) :+ varName(flag)
		case Node.Partition(in)           => List(varName(in))
		case Node.Scope(inner, from, out) => namesOf(inner) ++ List(varName(from), varName(out))
		case Node.Select(_, k, out)       => List(varName(k), varName(out))
		case Node.Update(_, k, _)         => List(varName(k))
		case Node.Put(_, k, v)            => List(varName(k), varName(v))
		case Node.UpdateWith(_, k, a, _)  => List(varName(k), varName(a))
	}
}
