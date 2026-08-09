package flow

import com.rpl.rama.{Agg, Block, Path}
import com.rpl.rama.ops.{RamaFunction1, RamaFunction2, RamaOperation1, OutputCollector}

/** Compiles the [[Node]] AST to a `com.rpl.rama.Block`.
  */
object RamaBackend {

	private def fn1[A, B](f: A => B): RamaFunction1[A, B] = new RamaFunction1[A, B] {
		def invoke(a: A): B = f(a)
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
	private def explodeStep(fn: List[Any] => Seq[Any], in: String, out: String): Block.Impl = {
		val op = new RamaOperation1[Any] {
			def invoke(x: Any, collector: OutputCollector): Unit =
				fn(List(x)).foreach(v => collector.emit(v))
		}
		Block.each(op, in).out(out)
	}

	private def compileNode(node: Node): Block.Impl = node match {
		case Node.Each(fn, ins, out)    => eachStep(fn, ins.map(compileVar), compileVar(out))
		case Node.Explode(fn, in, out) => explodeStep(fn, compileVar(in), compileVar(out))

		case Node.Filter(fn, in, flag) =>
			val flagName = compileVar(flag)
			eachStep(arg => fn(arg).asInstanceOf[Any], List(compileVar(in)), flagName)
				.`macro`(Block.keepTrue(flagName))

		case Node.Partition(in) => Block.hashPartition(compileVar(in))

		case Node.Scope(inner, collectFrom, out) =>
			// Rama's `agg` collapses an implicit region; here the region is exactly the
			// nested block, which is what makes `aggList`'s type change meaningful.
			Block.create()
				.`macro`(compileNodes(inner))
				.`macro`(Block.agg(Agg.list(compileVar(collectFrom))).out(compileVar(out)))

		case Node.Select(ps, key, out) =>
			Block.localSelect(compileIdentity(ps), Path.key(compileVar(key))).out(compileVar(out))

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

	// /** Compile a topology to a Rama block.
	//   */
	// def compileToBlock(t: Topology[?, ?]): Block.Impl =
	// 	compileNodes(t.compiled.map(_._1))

	/** What a declared topology compiles to, plus the names Rama needs positionally.
	  *
	  * `in` is where the input value must be bound: for a stream that's
	  * `topologies.stream(name).source(depot).out(in)`, for a query it is the `"*arg"`
	  * of `topologies.query(name, in)`. `out` is present only for a query — Rama's
	  * `.out("*result")` — because a stream has no result to name.
	  */
	final case class Compiled(source: Identity, in: Var[?], out: Option[Var[?]], block: Block.Impl)

	/** Compile a declared topology, reporting the names its input and result are bound to.
	  */
	def compile[S, R](t: Topology[S, R]): Compiled = {
		val (nodes, result) = t.compiled
		val out = t.builder.source.namespace match {
			case IdentityNamespace.Query => Some(result)
			case _ => None
		}
		Compiled(t.builder.source, t.input, out, compileNodes(nodes))
	}
}
