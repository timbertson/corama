package flow.internal

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid
import izumi.reflect.Tag

import flow._

// ---------------------------------------------------------------------------
// base AST
// ---------------------------------------------------------------------------

private[flow] sealed trait Node
private[flow] object Node {
	// operations backed by a user-supplied function
	sealed trait DynamicOp
	object DynamicOp {
		case class Map(out: VarId) extends DynamicOp
		case class Explode(out: VarId) extends DynamicOp
		case class Filter(condVar: VarId) extends DynamicOp
	}

	// Dynamic node represents anything with a user-supplied function.
	// The `op` contains the output var, if present
	case class Dynamic(pos: SourcePos, ins: List[VarId], fn: List[Any] => Any, op: DynamicOp) extends Node

	case class Assign(in: VarId, out: VarId) extends Node

	/** `flag` is where the predicate's result is bound before Rama's `keepTrue` reads
	  * it. Unused by `interp` backend.
		*/
	// case class Filter(pos: SourcePos, fn: Any => Boolean, in: VarId, flag: VarId) extends Node

	case class Partition(in: VarId) extends Node

	case class If(cond: VarId, ifBlock: List[Node], elseBlock: List[Node], out: VarId) extends Node

	case class Combine(block: List[Node], agg: flow.Agg[?], collectFrom: VarId, out: VarId) extends Node

	case class LocalAppend(depot: Identity, arg: VarId) extends Node

	sealed trait PStateOp
	object PStateOp {
		case class Add(v: VarId) extends PStateOp
		case class Remove(v: VarId) extends PStateOp
		
		// intermediate for Select/Get are only used in rama backend
		case class Select(intermediate: VarId, out: VarId, options: SelectOptions[?]) extends PStateOp
		case class Get(intermediate: VarId, out: VarId) extends PStateOp

		case class Put(v: VarId) extends PStateOp
		// case class Delete(v: VarId) extends PStateOp
	}

	case class PStateNode(pstate: Identity, path: CompiledPath, op: PStateOp) extends Node
	case class PStateUpdate(
		pos: SourcePos, pstate: Identity, path: CompiledPath, ins: List[VarId],
		fn: (Option[Any], List[Any]) => Option[Any]
	) extends Node

	/** Compiles the higher-order [[Flow]] to a [[Node]] list.
		*
		* TODO: remove unnecessary var return?
		*
		* NOTE: for the rama backend, _always_ extract `f` to a value first. Otherwise
		*       the closure captures Var[T], which isn't serializable.
		*/
	def compile[A](t: TopologyBuilder[?], flow: Flow[A, ?]): (List[Node], Var[A]) = {
		flow match {
			case Flow.Pure(v) => (Nil, v)

			case e: Flow.MapN[A] =>
				val out = t.define[A](e.tag, e.pos, "map")
				val f = e.f
				(List(Node.Dynamic(e.pos, e.ins.map(_.vid), f, DynamicOp.Map(out.vid))), out)

			case e: Flow.ExplodeOp[A] =>
				val out = t.define[A](e.tag, e.pos, "exploded")
				val f = e.f
				(List(Node.Dynamic(e.pos, e.ins.map(_.vid), f, DynamicOp.Explode(out.vid))), out)

			case e: Flow.FilterOp[i] =>
				val f = e.f
				val condVar = t.define[Boolean](Tag[Boolean], Var.internal, "cond")
				(List(Node.Dynamic(e.pos, e.ins.map(_.vid), f, DynamicOp.Filter(condVar.vid))), Var.unit.asInstanceOf[Var[A]])

			case p: Flow.Partition[i] =>
				(List(Node.Partition(p.in.vid)), Var.unit.asInstanceOf[Var[A]])

			case b: Flow.Bind[a, A, c1, c2] =>
				val (n1, v1) = compile(t, b.fa)
				val (n2, v2) = compile(t, b.k(v1))
				(n1 ++ n2, v2)

			case i: Flow.If[a, ccond, c1, c2] =>
				val out = t.define[a](i.tag, Var.internal, "ifOut")
				val (condBlock, vCond) = compile(t, i.cond)
				val (ifBlock, vIf) = compile(t, i.ifBlock)
				val (elseBlock, vElse) = compile(t, i.elseBlock)
				val bodyNodes = List(Node.If(
					vCond.vid,
					ifBlock.appended(Node.Assign(vIf.vid, out.vid)),
					elseBlock.appended(Node.Assign(vElse.vid, out.vid)),
					out.vid
				))

				(condBlock ++ bodyNodes, out)

			case a: Flow.Combine[a, ctx] =>
				val (block, innerVar) = compile(t, a.block)
				val out = t.define[A](a.tag, Var.internal, "agg")
				(List(Node.Combine(block, a.agg, innerVar.vid, out.vid)), out)

			case g: Flow.Get[a] =>
				val intermediate = t.define[Any](implicitly[Tag[Any]], Var.internal, "getIntermediate")
				val out = t.define[a](g.lens.tag, Var.internal, "get")
				(List(Node.PStateNode(g.lens.pstate.id, CompiledPath.from(g.lens.path), PStateOp.Get(intermediate.vid, out.vid))), out.asInstanceOf[Var[A]])

			case Flow.Put(lens, value) =>
				(List(Node.PStateNode(lens.pstate.id, CompiledPath.from(lens.path), PStateOp.Put(value.vid))), Var.unit.asInstanceOf[Var[A]])

			case Flow.Add(lens, value) =>
				(List(Node.PStateNode(lens.pstate.id, CompiledPath.from(lens.path), PStateOp.Add(value.vid))), Var.unit.asInstanceOf[Var[A]])

			case Flow.Remove(lens, value) =>
				(List(Node.PStateNode(lens.pstate.id, CompiledPath.from(lens.path), PStateOp.Remove(value.vid))), Var.unit.asInstanceOf[Var[A]])

			case u: Flow.Update[v] =>
				val f = u.f
				(List(Node.PStateUpdate(u.pos, u.lens.pstate.id, CompiledPath.from(u.lens.path), u.ins.map(_.vid), f)), Var.unit.asInstanceOf[Var[A]])

			case s: Flow.Select[v] =>
				val intermediate = t.define[Any](implicitly[Tag[Any]], Var.internal, "select")
				val out = t.define[Set[v]](s.lens.schema.setTag, Var.internal, "select")
				(List(Node.PStateNode(s.lens.pstate.id, CompiledPath.from(s.lens.path), PStateOp.Select(intermediate.vid, out.vid, s.options))), out.asInstanceOf[Var[A]])

			case Flow.LocalAppend(depot, value) =>
				(List(Node.LocalAppend(depot.id, value.vid)), Var.unit.asInstanceOf[Var[A]])
		}
	}
}
