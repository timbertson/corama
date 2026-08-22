package flow.internal

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid

import flow._

// ---------------------------------------------------------------------------
// base AST
// ---------------------------------------------------------------------------

private[flow] sealed trait Node
private[flow] object Node {
	case class MapN(pos: SourcePos, fn: List[Any] => Any, ins: List[VarId], out: VarId) extends Node

	case class Explode(pos: SourcePos, fn: Any => Seq[Any], in: VarId, out: VarId) extends Node

	case class Assign(in: VarId, out: VarId) extends Node

	/** `flag` is where the predicate's result is bound before Rama's `keepTrue` reads
	  * it. Unused by `interp` backend.
		*/
	case class Filter(pos: SourcePos, fn: Any => Boolean, in: VarId, flag: VarId) extends Node

	case class Partition(in: VarId) extends Node

	case class If(cond: VarId, ifBlock: List[Node], elseBlock: List[Node], out: VarId) extends Node

	// TODO: this is a special case of Agg, is it necessary?
	case class AggList(block: List[Node], collectFrom: VarId, out: VarId) extends Node

	case class Agg(block: List[Node], agg: flow.Agg[?], collectFrom: VarId, out: VarId) extends Node

	case class Select(pstate: Identity, key: VarId, out: VarId) extends Node
	case class Update(pos: SourcePos, pstate: Identity, key: VarId, fn: Option[Any] => Option[Any]) extends Node
	case class Put(pstate: Identity, key: VarId, value: VarId) extends Node
	case class UpdateWith(pos: SourcePos, pstate: Identity, key: VarId, arg: VarId, fn: (Option[Any], Any) => Option[Any]) extends Node
	case class PartitionAppend(depot: Identity, arg: VarId) extends Node
	case class AckReturn(value: VarId) extends Node

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
				val out = t.define[A](e.pos, "map")
				val f = e.f
				(List(Node.MapN(e.pos, f, e.ins.map(_.vid), out.vid)), out)

			case e: Flow.ExplodeOp[i, A] =>
				val out = t.define[A](e.pos, "exploded")
				val f = e.f
				val fn = (arg: Any) => f(arg.asInstanceOf[i]).asInstanceOf[Seq[Any]]
				(List(Node.Explode(e.pos, fn, e.in.vid, out.vid)), out)

			case e: Flow.FilterOp[i] =>
				val p = e.p
				val fn = (arg: Any) => p(arg.asInstanceOf[i])
				val flag = t.define[Boolean](e.pos, s"${e.in.vid.hint}_keep")
				(List(Node.Filter(e.pos, fn, e.in.vid, flag.vid)), Var.unit.asInstanceOf[Var[A]])

			case p: Flow.Partition[i] =>
				(List(Node.Partition(p.in.vid)), Var.unit.asInstanceOf[Var[A]])

			case b: Flow.Bind[a, A, c1, c2] =>
				val (n1, v1) = compile(t, b.fa)
				val (n2, v2) = compile(t, b.k(v1))
				(n1 ++ n2, v2)

			case i: Flow.If[a, ccond, c1, c2] =>
				val out = t.define[a](Var.internal, "ifOut")
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

			case a: Flow.AggList[x, c] =>
				val (block, innerVar) = compile(t, a.block)
				val out = t.define[A](Var.internal, "aggList")
				(List(Node.AggList(block, innerVar.vid, out.vid)), out)

			case a: Flow.Agg[a, b, c] =>
				val (block, innerVar) = compile(t, a.block)
				val out = t.define[A](Var.internal, "agg")
				(List(Node.Agg(block, a.agg, innerVar.vid, out.vid)), out)

			case s: Flow.Select[k, v] =>
				val out = t.define[A](Var.internal, "select")
				(List(Node.Select(s.ps.id, s.key.vid, out.vid)), out)

			case u: Flow.Update[k, v] =>
				val f = u.f
				val fn = (cur: Option[Any]) => f(cur.asInstanceOf[Option[v]]).asInstanceOf[Option[Any]]
				(List(Node.Update(u.pos, u.ps.id, u.key.vid, fn)), Var.unit.asInstanceOf[Var[A]])

			case Flow.Put(ps, key, value) =>
				(List(Node.Put(ps.id, key.vid, value.vid)), Var.unit.asInstanceOf[Var[A]])

			case up: Flow.UpdateWith[k, v, a] =>
				up match {
					case Flow.UpdateWith(pos, ps, key, arg, fn0) =>
						val fn = fn0.asInstanceOf[(Option[Any], Any) => Option[Any]]
						(List(Node.UpdateWith(pos, ps.id, key.vid, arg.vid, fn)), Var.unit.asInstanceOf[Var[A]])
				}

			case Flow.PartitionAppend(depot, value) =>
				(List(Node.PartitionAppend(depot.id, value.vid)), Var.unit.asInstanceOf[Var[A]])

			case Flow.AckReturn(value) =>
				(List(Node.AckReturn(value.vid)), Var.unit.asInstanceOf[Var[A]])
		}
	}
}
