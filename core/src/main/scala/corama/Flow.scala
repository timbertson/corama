package corama

import izumi.reflect.Tag
import org.tpolecat.sourcepos.*

/** A description of a fragment emitting 0..n values of type `A`, which requires a
  * context providing `Ctx` to run in. A fragment requiring nothing is `Flow[A, Any]`.
  */
sealed trait Flow[A, -Ctx] {
	import Flow._
	def flatMap[B, C](f: Var[A] => Flow[B, C])(implicit pos: SourcePos): Flow[B, Ctx & C] = Bind(pos, this, f)

	def map[B](f: Var[A] => Var[B])(implicit pos: SourcePos): Flow[B, Ctx] = Bind(pos, this, a => Flow.Pure(f(a)))

	def as[B](v: Var[B]): Flow[B, Ctx] = Bind(Var.internal, this, (_: Var[A]) => Pure(v))

	def void: Flow[Unit, Ctx] = as(Var.unit)

	def agg(agg: corama.Agg[A])(implicit tag: Tag[A]): Flow[A, Ctx & Batch] = Flow.Combine(tag, this, agg)
}

object Flow {
	private[corama] case class Pure[A](v: Var[A]) extends Flow[A, Any]

	private[corama] case class Bind[A, B, C1, C2](pos: SourcePos, fa: Flow[A, C1], k: Var[A] => Flow[B, C2]) extends Flow[B, C1 & C2]
	private[corama] case class MapN[O](tag: Tag[O], pos: SourcePos, ins: List[Var[Any]], f: List[Any] => O) extends Flow[O, Any]

	private[corama] case class ExplodeOp[O](tag: Tag[O], pos: SourcePos, ins: List[Var[Any]], f: List[Any] => Seq[O]) extends Flow[O, Any]
	private[corama] case class FilterOp[I](pos: SourcePos, ins: List[Var[Any]], f: List[Any] => Boolean) extends Flow[Unit, Any]
	private[corama] case class Partition[I](in: Var[I]) extends Flow[Unit, Any]

	private[corama] case class If[A, C0, C1, C2](tag: Tag[A], cond: Flow[Boolean, C0], ifBlock: Flow[A, C1], elseBlock: Flow[A, C2]) extends Flow[A, C0 & C1 & C2]
	private[corama] case class Combine[A, Ctx](tag: Tag[A], block: Flow[A, Ctx], agg: corama.Agg[A]) extends Flow[A, Ctx & Batch]

	// PState. Reads are available anywhere — a query topology reads PStates, that is
	// what it is for. Writes require `Etl`: "a PState can only be updated by the ETL
	// topology that owns it".
	private[corama] case class Get[A](lens: ValueLens[A]) extends Flow[Option[A], Any]
	private[corama] case class Put[A](lens: ValueLens[A], value: Var[A]) extends Flow[Unit, Etl]
	private[corama] case class Update[A](pos: SourcePos, lens: ValueLens[A], ins: List[Var[Any]], f: (Option[Any], List[Any]) => Option[Any]) extends Flow[Unit, Etl]

	private[corama] case class Select[A](lens: SetLens[A], options: SelectOptions[A]) extends Flow[List[A], Any]
	private[corama] case class Add[A](lens: SetLens[A], value: Var[A]) extends Flow[Unit, Etl]
	private[corama] case class Remove[A](lens: SetLens[A], value: Var[A]) extends Flow[Unit, Etl]

	private[corama] case class LocalAppend[A](depot: Depot[A], key: Var[A]) extends Flow[Unit, Etl]

	def of[A](v: Var[A]): Flow[A, Any] = Pure(v)

	def pure[A](value: A)(implicit pos: SourcePos, tag: Tag[A]): Flow[A, Any] = {
		// TODO this can probably be optimised
		Flow.MapN(tag, pos, Nil, _ => value)
	}

	def foreach[O](f: () => O)(implicit pos: SourcePos, tag: Tag[O]): Flow[O, Any] = MapN(tag, pos, Nil, _ => f())

	/** Move downstream execution to the partition owning `v`.
	  */
	def hashPartition[A](v: Var[A]): Flow[Unit, Any] = Partition(v)

	def unit: Flow[Unit, Any] = Pure(Var.unit)
}
