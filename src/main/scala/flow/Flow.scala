package flow

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid

import flow.internal._

/** A description of a fragment emitting 0..n values of type `A`, which requires a
  * context providing `Ctx` to run in. A fragment requiring nothing is `Flow[A, Any]`.
  */
sealed trait Flow[A, -Ctx] {
	import Flow._
	def flatMap[B, C](f: Var[A] => Flow[B, C])(implicit pos: SourcePos): Flow[B, Ctx & C] = Bind(pos, this, f)

	def map[B](f: Var[A] => Var[B])(implicit pos: SourcePos): Flow[B, Ctx] = Bind(pos, this, a => Flow.Pure(f(a)))

	def as[B](v: Var[B]): Flow[B, Ctx] = Bind(Var.internal, this, (_: Var[A]) => Pure(v))

	def void: Flow[Unit, Ctx] = as(Var.unit)

	// TODO: remove?
	def aggList: Flow[List[A], Ctx & Batch] = Flow.AggList(this)

	def agg[B](agg: flow.Agg[B]): Flow[B, Ctx & Batch] = Flow.Agg(this, agg)
}

object Flow {
	private[flow] case class Pure[A](v: Var[A]) extends Flow[A, Any]

	private[flow] case class Bind[A, B, C1, C2](pos: SourcePos, fa: Flow[A, C1], k: Var[A] => Flow[B, C2]) extends Flow[B, C1 & C2]
	private[flow] case class MapN[O](pos: SourcePos, ins: List[Var[Any]], f: List[Any] => O) extends Flow[O, Any]

	private[flow] case class ExplodeOp[I, O](pos: SourcePos, in: Var[I], f: I => Seq[O]) extends Flow[O, Any]
	private[flow] case class FilterOp[I](pos: SourcePos, in: Var[I], p: I => Boolean) extends Flow[Unit, Any]
	private[flow] case class Partition[I](in: Var[I]) extends Flow[Unit, Any]

	private[flow] case class If[A, Ccond, C1, C2](cond: Flow[Boolean, Ccond], ifBlock: Flow[A, C1], elseBlock: Flow[A, C2]) extends Flow[A, C1 & C2]
	private[flow] case class AggList[A, C](block: Flow[A, C]) extends Flow[List[A], C & Batch]
	private[flow] case class Agg[A, B, C](block: Flow[A, C], agg: flow.Agg[B]) extends Flow[B, C & Batch]

	// PState. Reads are available anywhere — a query topology reads PStates, that is
	// what it is for. Writes require `Etl`: "a PState can only be updated by the ETL
	// topology that owns it".
	private[flow] case class Select[K, V](ps: PState[K, V], key: Var[K]) extends Flow[Option[V], Any]
	private[flow] case class Update[K, V](pos: SourcePos, ps: PState[K, V], key: Var[K], f: Option[V] => Option[V]) extends Flow[Unit, Etl]
	private[flow] case class Put[K, V](ps: PState[K, V], key: Var[K], value: Var[V]) extends Flow[Unit, Etl]
	private[flow] case class UpdateWith[K, V, A](pos: SourcePos,  ps: PState[K, V], key: Var[K], arg: Var[A], f: (Option[V], A) => Option[V]) extends Flow[Unit, Etl]

	// Depot. `Etl` for the same reason: an append is an effect on the module's state,
	// and a query is invoked by a client to compute a value.
	private[flow] case class PartitionAppend[A](depot: Depot[A], key: Var[A]) extends Flow[Unit, Etl]

	private[flow] case class AckReturn[O](value: Var[O]) extends Flow[Unit, AckReturning[O]]

	def of[A](v: Var[A]): Flow[A, Any] = Pure(v)

	def pure[A](value: A)(implicit pos: SourcePos): Flow[A, Any] = {
		// TODO this can probably be optimised
		Flow.MapN(pos, Nil, _ => value)
	}

	def foreach[O](f: () => O)(implicit pos: SourcePos): Flow[O, Any] = MapN(pos, Nil, _ => f())

	/** Move downstream execution to the partition owning `v`.
	  */
	def hashPartition[A](v: Var[A]): Flow[Unit, Any] = Partition(v)

	/** Works from any partition — Rama routes the value to the appender rather than to
	  * the origin task, which is what makes this usable after a `hashPartition`. Note it
	  * does not end the flow: execution continues, and a fanned-out flow can reach this
	  * more than once, which is what [[AckAgg]] exists to resolve.
	  */
	def ackReturn[O](v: Var[O]): Flow[Unit, AckReturning[O]] = AckReturn(v)

	def unit: Flow[Unit, Any] = Pure(Var.unit)
}
