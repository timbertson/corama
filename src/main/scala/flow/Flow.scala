package flow

import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq

private[flow] final class VarId(private[flow] val id: Int, private[flow] val hint: String) {
	override def equals(other: Any): Boolean = other match {
		case that: VarId => that.id == id
		case _           => false
	}
	override def hashCode: Int = id
	override def toString: String = s"$hint#$id"
}

object VarId {
	implicit val eq: Eq[VarId] = Eq.fromUniversalEquals
}

final class Var[A] private[flow] (private[flow] val vid: VarId) {
	private def asAny: Var[Any] = this.asInstanceOf[Var[Any]]

	def map[B](f: A => B): Flow[B, Any] = Flow.MapN(List(this.asAny), {
		case List(a) => f(a.asInstanceOf[A])
			case _ => ??? // impossible
	})
	def zipMap[B, C](b: Var[B])(f: (A, B) => C): Flow[C, Any] = Flow.MapN(List(this.asAny, b.asAny), {
		case List(a,b) => f(a.asInstanceOf[A], b.asInstanceOf[B])
		case _ => ??? // impossible
	})

	def zipMap[B, C, D](b: Var[B], c: Var[C])(f: (A, B, C) => D): Flow[D, Any] = Flow.MapN(List(this.asAny, b.asAny, c.asAny), {
		case List(a,b, c) => f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C])
		case _ => ??? // impossible
	})

	def foreach(f: A => Unit) = map[Unit](f)

	def into[F[_]](using f: Applicative[F]): Flow[F[A], Any] = this.map(f.pure)

	/** Fan out: emit one value per element, running everything downstream per element. */
	def explode[B](f: A => Seq[B]): Flow[B, Any] = Flow.ExplodeOp(this, f)

	def filter(p: A => Boolean): Flow[A, Any] = Flow.FilterOp(this, p).as(this)

	def void: Flow[Unit, Any] = Flow.Pure(Var.unit)

	override def toString: String = s"Var($vid)"
	override def equals(other: Any): Boolean = other match {
		case that: Var[?] => that.vid == vid
		case _            => false
	}
	override def hashCode: Int = vid.hashCode
}

object Var {
	// Virtual vars have a unique id < 0 and are never given a Rama name
	val unit: Var[Unit] = new Var[Unit](new VarId(-2, "unit"))

	extension [A](v: Var[Option[A]]) {
		def fold[B, C](ifNone: Flow[B, C])(ifSome: Var[A] => Flow[B, C]): Flow[B, C] =
			Flow.If[B, Any, C, C](
				v.map(_.isEmpty),
				ifNone,
				v.map(_.get).flatMap(ifSome)
			)
	}
}

final case class TopologyId(value: Int) extends AnyVal

// Allocates var ids within one topology.
private[flow] class VarRegistry {
	private var defined = 0

	def add[A](hint: String): Var[A] = {
		// println(s"Adding var: $hint")
		val v = new Var[A](new VarId(defined, hint))
		defined += 1
		v
	}

	def count: Int = defined
}

enum TopologyKind {
	// TODO: add MicroBatch
	case Stream, Query
}

// S represents the source type, e.g. the type of elements from a depot
// or query input
final class TopologyBuilder[S] private[flow] (
	val id: TopologyId,
	// source may be a Depot or Query, it won't be a PState
	val source: Identity
	) {

	private[flow] val vars = new VarRegistry()

	val input: Var[S] = vars.add[S](source.name) // mint a generated var name for elements in the source

	val output: Var[S] = vars.add[S]("out") // mint a generated var name for output. Only used for queries

	def define[A](hint: String = "var"): Var[A] = vars.add[A](hint)

	override def toString: String = s"TopologyBuilder($source)"
}

/** The topologies of one module, and the declarations they share.
  *
  * PState names, depot names and query names are enforced to be unique within this module.
  */
final class Topologies {
	private [flow] var ids = Set.empty[Identity]
	private var built = List.empty[Topology[?, ?]]

	def topologies: List[Topology[?, ?]] = built.reverse

	private[flow] def declared(t: Topology[?, ?]): Boolean = built.exists(_ eq t)

	private def claim(identity: Identity): Identity = {
		if (ids.contains(identity)) {
			throw new RuntimeException(s"Multiple definition of $identity")
		}
		ids = ids.incl(identity)
		identity
	}

	def pstate[K, V](name: String, @unused /* TODO */ schame: Any = ()): PState[K, V] = PState[K, V](claim(Identity(IdentityNamespace.PState, name)))

	def depot[A](name: String, @unused /* TODO */ partitionBy: Any = ()): Depot[A] = Depot[A](claim(Identity(IdentityNamespace.Depot, name)))

	/** Declare a query topology: invoked by a client, returns exactly one value.
	  */
	def query[Input, Output](name: String, agg: Agg[Output])(
		block: TopologyBuilder[Input] => Flow[Output, QueryContext],
	): QueryTopology[Input, Output] = {
		QueryTopology(declare[Input, Output, QueryContext](claim(Identity(IdentityNamespace.Query, name)), agg)(block))
	}

	/** Attach a stream topology to a depot: driven by an append, writes PStates,
	  * returns nothing.
	  *
	  * The context provided is `StreamContext[Nothing]`, so [[Flow.ackReturn]] does not
	  * compile here — there is no declared aggregator for a value to reach.
	  *
		* TODO: does the block need to accept TopologyBuilder, or is Flow[A] / Var[A] enough?
	  */
	def stream[A](depot: Depot[A])(
		block: TopologyBuilder[A] => Flow[Unit, StreamContext[Nothing]]
	): Topology[A, Unit] = {
		declare[A, Unit, StreamContext[Nothing]](depot.id, Agg.unit)(block)
	}

	/** Attach a stream topology that hands a value back to whoever appended.
	  *
	  * The aggregator determines the topology's `R`: a flow emits 0..n ack returns across
	  * however many partitions its branches reached, and [[AckAgg]] says how those become
	  * the one value an `AckLevel.Ack` append reads out of [[AckReturns]]. Rama declares
	  * it the same way, as `StreamSourceOptions.ackReturnAgg`.
	  */
	def stream[A, O](depot: Depot[A], agg: Agg[O])(
		block: TopologyBuilder[A] => Flow[Unit, StreamContext[O]]
	): StreamTopology[A, O] = {
		StreamTopology(declare[A, O, StreamContext[O]](depot.id, agg)(block))
	}

	private def declare[A, R, C](source: Identity, agg: Agg[R])(
		block: TopologyBuilder[A] => Flow[?, C]
	): Topology[A, R] = {
		val b = new TopologyBuilder[A](TopologyId(built.length), source)
		val t = Topology[A, R](b, block(b), agg)
		built = t :: built
		t
	}
}

object Topologies {
	def apply(): Topologies = new Topologies()
}

/** A declared topology, and how it produces its `R`.
  *
  * `R` is returned in two ways: a query returns the value its flow ends on, while a stream returns
  * whatever `ackAgg` makes of the flow's ack returns (and a stream without one returns
  * `Unit`). So the flow is stored existentially and `R` is tied down at the declaration
  * site instead — by `query`'s `Flow[R, QueryContext]`, or by `ackAgg`.
  */
final case class Topology[S, R](
	builder: TopologyBuilder[S],
	flow: Flow[?, Nothing],
	agg: Agg[R]) {
	def input: Var[S] = builder.input
	val compiled = Node.compile(builder, flow)
}

// wrappers for the different types of topology
final case class QueryTopology[S, R] private[flow](topology: Topology[S, R])

final case class StreamTopology[S, R] private[flow](topology: Topology[S, R])

/** A description of a fragment emitting 0..n values of type `A`, which requires a
  * context providing `Ctx` to run in. A fragment requiring nothing is `Flow[A, Any]`.
  */
sealed trait Flow[A, -Ctx] {
	import Flow._
	def flatMap[B, C](f: Var[A] => Flow[B, C]): Flow[B, Ctx & C] = Bind(this, f)

	def map[B](f: Var[A] => Var[B]): Flow[B, Ctx] = Bind(this, a => Flow.Pure(f(a)))

	def as[B](v: Var[B]): Flow[B, Ctx] = Bind(this, (_: Var[A]) => Pure(v))

	def void: Flow[Unit, Ctx] = as(Var.unit)

	// TODO: remove?
	def aggList: Flow[List[A], Ctx & Batch] = Flow.AggList(this)

	def agg[B](agg: flow.Agg[B]): Flow[B, Ctx & Batch] = Flow.Agg(this, agg)

	// def query[R](agg: AckAgg[A, R]) = {
	// 	new Query[A, R, Ctx](this, agg)
	// }
}

// case class Query[A,R,-Ctx](flow: Flow[A, Ctx], agg: AckAgg[A, R])

object Flow {
	private[flow] case class Pure[A](v: Var[A]) extends Flow[A, Any]

	private[flow] case class Bind[A, B, C1, C2](fa: Flow[A, C1], k: Var[A] => Flow[B, C2]) extends Flow[B, C1 & C2]
	private[flow] case class MapN[O](ins: List[Var[Any]], f: List[Any] => O) extends Flow[O, Any]

	private[flow] case class ExplodeOp[I, O](in: Var[I], f: I => Seq[O]) extends Flow[O, Any]
	private[flow] case class FilterOp[I](in: Var[I], p: I => Boolean) extends Flow[Unit, Any]
	private[flow] case class Partition[I](in: Var[I]) extends Flow[Unit, Any]

	private[flow] case class If[A, Ccond, C1, C2](cond: Flow[Boolean, Ccond], ifBlock: Flow[A, C1], elseBlock: Flow[A, C2]) extends Flow[A, C1 & C2]
	private[flow] case class AggList[A, C](block: Flow[A, C]) extends Flow[List[A], C & Batch]
	private[flow] case class Agg[A, B, C](block: Flow[A, C], agg: flow.Agg[B]) extends Flow[B, C & Batch]

	// PState. Reads are available anywhere — a query topology reads PStates, that is
	// what it is for. Writes require `Etl`: "a PState can only be updated by the ETL
	// topology that owns it".
	private[flow] case class Select[K, V](ps: PState[K, V], key: Var[K]) extends Flow[Option[V], Any]
	private[flow] case class Update[K, V](ps: PState[K, V], key: Var[K], f: Option[V] => Option[V]) extends Flow[Unit, Etl]
	private[flow] case class Put[K, V](ps: PState[K, V], key: Var[K], value: Var[V]) extends Flow[Unit, Etl]
	private[flow] case class UpdateWith[K, V, A]( ps: PState[K, V], key: Var[K], arg: Var[A], f: (Option[V], A) => Option[V]) extends Flow[Unit, Etl]

	// Depot. `Etl` for the same reason: an append is an effect on the module's state,
	// and a query is invoked by a client to compute a value.
	private[flow] case class PartitionAppend[A](depot: Depot[A], key: Var[A]) extends Flow[Unit, Etl]

	private[flow] case class AckReturn[O](value: Var[O]) extends Flow[Unit, AckReturning[O]]

	def of[A](v: Var[A]): Flow[A, Any] = Pure(v)

	def pure[A](value: A): Flow[A, Any] = {
		// TODO this can probably be optimised
		Flow.MapN(Nil, _ => value)
	}

	def foreach[O](f: () => O): Flow[O, Any] = MapN(Nil, _ => f())

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

sealed trait IdentityNamespace
object IdentityNamespace {
	case object PState extends IdentityNamespace
	case object Depot extends IdentityNamespace
	case object Query extends IdentityNamespace
}

// Shared trait for PState, Depot and Query which all
// have publically referenceable names
private case class Identity(namespace: IdentityNamespace, name: String)

final case class PState[K, V](id: Identity) {
	def get(key: Var[K]): Flow[Option[V], Any] = Flow.Select(this, key)

	def getOrElse(key: Var[K], dfl: V): Flow[V, Any] = Flow.Select(this, key).flatMap(v => v.map(_.getOrElse(dfl)))

	def update(key: Var[K])(f: Option[V] => Option[V]): Flow[Unit, Etl] = Flow.Update(this, key, f)

	def update[A](key: Var[K], arg: Var[A])(f: (Option[V], A) => Option[V]): Flow[Unit, Etl] =
		Flow.UpdateWith(this, key, arg, f)

	def put(key: Var[K], value: Var[V]): Flow[Unit, Etl] = Flow.Put(this, key, value)
	def delete(key: Var[K]): Flow[Unit, Etl] = Flow.Update(this, key, _ => None)
}

final case class Depot[A](id: Identity) {
	// append to the current partition of the depot. Care must be taken to ensure this
	// matches the partitioning scheme of the depot.
	// TODO: this could fail at runtime (or compile time?) if you're not on the right partition for a depot
	// with deterministic partitioning.
	def localAppend(value: Var[A]): Flow[Unit, Etl] = Flow.PartitionAppend(this, value)
}

// ---------------------------------------------------------------------------
// base AST
// ---------------------------------------------------------------------------

private[flow] sealed trait Node
private[flow] object Node {
	case class MapN(fn: List[Any] => Any, ins: List[VarId], out: VarId) extends Node

	case class Explode(fn: Any => Seq[Any], in: VarId, out: VarId) extends Node

	case class Assign(in: VarId, out: VarId) extends Node

	/** `flag` is where the predicate's result is bound before Rama's `keepTrue` reads
	  * it. Unused by `interp` backend.
		*/
	case class Filter(fn: Any => Boolean, in: VarId, flag: VarId) extends Node

	case class Partition(in: VarId) extends Node

	case class If(cond: VarId, ifBlock: List[Node], elseBlock: List[Node], out: VarId) extends Node

	// TODO: this is a special case of Agg, is it necessary?
	case class AggList(block: List[Node], collectFrom: VarId, out: VarId) extends Node

	case class Agg(block: List[Node], agg: flow.Agg[?], collectFrom: VarId, out: VarId) extends Node

	case class Select(pstate: Identity, key: VarId, out: VarId) extends Node
	case class Update(pstate: Identity, key: VarId, fn: Option[Any] => Option[Any]) extends Node
	case class Put(pstate: Identity, key: VarId, value: VarId) extends Node
	case class UpdateWith(pstate: Identity, key: VarId, arg: VarId, fn: (Option[Any], Any) => Option[Any]) extends Node
	case class PartitionAppend(depot: Identity, arg: VarId) extends Node
	case class AckReturn(value: VarId) extends Node

	// case class Aggregate(agg: AckAgg[?,?], in: VarId, out: VarId) extends Node

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
				val out = t.define[A]("map")
				val f = e.f
				(List(Node.MapN(args => f(args), List(), out.vid)), out)

			case e: Flow.ExplodeOp[i, A] =>
				val out = t.define[A]("exploded")
				val f = e.f
				val fn = (arg: Any) => f(arg.asInstanceOf[i]).asInstanceOf[Seq[Any]]
				(List(Node.Explode(fn, e.in.vid, out.vid)), out)

			case e: Flow.FilterOp[i] =>
				val p = e.p
				val fn = (arg: Any) => p(arg.asInstanceOf[i])
				val flag = t.define[Boolean](s"${e.in.vid.hint}_keep")
				(List(Node.Filter(fn, e.in.vid, flag.vid)), Var.unit.asInstanceOf[Var[A]])

			case p: Flow.Partition[i] =>
				(List(Node.Partition(p.in.vid)), Var.unit.asInstanceOf[Var[A]])

			case b: Flow.Bind[a, A, c1, c2] =>
				val (n1, v1) = compile(t, b.fa)
				val (n2, v2) = compile(t, b.k(v1))
				(n1 ++ n2, v2)

			case i: Flow.If[a, ccond, c1, c2] =>
				val out = t.define[a]("ifOut")
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
				val out = t.define[A]("aggList")
				(List(Node.AggList(block, innerVar.vid, out.vid)), out)

			case a: Flow.Agg[a, b, c] =>
				val (block, innerVar) = compile(t, a.block)
				val out = t.define[A]("agg")
				(List(Node.Agg(block, a.agg, innerVar.vid, out.vid)), out)

			case s: Flow.Select[k, v] =>
				val out = t.define[A]("select")
				(List(Node.Select(s.ps.id, s.key.vid, out.vid)), out)

			case u: Flow.Update[k, v] =>
				val f = u.f
				val fn = (cur: Option[Any]) => f(cur.asInstanceOf[Option[v]]).asInstanceOf[Option[Any]]
				(List(Node.Update(u.ps.id, u.key.vid, fn)), Var.unit.asInstanceOf[Var[A]])

			case Flow.Put(ps, key, value) =>
				(List(Node.Put(ps.id, key.vid, value.vid)), Var.unit.asInstanceOf[Var[A]])

			case up: Flow.UpdateWith[k, v, a] =>
				up match {
					case Flow.UpdateWith(ps, key, arg, fn) =>
						val fnWrapped = (cur: Option[Any], arg: Any) =>
							fn(cur.asInstanceOf[Option[v]], arg.asInstanceOf[a]).asInstanceOf[Option[Any]]
						(List(Node.UpdateWith(ps.id, key.vid, arg.vid, fnWrapped)), Var.unit.asInstanceOf[Var[A]])
				}

			case Flow.PartitionAppend(depot, value) =>
				(List(Node.PartitionAppend(depot.id, value.vid)), Var.unit.asInstanceOf[Var[A]])

			case Flow.AckReturn(value) =>
				(List(Node.AckReturn(value.vid)), Var.unit.asInstanceOf[Var[A]])
		}
	}
}
