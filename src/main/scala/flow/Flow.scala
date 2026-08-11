package flow

private[flow] final class VarId(private[flow] val id: Int, private[flow] val hint: String) {
	override def equals(other: Any): Boolean = other match {
		case that: VarId => that.id == id
		case _           => false
	}
	override def hashCode: Int = id
	override def toString: String = s"$hint#$id"
}

final class Var[A] private[flow] (private[flow] val vid: VarId) {

	def map[B](f: A => B): Flow[B, Any] = Flow.Each1(this, f)

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
}

final case class TopologyId(value: Int) extends AnyVal

// Allocates var ids within one topology.
private[flow] class VarRegistry {
	private var defined = 0

	def add[A](hint: String): Var[A] = {
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

final class TopologyBuilder[S] private[flow] (
	val id: TopologyId,
	// source may be a Depot or Query, it won't be a PState
	val source: Identity
	) {

	private[flow] val vars = new VarRegistry()

	val input: Var[S] = vars.add[S](source.name) // mint a generated var name for elements in the source

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

	private def claim(identity: Identity): Identity = {
		if (ids.contains(identity)) {
			throw new RuntimeException(s"Multiple definition of $identity")
		}
		ids = ids.incl(identity)
		identity
	}

	def pstate[K, V](name: String): PState[K, V] = PState[K, V](claim(Identity(IdentityNamespace.PState, name)))

	def depot[A](name: String): Depot[A] = Depot[A](claim(Identity(IdentityNamespace.Depot, name)))

	/** Declare a query topology: invoked by a client, returns exactly one value.
	  */
	def query[A, R](name: String)(
		block: TopologyBuilder[A] => Flow[R, QueryContext]
	): Topology[A, R] = {
		declare[A, R, QueryContext](claim(Identity(IdentityNamespace.Query, name)))(block)
	}

	/** Attach a stream topology to a depot: driven by an append, writes PStates,
	  * returns nothing.
	  */
	def stream[A](depot: Depot[A])(
		block: TopologyBuilder[A] => Flow[Unit, StreamContext]
	): Topology[A, Unit] = {
		declare[A, Unit, StreamContext](depot.id)(block)
	}

	private def declare[A, R, C](source: Identity)(
		block: TopologyBuilder[A] => Flow[R, C]
	): Topology[A, R] = {
		val b = new TopologyBuilder[A](TopologyId(built.length), source)
		val t = Topology(b, block(b))
		built = t :: built
		t
	}
}

object Topologies {
	def apply(): Topologies = new Topologies()
}

final case class Topology[S, R](builder: TopologyBuilder[S], flow: Flow[R, Nothing]) {
	def input: Var[S] = builder.input
	val compiled = Node.compile(builder, flow)
}

/** A description of a fragment emitting 0..n values of type `A`, which requires a
  * context providing `Ctx` to run in. A fragment requiring nothing is `Flow[A, Any]`.
  */
sealed trait Flow[A, -Ctx] {
	import Flow._

	def flatMap[B, C](f: Var[A] => Flow[B, C]): Flow[B, Ctx & C] = Bind(this, f)

	def map[B](f: Var[A] => Var[B]): Flow[B, Ctx] = Bind(this, (v: Var[A]) => Pure(f(v)))

	def as[B](v: Var[B]): Flow[B, Ctx] = Bind(this, (_: Var[A]) => Pure(v))

	def void: Flow[Unit, Ctx] = as(Var.unit)

	/** Collapse this fan-out region into one list — Rama's `agg(Agg.list(...)).out(v)`.
	  *
	  * Requires [[Batch]]: this is the value-producing form of `agg`, legal only inside
	  * a batch block, so a stream topology cannot use it.
	  */
	def aggList: Flow[List[A], Ctx & Batch] = Flow.Agg(this)
}

object Flow {
	private[flow] case class Pure[A](v: Var[A]) extends Flow[A, Any]

	// TODO: consider HList?
	private[flow] case class Each1[I, O](in: Var[I], f: I => O) extends Flow[O, Any]
	private[flow] case class Each2[A, B, O](a: Var[A], b: Var[B], f: (A, B) => O) extends Flow[O, Any]

	private[flow] case class ExplodeOp[I, O](in: Var[I], f: I => Seq[O]) extends Flow[O, Any]
	private[flow] case class FilterOp[I](in: Var[I], p: I => Boolean) extends Flow[Unit, Any]
	private[flow] case class Partition[I](in: Var[I]) extends Flow[Unit, Any]
	private[flow] case class Bind[A, B, C1, C2](fa: Flow[A, C1], k: Var[A] => Flow[B, C2]) extends Flow[B, C1 & C2]
	private[flow] case class Agg[A, C](inner: Flow[A, C]) extends Flow[List[A], C & Batch]

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

	def pure[A](v: Var[A]): Flow[A, Any] = Pure(v)

	def map2[A, B, O](a: Var[A], b: Var[B])(f: (A, B) => O): Flow[O, Any] = Each2(a, b, f)

	/** Move downstream execution to the partition owning `v`.
	  */
	def hashPartition[A](v: Var[A]): Flow[Unit, Any] = Partition(v)

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

	def update(key: Var[K])(f: Option[V] => Option[V]): Flow[Unit, Etl] = Flow.Update(this, key, f)

	def updateWith[A](key: Var[K], arg: Var[A])(f: (Option[V], A) => Option[V]): Flow[Unit, Etl] =
		Flow.UpdateWith(this, key, arg, f)

	def put(key: Var[K], value: Var[V]): Flow[Unit, Etl] = Flow.Put(this, key, value)
	def delete(key: Var[K]): Flow[Unit, Etl] = Flow.Update(this, key, _ => None)
}

final case class Depot[A](id: Identity) {
	// append to the current partition of the depot. Care must be taken to ensure this
	// matches the partitioning scheme of the depot.
	// TODO: this could fail at runtime (or compile time?) if you're not on the right partition for a depot
	// with deterministic partitioning.
	def partitionAppend(value: Var[A]): Flow[Unit, Etl] = Flow.PartitionAppend(this, value)
}

// ---------------------------------------------------------------------------
// base AST
// ---------------------------------------------------------------------------

private[flow] sealed trait Node
private[flow] object Node {
	case class Each(fn: List[Any] => Any, ins: List[VarId], out: VarId) extends Node

	case class Explode(fn: Any => Seq[Any], in: VarId, out: VarId) extends Node

	/** `flag` is where the predicate's result is bound before Rama's `keepTrue` reads
	  * it. Unused by `interp` backend.
		*/
	case class Filter(fn: Any => Boolean, in: VarId, flag: VarId) extends Node

	case class Partition(in: VarId) extends Node

	case class Scope(inner: List[Node], collectFrom: VarId, out: VarId) extends Node

	case class Select(pstate: Identity, key: VarId, out: VarId) extends Node
	case class Update(pstate: Identity, key: VarId, fn: Option[Any] => Option[Any]) extends Node
	case class Put(pstate: Identity, key: VarId, value: VarId) extends Node
	case class UpdateWith(pstate: Identity, key: VarId, arg: VarId, fn: (Option[Any], Any) => Option[Any]) extends Node
	case class PartitionAppend(depot: Identity, arg: VarId) extends Node


	/** Compiles the higher-order [[Flow]] to a [[Node]] list.
		*
		* TODO: remove unnecessary var return?
		*/
	def compile[A](t: TopologyBuilder[?], flow: Flow[A, ?]): (List[Node], Var[A]) = {
		flow match {
			case Flow.Pure(v) => (Nil, v)

			case e: Flow.Each1[i, A] =>
				val out = t.define[A]("each")
				(List(Node.Each(args => e.f(args.head.asInstanceOf[i]), List(e.in.vid), out.vid)), out)

			case e: Flow.Each2[a, b, A] =>
				val out = t.define[A]("each")
				val fn = (args: List[Any]) => e.f(args(0).asInstanceOf[a], args(1).asInstanceOf[b])
				(List(Node.Each(fn, List(e.a.vid, e.b.vid), out.vid)), out)

			case e: Flow.ExplodeOp[i, A] =>
				val out = t.define[A]("exploded")
				val fn = (arg: Any) => e.f(arg.asInstanceOf[i]).asInstanceOf[Seq[Any]]
				(List(Node.Explode(fn, e.in.vid, out.vid)), out)

			case f: Flow.FilterOp[i] =>
				val fn = (arg: Any) => f.p(arg.asInstanceOf[i])
				val flag = t.define[Boolean](s"${f.in.vid.hint}_keep")
				(List(Node.Filter(fn, f.in.vid, flag.vid)), Var.unit.asInstanceOf[Var[A]])

			case p: Flow.Partition[i] =>
				(List(Node.Partition(p.in.vid)), Var.unit.asInstanceOf[Var[A]])

			case b: Flow.Bind[a, A, c1, c2] =>
				val (n1, v1) = compile(t, b.fa)
				val (n2, v2) = compile(t, b.k(v1))
				(n1 ++ n2, v2)

			case a: Flow.Agg[x, c] =>
				val (inner, innerVar) = compile(t, a.inner)
				val out = t.define[A]("agg")
				(List(Node.Scope(inner, innerVar.vid, out.vid)), out)

			case s: Flow.Select[k, v] =>
				val out = t.define[A]("select")
				(List(Node.Select(s.ps.id, s.key.vid, out.vid)), out)

			case u: Flow.Update[k, v] =>
				val fn = (cur: Option[Any]) => u.f(cur.asInstanceOf[Option[v]]).asInstanceOf[Option[Any]]
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
		}
	}
}
