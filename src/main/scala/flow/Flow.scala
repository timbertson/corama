package flow

private[flow] final class VarId(private[flow] val id: Int, private[flow] val hint: String) {
	override def equals(other: Any): Boolean = other match {
		case that: VarId => that.id == id
		case _           => false
	}
	override def hashCode: Int = id
	override def toString: String = s"$hint#$id"
}

/** A name standing for one runtime value of type `A`.
  *
  * Not constructible outside this package: names are minted, never written. That is
  * what keeps the namespace flat-but-safe — a user cannot pick a name that collides
  * with one [[Lower]] or [[RamaBackend]] will generate later.
  */
final class Var[A] private[flow] (private[flow] val vid: VarId) {

	/** One value in, one value out: a Rama `each` with a pure function. */
	def map[B](f: A => B): Flow[B] = Flow.Each1(this, f)

	/** Fan out: emit one value per element, running everything downstream per element. */
	def explode[B](f: A => Seq[B]): Flow[B] = Flow.ExplodeOp(this, f)

	/** Keep going only when the predicate holds — a Rama `keepTrue`. */
	def filter(p: A => Boolean): Flow[A] = Flow.FilterOp(this, p).as(this)

	def void: Flow[Unit] = Flow.Pure(Var.unit)

	override def toString: String = s"Var($vid)"
	override def equals(other: Any): Boolean = other match {
		case that: Var[?] => that.vid == vid
		case _            => false
	}
	override def hashCode: Int = vid.hashCode
}

object Var {
	/** Virtual vars have a unique id < 0 and are never given a Rama name — the same
	  * convention as `ramato.dsl.Var`, where `taskId` is -1 and `unit` is -2.
	  */
	val unit: Var[Unit] = new Var[Unit](new VarId(-2, "unit"))
}

final case class TopologyId(value: Int) extends AnyVal

/** Allocates var ids within one topology.
  *
  * Ids are positions, handed out from 0 in construction order, so they are stable
  * for a given program without any coordination. That is what lets [[RamaBackend]]
  * render `*hint_id` directly rather than re-numbering vars in traversal order.
  *
  * Mutation, not threadsafe — as `ramato.dsl.VarRegistry`.
  */
private[flow] class VarRegistry {
	private var defined = 0

	def add[A](hint: String): Var[A] = {
		val v = new Var[A](new VarId(defined, hint))
		defined += 1
		v
	}

	def count: Int = defined
}

/** Which of Rama's two topology kinds this is.
  *
  * Not cosmetic. A stream topology is driven by depot appends, writes PStates, and
  * returns nothing. A query topology is invoked by a client, returns exactly one
  * value, and may **not** write — Rama rejects that at declaration, and so does
  * [[Lower]].
  */
enum TopologyKind {
	case Stream, Query
}

final class TopologyBuilder[S] private[flow] (
	val id: TopologyId,
	// source may be a Depot or Query, it won't be a PState
	val source: Identity
	) {

	private[flow] val vars = new VarRegistry()

	val input: Var[S] = vars.add[S](source.name) // mint a generated var name for elements in the source

	def variable[A](hint: String): Var[A] = vars.add[A](hint)

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

	// define a query which returns a value (with no side-effects)
	def query[A, R](name: String)(
		block: TopologyBuilder[A] => Flow[R]
	): Topology[A, R] = {
		declare[A, R](claim(Identity(IdentityNamespace.Query, name)))(block)
	}

	// attach a stream process to a depot, with side effects and no return value
	def stream[A](depot: Depot[A])(
		block: TopologyBuilder[A] => Flow[Unit]
	): Topology[A, Unit] = {
		declare[A, Unit](depot.id)(block)
	}

	private def declare[A, R](source: Identity)(
		block: TopologyBuilder[A] => Flow[R]
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

/** A declared topology: its builder, and the flow built against it.
  *
  * The two travel together because lowering needs both — the flow for the tree, the
  * builder for the var registry the tree's names come from.
  */
final case class Topology[S, R](builder: TopologyBuilder[S], flow: Flow[R]) {
	def input: Var[S] = builder.input
	val compiled = Node.compile(builder, flow)
}

sealed trait Flow[A] {
	import Flow._

	def flatMap[B](f: Var[A] => Flow[B]): Flow[B] = Bind(this, f)

	def map[B](f: Var[A] => Var[B]): Flow[B] = Bind(this, (v: Var[A]) => Pure(f(v)))

	def as[B](v: Var[B]): Flow[B] = Bind(this, (_: Var[A]) => Pure(v))

	def void: Flow[Unit] = as(Var.unit)

	def aggList: Flow[List[A]] = Flow.Agg(this)
}

object Flow {
	private[flow] case class Pure[A](v: Var[A]) extends Flow[A]

	// TODO: consider HList?
	private[flow] case class Each1[I, O](in: Var[I], f: I => O) extends Flow[O]
	private[flow] case class Each2[A, B, O](a: Var[A], b: Var[B], f: (A, B) => O) extends Flow[O]

	private[flow] case class ExplodeOp[I, O](in: Var[I], f: I => Seq[O]) extends Flow[O]
	private[flow] case class FilterOp[I](in: Var[I], p: I => Boolean) extends Flow[Unit]
	private[flow] case class Partition[I](in: Var[I]) extends Flow[Unit]
	private[flow] case class Bind[A, B](fa: Flow[A], k: Var[A] => Flow[B]) extends Flow[B]
	private[flow] case class Agg[A](inner: Flow[A]) extends Flow[List[A]]
	
	// PState
	private[flow] case class Select[K, V](ps: PState[K, V], key: Var[K]) extends Flow[Option[V]]
	private[flow] case class Update[K, V](ps: PState[K, V], key: Var[K], f: Option[V] => Option[V]) extends Flow[Unit]
	private[flow] case class Put[K, V](ps: PState[K, V], key: Var[K], value: Var[V]) extends Flow[Unit]
	private[flow] case class UpdateWith[K, V, A]( ps: PState[K, V], key: Var[K], arg: Var[A], f: (Option[V], A) => Option[V]) extends Flow[Unit]
	
	// Depot
	private[flow] case class Emit[A](depot: Depot[A], key: Var[A]) extends Flow[Unit]

	def pure[A](v: Var[A]): Flow[A] = Pure(v)

	def map2[A, B, O](a: Var[A], b: Var[B])(f: (A, B) => O): Flow[O] = Each2(a, b, f)

	/** Move downstream execution to the partition owning `v`.
	  *
	  * In Rama this also ends the current event, and so the current *transaction*:
	  * PState writes before and after are in different atomic units. That is
	  * invisible in Rama's Java API and still invisible here — see notes in
	  * FLOW-NOTES.md; typing it is the next piece of work, not this one.
	  */
	def hashPartition[A](v: Var[A]): Flow[Unit] = Partition(v)

	def unit: Flow[Unit] = Pure(Var.unit)
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
	def get(key: Var[K]): Flow[Option[V]] = Flow.Select(this, key)
	def update(key: Var[K])(f: Option[V] => Option[V]): Flow[Unit] = Flow.Update(this, key, f)

	/** Read-modify-write against another runtime value.
	  *
	  * Needed because `update`'s function is a *staged* closure: it runs at
	  * execution time and can only see the stored value, so referring to a `Var`
	  * inside it captures the name rather than the value. Passing the var
	  * explicitly is the fix, and mirrors Rama's `Path.term(fn, "*extraArg")`.
	  */
	def updateWith[A](key: Var[K], arg: Var[A])(f: (Option[V], A) => Option[V]): Flow[Unit] =
		Flow.UpdateWith(this, key, arg, f)
	def put(key: Var[K], value: Var[V]): Flow[Unit] = Flow.Put(this, key, value)
	def delete(key: Var[K]): Flow[Unit] = Flow.Update(this, key, _ => None)
}

final case class Depot[A](id: Identity) {
	def emit(value: Var[A]): Flow[Unit] = Flow.Emit(this, value)
}

// ---------------------------------------------------------------------------
// base AST
// ---------------------------------------------------------------------------

private[flow] sealed trait Node
private[flow] object Node {
	case class Each(fn: List[Any] => Any, ins: List[VarId], out: VarId) extends Node

	case class Explode(fn: List[Any] => Seq[Any], ins: List[VarId], out: VarId) extends Node

	/** `flag` is where the predicate's result is bound before Rama's `keepTrue` reads
	  * it. Unused by `interp` backend.
		*/
	case class Filter(fn: List[Any] => Boolean, ins: List[VarId], flag: VarId) extends Node

	case class Partition(in: VarId) extends Node

	case class Scope(inner: List[Node], collectFrom: VarId, out: VarId) extends Node

	case class Select(pstate: Identity, key: VarId, out: VarId) extends Node
	case class Update(pstate: Identity, key: VarId, fn: Option[Any] => Option[Any]) extends Node
	case class Put(pstate: Identity, key: VarId, value: VarId) extends Node
	case class UpdateWith(pstate: Identity, key: VarId, arg: VarId, fn: (Option[Any], Any) => Option[Any]) extends Node
	case class Emit(depot: Identity, arg: VarId) extends Node


	/** Compiles the higher-order [[Flow]] to a [[Node]] list.
		*
		* A query topology may not write PState (Rama rejects it at declaration), so compiling
		* a write inside one fails here.
		*/
	def compile[A](t: TopologyBuilder[?], flow: Flow[A]): (List[Node], Var[A]) = {
		flow match {
			case Flow.Pure(v) => (Nil, v)

			case e: Flow.Each1[i, A] =>
				val out = t.variable[A]("each")
				(List(Node.Each(args => e.f(args.head.asInstanceOf[i]), List(e.in.vid), out.vid)), out)

			case e: Flow.Each2[a, b, A] =>
				val out = t.variable[A]("each")
				val fn = (args: List[Any]) => e.f(args(0).asInstanceOf[a], args(1).asInstanceOf[b])
				(List(Node.Each(fn, List(e.a.vid, e.b.vid), out.vid)), out)

			case e: Flow.ExplodeOp[i, A] =>
				val out = t.variable[A]("exploded")
				val fn = (args: List[Any]) => e.f(args.head.asInstanceOf[i]).asInstanceOf[Seq[Any]]
				(List(Node.Explode(fn, List(e.in.vid), out.vid)), out)

			case f: Flow.FilterOp[i] =>
				val fn = (args: List[Any]) => f.p(args.head.asInstanceOf[i])
				val flag = t.variable[Boolean](s"${f.in.vid.hint}_keep")
				(List(Node.Filter(fn, List(f.in.vid), flag.vid)), Var.unit.asInstanceOf[Var[A]])

			case p: Flow.Partition[i] =>
				(List(Node.Partition(p.in.vid)), Var.unit.asInstanceOf[Var[A]])

			case b: Flow.Bind[a, A] =>
				val (n1, v1) = compile(t, b.fa)
				val (n2, v2) = compile(t, b.k(v1))
				(n1 ++ n2, v2)

			case a: Flow.Agg[x] =>
				val (inner, innerVar) = compile(t, a.inner)
				val out = t.variable[A]("agg")
				(List(Node.Scope(inner, innerVar.vid, out.vid)), out)

			case s: Flow.Select[k, v] =>
				val out = t.variable[A]("select")
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

			case Flow.Emit(depot, value) =>
				(List(Node.Emit(depot.id, value.vid)), Var.unit.asInstanceOf[Var[A]])
		}
	}
}
