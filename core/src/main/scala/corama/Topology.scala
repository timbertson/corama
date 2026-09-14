package corama

import corama.internal.*
import izumi.reflect.Tag
import org.tpolecat.sourcepos.*

import scala.annotation.unused

final case class TopologyId(value: Int) extends AnyVal

enum TopologyKind {
	// TODO: add MicroBatch
	case Stream, Query
}

// S represents the source type, e.g. the type of elements from a depot
// or query input
final class TopologyBuilder[S] private[corama] (
	val id: TopologyId,
	// source may be a Depot or Query, it won't be a PState
	val source: Identity,
	tag: Tag[S],
	pos: SourcePos
) {

	private[corama] val vars = new VarRegistry()

	val input: Var[S] = vars.add[S](source.name, tag, pos) // mint a generated var name for elements in the source

	def define[A](tag: Tag[A], pos: SourcePos, hint: String = "var"): Var[A] = vars.add[A](hint, tag, pos)

	override def toString: String = s"TopologyBuilder($source)"
}

/** The topologies of one module, and the declarations they share.
  *
  * PState names, depot names and query names are enforced to be unique within this module.
  */

final class Topologies {
	private [corama] var ids = Set.empty[Identity]
	private var built = List.empty[Topology[?, ?]]

	private var builtPstates = List.empty[PState]

	def topologies: List[Topology[?, ?]] = built.reverse

	def pstates: List[PState] = builtPstates.reverse

	private[corama] def declared(t: Topology[?, ?]): Boolean = built.exists(_ eq t)

	private def claim(identity: Identity): Identity = {
		if (ids.contains(identity)) {
			throw new RuntimeException(s"Multiple definition of $identity")
		}
		ids = ids.incl(identity)
		identity
	}
	
	private def registerPstate[A](value: A, access: A => PState): A = {
		builtPstates = access(value) :: builtPstates
		value
	}

	def pstateKV[K: Tag, V: Tag](name: String): PStateKV[K, V] =
		registerPstate(PStateKV[K, V](claim(Identity(IdentityNamespace.PState, name))), _.inner)

	def pstateKVSet[K: Tag, V: Tag : Ordering](name: String, subindex: Subindex = Subindex.default): PStateKVSet[K, V] =
		registerPstate(PStateKVSet[K, V](claim(Identity(IdentityNamespace.PState, name)), subindex), _.inner)

	def pstate(name: String, schema: Schema): PState =
		registerPstate(PState(claim(Identity(IdentityNamespace.PState, name)), schema), identity)

	def depot[A](name: String, @unused /* TODO */ partitionBy: Any = ()): Depot[A] =
		Depot[A](claim(Identity(IdentityNamespace.Depot, name)))

	/** Declare a query topology: invoked by a client, returns one aggregated value.
	  */
	def query[S, R](name: String, agg: Agg[R])(
		block: Var[S] => Flow[R, QueryContext],
	)(implicit pos: SourcePos, tagS: Tag[S], tagR: Tag[R]): QueryTopology[S, R] = {
		QueryTopology(declare[S, R, QueryContext](claim(Identity(IdentityNamespace.Query, name)), agg, tagS, pos)(block))
	}

	/** A stream topology: driven by an append, writes PStates, returns nothing.
	  *
	  * The context provided is `StreamContext[Nothing]`, making `Flow.ackReturn` impossible.
	  */
	def stream_[A](depot: Depot[A])(
		block: Var[A] => Flow[Unit, StreamContext[Nothing]]
	)(implicit pos: SourcePos, tagA: Tag[A]): StreamTopology[A, Unit] = {
		StreamTopology(declare[A, Unit, StreamContext[Nothing]](depot.id, Agg.unit, tagA, pos)(block))
	}

	// A stream topology returning `R` aggregated values
	def stream[S, R](depot: Depot[S], agg: Agg[R])(
		block: Var[S] => Flow[R, StreamContext[R]]
	)(implicit pos: SourcePos, tagS: Tag[S]): StreamTopology[S, R] = {
		StreamTopology(declare[S, R, StreamContext[R]](depot.id, agg, tagS, pos)(block))
	}

	private def declare[S, R, C](source: Identity, agg: Agg[R], tagS: Tag[S], pos: SourcePos)(
		block: Var[S] => Flow[R, C]
	): Topology[S, R] = {
		val b = new TopologyBuilder[S](TopologyId(built.length), source, tagS, pos)
		val t = Topology[S, R](b, block(b.input), agg)
		built = t :: built
		t
	}
}

object Topologies {
	def apply(): Topologies = new Topologies()
}

/** A declared topology, and how it produces its result.
	* S: source type (input variable)
	* R: aggregated return type
	*/
final case class Topology[S, R](
	builder: TopologyBuilder[S],
	flow: Flow[R, Nothing],
	agg: Agg[R]) {
	def input: Var[S] = builder.input

	// only used in rama backend
	val queryOutputVar: Var[R] = builder.vars.add("queryOutput", null /* TODO */, implicitly[SourcePos])

	val compiled = Node.compile(builder, flow)
}

// wrappers for the different types of topology
final case class QueryTopology[S, R] private[corama](topology: Topology[S, R])

final case class StreamTopology[S, R] private[corama](topology: Topology[S, R])


sealed trait IdentityNamespace
object IdentityNamespace {
	case object PState extends IdentityNamespace
	case object Depot extends IdentityNamespace
	case object Query extends IdentityNamespace
}

// Shared trait for PState, Depot and Query which all
// have publically referenceable names
private case class Identity(namespace: IdentityNamespace, name: String)
