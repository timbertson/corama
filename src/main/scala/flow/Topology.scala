package flow

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid
import izumi.reflect.Tag

import flow.internal._

final case class TopologyId(value: Int) extends AnyVal

enum TopologyKind {
	// TODO: add MicroBatch
	case Stream, Query
}

// S represents the source type, e.g. the type of elements from a depot
// or query input
final class TopologyBuilder[S] private[flow] (
	val id: TopologyId,
	// source may be a Depot or Query, it won't be a PState
	val source: Identity,
	tag: Tag[S],
	pos: SourcePos
) {

	private[flow] val vars = new VarRegistry()

	val input: Var[S] = vars.add[S](source.name, tag, pos) // mint a generated var name for elements in the source

	def define[A](tag: Tag[A], pos: SourcePos, hint: String = "var"): Var[A] = vars.add[A](hint, tag, pos)

	override def toString: String = s"TopologyBuilder($source)"
}

/** The topologies of one module, and the declarations they share.
  *
  * PState names, depot names and query names are enforced to be unique within this module.
  */

final class Topologies {
	private [flow] var ids = Set.empty[Identity]
	private var built = List.empty[Topology[?, ?, ?]]

	// private var pstates: List[Pstate]

	def topologies: List[Topology[?, ?, ?]] = built.reverse

	private[flow] def declared(t: Topology[?, ?, ?]): Boolean = built.exists(_ eq t)

	private def claim(identity: Identity): Identity = {
		if (ids.contains(identity)) {
			throw new RuntimeException(s"Multiple definition of $identity")
		}
		ids = ids.incl(identity)
		identity
	}

	def pstateKV[K: Tag, V: Tag](name: String): PStateKV[K, V] =
		PStateKV[K, V](claim(Identity(IdentityNamespace.PState, name)))

	def pstateKVSet[K: Tag, V: Tag : Ordering](name: String, subindex: Subindex = Subindex.default): PStateKVSet[K, V] =
		PStateKVSet[K, V](claim(Identity(IdentityNamespace.PState, name)), subindex)

	def pstate(name: String, schema: Schema): PState =
		PState(claim(Identity(IdentityNamespace.PState, name)), schema)

	def depot[A](name: String, @unused /* TODO */ partitionBy: Any = ()): Depot[A] =
		Depot[A](claim(Identity(IdentityNamespace.Depot, name)))

	/** Declare a query topology: invoked by a client, returns exactly one value.
	  */
	def query[S, A, R](name: String, agg: Aggregator[A, R])(
		block: Var[S] => Flow[A, QueryContext],
	)(implicit pos: SourcePos, tag: Tag[S]): QueryTopology[S, A, R] = {
		QueryTopology(declare[S, A, R, QueryContext](claim(Identity(IdentityNamespace.Query, name)), agg, tag, pos)(block))
	}

	// Overload accepting a simple combiner
	def query[Input, R](name: String, agg: Combiner[R])(
		block: Var[Input] => Flow[R, QueryContext],
	)(implicit pos: SourcePos, tag: Tag[Input]): QueryTopology[Input, R, R] = {
		QueryTopology(declare[Input, R, R, QueryContext](claim(Identity(IdentityNamespace.Query, name)), agg, tag, pos)(block))
	}

	/** A stream topology: driven by an append, writes PStates, returns nothing.
	  *
	  * The context provided is `StreamContext[Nothing]`, making `Flow.ackReturn` impossible.
	  */
	def stream_[A](depot: Depot[A])(
		block: Var[A] => Flow[Unit, StreamContext[Nothing]]
	)(implicit pos: SourcePos, tag: Tag[A]): Topology[A, Unit, Unit] = {
		declare[A, Unit, Unit, StreamContext[Nothing]](depot.id, Agg.unit, tag, pos)(block)
	}

	// A stream topology returning `A` values, aggregated into an `R` value.
	def stream[S, A, R](depot: Depot[S], agg: Agg[A, R])(
		block: Var[S] => Flow[A, StreamContext[A]]
	)(implicit pos: SourcePos, tag: Tag[S]): StreamTopology[S, A, R] = {
		StreamTopology(declare[S, A, R, StreamContext[A]](depot.id, agg, tag, pos) { input =>
			block(input)
		})
	}

	private def declare[S, A, R, C](source: Identity, agg: Agg[A, R], tag: Tag[S], pos: SourcePos)(
		block: Var[S] => Flow[A, C]
	): Topology[S, A, R] = {
		val b = new TopologyBuilder[S](TopologyId(built.length), source, tag, pos)
		val t = Topology[S, A, R](b, block(b.input), agg)
		built = t :: built
		t
	}
}

object Topologies {
	def apply(): Topologies = new Topologies()
}

/** A declared topology, and how it produces its result.
	* S: source type (input variable)
	* A: accumulation type (the result of the flow)
	* R: final post-aggregation return type; often the same as A
	*/
final case class Topology[S, A, R](
	builder: TopologyBuilder[S],
	flow: Flow[A, Nothing],
	agg: Agg[A, R]) {
	def input: Var[S] = builder.input
	val compiled = Node.compile(builder, flow)
}

// wrappers for the different types of topology
final case class QueryTopology[S, A, R] private[flow](topology: Topology[S, A, R])

final case class StreamTopology[S, A, R] private[flow](topology: Topology[S, A, R])


sealed trait IdentityNamespace
object IdentityNamespace {
	case object PState extends IdentityNamespace
	case object Depot extends IdentityNamespace
	case object Query extends IdentityNamespace
}

// Shared trait for PState, Depot and Query which all
// have publically referenceable names
private case class Identity(namespace: IdentityNamespace, name: String)
