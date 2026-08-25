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
	private var built = List.empty[Topology[?, ?]]

	// private var pstates: List[Pstate]

	def topologies: List[Topology[?, ?]] = built.reverse

	private[flow] def declared(t: Topology[?, ?]): Boolean = built.exists(_ eq t)

	private def claim(identity: Identity): Identity = {
		if (ids.contains(identity)) {
			throw new RuntimeException(s"Multiple definition of $identity")
		}
		ids = ids.incl(identity)
		identity
	}

	def pstateKV[K: Tag, V: Tag](name: String): PStateKV[K, V] =
		PStateKV[K, V](claim(Identity(IdentityNamespace.PState, name)))

	def pstateKVSet[K: Tag, V: Tag](name: String, subindex: Subindex = Subindex.default): PStateKVSet[K, V] =
		PStateKVSet[K, V](claim(Identity(IdentityNamespace.PState, name)), subindex)

	def pstate(name: String, schema: Schema): PState =
		PState(claim(Identity(IdentityNamespace.PState, name)), schema)

	def depot[A](name: String, @unused /* TODO */ partitionBy: Any = ()): Depot[A] =
		Depot[A](claim(Identity(IdentityNamespace.Depot, name)))

	/** Declare a query topology: invoked by a client, returns exactly one value.
	  */
	def query[Input, Output](name: String, agg: Agg[Output])(
		block: Var[Input] => Flow[Output, QueryContext],
	)(implicit pos: SourcePos, tag: Tag[Input]): QueryTopology[Input, Output] = {
		QueryTopology(declare[Input, Output, QueryContext](claim(Identity(IdentityNamespace.Query, name)), agg, tag, pos)(block))
	}

	/** Attach a stream topology to a depot: driven by an append, writes PStates,
	  * returns nothing.
	  *
	  * The context provided is `StreamContext[Nothing]`, so [[Flow.ackReturn]] does not
	  * compile here — there is no declared aggregator for a value to reach.
	  *
		* TODO: does the block need to accept TopologyBuilder, or is Flow[A] / Var[A] enough?
	  */
	def stream_[A](depot: Depot[A])(
		block: Var[A] => Flow[Unit, StreamContext[Nothing]]
	)(implicit pos: SourcePos, tag: Tag[A]): Topology[A, Unit] = {
		declare[A, Unit, StreamContext[Nothing]](depot.id, Agg.unit, tag, pos)(block)
	}

	/** Attach a stream topology that hands a value back to whoever appended.
	  *
	  * The aggregator determines the topology's `R`: a flow emits 0..n ack returns across
	  * however many partitions its branches reached, and [[AckAgg]] says how those become
	  * the one value an `AckLevel.Ack` append reads out of [[AckReturns]]. Rama declares
	  * it the same way, as `StreamSourceOptions.ackReturnAgg`.
	  */
	def stream[A, O](depot: Depot[A], agg: Agg[O])(
		block: Var[A] => Flow[O, StreamContext[O]]
	)(implicit pos: SourcePos, tag: Tag[A]): StreamTopology[A, O] = {
		StreamTopology(declare[A, O, StreamContext[O]](depot.id, agg, tag, pos) { input =>
			block(input).flatMap(Flow.ackReturn)
		})
	}

	private def declare[A, R, C](source: Identity, agg: Agg[R], tag: Tag[A], pos: SourcePos)(
		block: Var[A] => Flow[?, C]
	): Topology[A, R] = {
		val b = new TopologyBuilder[A](TopologyId(built.length), source, tag, pos)
		val t = Topology[A, R](b, block(b.input), agg)
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


sealed trait IdentityNamespace
object IdentityNamespace {
	case object PState extends IdentityNamespace
	case object Depot extends IdentityNamespace
	case object Query extends IdentityNamespace
}

// Shared trait for PState, Depot and Query which all
// have publically referenceable names
private case class Identity(namespace: IdentityNamespace, name: String)
