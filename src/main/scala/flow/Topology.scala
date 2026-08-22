package flow

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid

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
	val source: Identity
	)(implicit pos: SourcePos) {

	private[flow] val vars = new VarRegistry()

	val input: Var[S] = vars.add[S](pos, source.name) // mint a generated var name for elements in the source

	val output: Var[S] = vars.add[S](pos, "out") // mint a generated var name for output. Only used for queries

	def define[A](pos: SourcePos, hint: String = "var"): Var[A] = vars.add[A](pos, hint)

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
		block: Var[Input] => Flow[Output, QueryContext],
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
	def stream_[A](depot: Depot[A])(
		block: Var[A] => Flow[Unit, StreamContext[Nothing]]
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
		block: Var[A] => Flow[O, StreamContext[O]]
	): StreamTopology[A, O] = {
		StreamTopology(declare[A, O, StreamContext[O]](depot.id, agg) { input =>
			block(input).flatMap(Flow.ackReturn)
		})
	}

	private def declare[A, R, C](source: Identity, agg: Agg[R])(
		block: Var[A] => Flow[?, C]
	): Topology[A, R] = {
		val b = new TopologyBuilder[A](TopologyId(built.length), source)
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

final case class PState[K, V](id: Identity) {
	def get(key: Var[K]): Flow[Option[V], Any] = Flow.Select(this, key)

	def getOrElse(key: Var[K], dfl: V): Flow[V, Any] = Flow.Select(this, key).flatMap(v => v.map(_.getOrElse(dfl)))

	def update(key: Var[K]): PState.Update[K, V] = new PState.Update(this, key)

	def put(key: Var[K], value: Var[V]): Flow[Unit, Etl] = Flow.Put(this, key, value)
	def delete(key: Var[K])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Update(pos, this, key, _ => None)
}

object PState {
	class Update[K, V](pstate: PState[K,V], key: Var[K]) {
		def apply(f: Option[V] => Option[V])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Update(pos, pstate, key, f)
		def zip[A](arg: Var[A])(f: (Option[V], A) => Option[V])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.UpdateWith(pos, pstate, key, arg, f)
	}
}

final case class Depot[A](id: Identity) {
	def localAppend(value: Var[A]): Flow[Unit, Etl] = Flow.PartitionAppend(this, value)
}
