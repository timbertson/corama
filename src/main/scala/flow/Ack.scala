package flow

import cats.{Monoid, Semigroup}
import cats.syntax.all._

sealed trait AckLevel[R]

object AckLevel {

	// Fire and forget, data has been accepted but not durably (not yet replicated)
	case object NoAck extends AckLevel[Unit]

	// Return once the data has been replicated
	case object AppendAck extends AckLevel[Unit]

	// Wait for all associated topologies to return
	case object Ack extends AckLevel[AckReturns]
}

/** How a stream topology's ack returns are combined.
  *
  * A stream flow emits 0..n values — one per branch, on whichever partition the branch
  * ended up on — each is a separate `ackReturn`.
	* The aggregator defines how those values are returned to the appender.
	*
	* // TODO is `prepare` necessary?
  */
final class AckAgg[O, R] private[flow] (
	private[flow] val prepare: O => R,
	private[flow] val monoid: Monoid[Option[R]]) {

	private[flow] def combine(values: List[O]): Option[R] =
		monoid.combineAll(values.map(o => Some(prepare(o))))

	/** Erased forms, for the interpreter and the Rama backend — an ack-returned value is
	  * an `Any` by the time either sees it.
	  *
	  * `null` stands for the zero on the Rama side, since `initVal` has to produce a
	  * concrete `Object`
	  */
	private[flow] def combineAny(values: List[Any]): Option[R] = combine(values.asInstanceOf[List[O]])

	private[flow] def initAny: Any = null

	private[flow] def accumulateAny(acc: Any, value: Any): Any =
		monoid.combine(
			Option(acc).asInstanceOf[Option[R]],
			Some(prepare(value.asInstanceOf[O]))
		) match {
			case Some(r) => r
			case None => null
		}
}

object AckAgg {

	/** Map each ack return into the result type, then combine. */
	def reduceMap[O, R](f: O => R)(using Semigroup[R]): AckAgg[O, R] =
		new AckAgg(f, Monoid[Option[R]])

	/** Combine ack returns using their own type's `Semigroup`.
	  *
	  * So `AckAgg.reduce[Int]` sums, which is the ack-return example Rama's own docs use,
	  * and it arrives for free rather than as another case here.
	  */
	def reduce[R](using Semigroup[R]): AckAgg[R, R] = reduceMap(identity)

	/** Every ack return, in no specific order */
	def all[O]: AckAgg[O, List[O]] = reduceMap(List(_))
}

/** The ack returns from one depot append, keyed by the topology that produced each.
  *
  * Values arrive as a list per topology — one entry per branch that reached `ackReturn` —
  * and are reduced by that topology's own [[AckAgg]] on the way out.
  */
final class AckReturns private[flow] (
	private val values: Map[TopologyId, List[Any]],
	private val topologies: Topologies) {

	// TODO: assert topology is a Stream, not a Microbatch
	def raw[S, R](t: Topology[S, R]): Option[R] = {
		require(
			t.builder.source.namespace == IdentityNamespace.Depot,
			s"${t.builder.source} is not a stream topology, so it cannot produce an ack return")
		require(
			topologies.declared(t),
			s"${t.builder.source} was not declared in the module this append went to")

		val raw = values.getOrElse(t.builder.id, Nil)
		t.ackAgg match {
			case Some(agg) => agg.combineAny(raw)

			// No declared aggregator, so `R` is `Unit` and the flow had no way to call
			// `ackReturn` — nothing to hand back, and nothing was collected.
			case None => None.asInstanceOf[Option[R]]
		}
	}

	// TODO: def unique() which returns Try[O]. Fails if there was no response, or more than one response.

	override def toString: String = s"AckReturns(${values.size})"
}
