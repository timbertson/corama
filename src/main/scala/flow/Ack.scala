package flow

import cats.{Monoid, Semigroup}
import cats.syntax.all._
import cats.Functor
import cats.Applicative
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import cats.data.NonEmptyList

sealed trait AckLevel[R]

object AckLevel {

	// Fire and forget, data has been accepted but not durably (not yet replicated)
	case object NoAck extends AckLevel[Unit]

	// Return once the data has been replicated
	case object AppendAck extends AckLevel[Unit]

	// Wait for all associated topologies to return
	case object Ack extends AckLevel[AckReturns]
}

/** How return values are combined. This is used for both
 * streaming ack returns, and also for query returns.
 *
 * `prepare` always happens locally.
 * `combine` happens locally to combine multiple local results,
 * and again when combining results from multiple partitions.
 */
final class AckAgg[O, R] private[flow] (
	private[flow] val map: O => R,
	// private[flow] val zero: R,
	private[flow] val combine: (R, R) => R
) {

	private[flow] def merge(values: NonEmptyList[O]): R = values.map(map).reduceLeft(combine)

	private[flow] def mergeAny(values: List[Any]): Option[R] =
		NonEmptyList.fromList(values.asInstanceOf[List[O]]).map(merge)

	private[flow] def mapAny(value: Any): R = map(value.asInstanceOf[O])
	private[flow] def mapCombineAny(acc: Any, value: Any): R = combine(acc.asInstanceOf[R], mapAny(value))
}

object AckAgg {
	def apply[O, R](map: O => R, combine: (R, R) => R): AckAgg[O, R] = new AckAgg(map, combine)

	// Collect act returns into a sequence (monoid)
	def collect[A, M[_]](using app: Applicative[M], m: Monoid[M[A]]): AckAgg[A, M[A]] = {
		new AckAgg[A, M[A]](app.pure, m.combine)
	}

	// convenience overload of collect to hardcode the List type
	def list[O]: AckAgg[O, List[O]] = collect

	// return nothing, accumulate nothing
	def unit: AckAgg[Unit, Unit] = apply(_ => (), (_, _) => ())
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
	def result[S, R](t: Topology[S, R]): Option[R] = {
		// TODO: make this impossible via types?
		require(
			t.builder.source.namespace == IdentityNamespace.Depot,
			s"${t.builder.source} is not a stream topology, so it cannot produce an ack return")
		require(
			topologies.declared(t),
			s"${t.builder.source} was not declared in the module this append went to")

		val raw = values.getOrElse(t.builder.id, Nil)
		t.ackAgg.mergeAny(raw)
	}

	// if the result is already a monoid, we can collapse the Option into empty
	def results[S, R : Monoid](t: Topology[S, R]): R = {
		result(t).getOrElse(Monoid[R].empty)
	}

	def unique[S, T, R <: Seq[T]](t: Topology[S, R]): Try[T] = {
		result(t) match {
			case Some(seq) if seq.length == 1 => Success(seq.head)
			case other => Failure(new RuntimeException(s"Expected ${t.builder.source} to produce 1 element, but received ${other.map(_.length).getOrElse(0)}"))
		}
	}

	override def toString: String = s"AckReturns(${values.size})"
}
