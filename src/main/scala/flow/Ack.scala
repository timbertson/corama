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

/** The ack returns from one depot append, keyed by the topology that produced each.
  *
  * Values arrive as a list per topology. TODO: these could be pre-combined with agg?
  */
final class AckReturns private[flow] (
	private val values: Map[TopologyId, List[Any]],
	private val topologies: Topologies) {

	// Results are stored as type A, the aggregation. `mapOutput` is only performed when the
	// result is actually returned.
	def result[S, A, R](stream: StreamTopology[S, A, R]): R = {
		val t = stream.topology
		require(
			topologies.declared(t),
			s"${t.builder.source} was not declared in the module this append went to")

		val aggregator: Aggregator[A, R] = Agg.asAggregator(t.agg)
		val combiner: Combiner[A] = aggregator.combiner
		val aggregated: A = values.getOrElse(t.builder.id, Nil).foldLeft(combiner.zero)(combiner.combineAny)
		aggregator.mapOut(aggregated)
	}

	def unique[S, A, T, M <: Seq[T]](stream: StreamTopology[S, A, M]): Try[T] = {
		val seq = result[S, A, M](stream)
		if (seq.length == 1) {
			Success(seq.head)
		} else {
			Failure(new RuntimeException(s"Expected ${stream.topology.builder.source} to produce 1 element, but received ${seq.length}"))
		}
	}

	override def toString: String = s"AckReturns(${values.size})"
}
