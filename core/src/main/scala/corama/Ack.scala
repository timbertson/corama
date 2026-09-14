package corama

import cats.syntax.all.*

import scala.util.Failure
import scala.util.Success
import scala.util.Try

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
final class AckReturns private[corama] (
	private val values: Map[TopologyId, List[Any]],
	private val topologies: Topologies) {

	// Results are stored as type A, the aggregation. `mapOutput` is performed when the result is returned.
	def result[S, R](stream: StreamTopology[S, R]): R = {
		val t = stream.topology
		require(
			topologies.declared(t),
			s"${t.builder.source} was not declared in the module this append went to")

		val agg: Agg[R] = t.agg
		values.getOrElse(t.builder.id, Nil).foldLeft(agg.zero)(agg.combineAny)
	}

	def unique[S, T, M <: Seq[T]](stream: StreamTopology[S, M]): Try[T] = {
		val seq = result[S, M](stream)
		if (seq.length == 1) {
			Success(seq.head)
		} else {
			Failure(new RuntimeException(s"Expected ${stream.topology.builder.source} to produce 1 element, but received ${seq.length}"))
		}
	}

	override def toString: String = s"AckReturns(${values.size})"
}
