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
  */
final class AckReturns private[corama] (
	private val values: Map[TopologyId, Any],
	private val topologies: Topologies) {

	def result[S, R](stream: StreamTopology[S, R]): R = {
		val t = stream.topology
		require(
			topologies.declared(t),
			s"${t.builder.source} was not declared in the module this append went to")

		values.get(t.builder.id).map(_.asInstanceOf[R]).getOrElse(
			throw new RuntimeException(s"no result recorded for topology ${t.id}")
		)
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

object AckReturns {
	def codec(topologies: Topologies): Codec[AckReturns] = {
		Codec.tryVia[AckReturns, List[(Int, Bytes)]] { ackReturns =>
			ackReturns.values.toList.traverse { (topologyId, value) =>
				topologies.get(topologyId).map { t =>
					(topologyId.value, t.codec.asAny.encode(value))
				}
			}.get // TODO unsafe
		} { list =>
			list.traverse { (key, bytes) =>
				val topologyId = TopologyId(key)
				topologies.get(topologyId).flatMap { t =>
					t.codec.asAny.decode(bytes).map(topologyId -> _)
				}
			}.map(list => AckReturns(list.toMap, topologies))
		}
	}
}

