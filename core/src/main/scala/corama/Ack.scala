package corama

import cats.syntax.all.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import corama.error.Util

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

	def result[S, R](stream: StreamTopology[S, R]): Try[R] = {
		val t = stream.topology
		for {
			_ <- if (topologies.declared(t)) {
				Success(())
			} else {
				Failure(Util.err(s"${t.builder.source} was not declared in this depot's topologies"))
			}
			any <- values.get(t.builder.id)
				.toRight(Util.err(s"no result recorded for topology ${t.id}")).toTry
			value <- Try(any.asInstanceOf[R])
		} yield value
	}
	
	def unique[S, T, M <: Seq[T]](stream: StreamTopology[S, M]): Try[T] = {
		result[S, M](stream).flatMap(extractOne(stream))
	}

	private def extractOne[A](stream: StreamTopology[?, ?])(seq: Seq[A]): Try[A] = {
			if (seq.length == 1) {
				Success(seq.head)
			} else {
				Failure(new RuntimeException(s"Expected ${stream.topology.builder.source} to produce 1 element, but received ${seq.length}"))
			}
	}

	def uniqueOpt[S, T, M <: Seq[T]](stream: StreamTopology[S, M]): Try[Option[T]] = {
		result[S, M](stream).flatMap { seq =>
			if (seq.isEmpty) {
				Success(None)
			} else {
				extractOne(stream)(seq).map(Some.apply)
			}
		}
	}

	override def toString: String = s"AckReturns(${values.size})"
}

object AckReturns {
	// TODO: should there be a SerializedAckReturns type so that consumers
	// don't have to deserialize every topology if they're only interested in one?
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

