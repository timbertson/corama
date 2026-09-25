package corama

import cats.syntax.all.*
import cats.effect.std.Random
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import flogger.Log
import cats.effect.IO
import cats.effect.Ref
import cats.effect.Deferred
import flogger.IOLogging

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

case class EdgeId(id: Long) {
	// A single ID with parent & ID XOR'd can close the parent edge and atomically spawn a child edge
	def xor(other: EdgeId) = EdgeId(id ^ other.id)
}

object EdgeId {
	val empty = EdgeId(0L)

	def generate(using rnd: Random[IO]): IO[EdgeId] = {
		rnd.nextLong.map(apply)
	}
	
	def union(ids: List[EdgeId]) = ids.foldLeft(empty)(_.xor(_))
}

case class Notification[R](edge: EdgeId, result: Option[R])

trait TopologyResults[R] {
	val varId: VarId

	def notify(edge: EdgeId, value: Option[R]): IO[Unit]

	def completeList(edge: EdgeId, values: List[R]): IO[Unit]
}

/* A tracker for a single topology & execution. Performs aggregation, and tracks the event tree.
 * The tree is extended by xoring a (randomly generated) edge ID, and is complete when every edge extension
 * is canceled out (i.e. the state returns to zero).
 *
 * This technique is based on https://storm.apache.org/releases/2.6.0/Acking-framework-implementation.html
 */
private[corama] final class LocalTopologyResults[R](
	val varId: VarId,
	agg: Agg[R],
	log: Log[IO],
	xorState: Ref[IO, Long],
	acc: Ref[IO, R],
	done: Deferred[IO, Try[R]],
) extends TopologyResults[R] with IOLogging {

	// NOTE: this can open a new edge, close an edge, or do _both_
	// (by xoring an open edge with a new edge, the open one is closed
	// and the new one is opened atomically)
	def notify(edge: EdgeId, value: Option[R]) = {
		for {
			_ <- value.traverse_(value => acc.updateAndGet(x => agg.combine(x, value)))
			xorValue <- xorState.updateAndGet(x => x ^ edge.id)
			_ <- IO.whenA(xorValue === 0L) {
				acc.get.flatMap(x => done.complete(Success(x))).flatMap { wasFirst =>
					IO.whenA(!wasFirst) {
						log.warn(s"results returned more than once")
					}
				}
			}
		} yield ()
	}

	def completeList(edge: EdgeId, values: List[R]) = notify(edge, Option.when(!values.isEmpty)(agg.merge(values)))

	def fail(t: Throwable): IO[Unit] = done.complete(Failure(t)).void
	
	def await: IO[R] = done.get.flatMap(IO.fromTry)
}

private [corama] object TopologyResults {
	def local[R](out: VarId, agg: Agg[R], start: EdgeId)(using log: Log[IO]): IO[LocalTopologyResults[R]] = {
		for {
			xorState <- Ref[IO].of(start.id)
			acc <- Ref[IO].of(agg.zero)
			done <- Deferred[IO, Try[R]]
		} yield new LocalTopologyResults(out, agg, log, xorState, acc, done)
	}
}
