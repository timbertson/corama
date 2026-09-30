package corama.interpreter

import cats.effect.std.Random
import cats.syntax.all.*
import cats.effect.IO
import cats.effect.kernel.Ref
import corama.*
import corama.error.Util.*
import org.tpolecat.sourcepos.SourcePos

import scala.util.*
import scala.util.control.NoStackTrace
import flogger.*
import cats.effect.Deferred
import corama.internal.HexString
import corama.internal.HexId


// EvalId is dynamically minted each time we perform a fanout
case class EvalId(id: Long) extends HexId {
	override def toString(): String = s"EvalId($hex)"
}

object EvalId {
	def generate(using rand: Random[IO]): IO[EvalId] = rand.nextLong.map(EvalId.apply)
}

case class PartitionBatch[A](value: List[(PartitionId, List[A])])

case class Origin(
	instanceId: InstanceId,
	evalId: EvalId, // unique key for this specific append / query root
)

class EvalError(msg: String, pos: SourcePos, cause: Throwable) extends RuntimeException(s"$msg defined at $pos", cause) with NoStackTrace

// Tracker for every topology spawned by a given depot append (or query)
private[corama] final class EvalResults(results: Map[TopologyId, LocalEvalSink[?]])(using log: Log[IO]) extends IOLogging {
	def forTopology[R](topology: Topology[?, R]): Try[LocalEvalSink[R]] =
		results.get(topology.id)
			.toRight(err(s"TopologyResults(${topology.id}) not found")).toTry
			.map(_.asInstanceOf[LocalEvalSink[R]])

	def await: IO[Map[TopologyId, Any]] = results.toList.traverse { (id, results) =>
		log.debug(s"Awaiting results for append on topology $id") *>
		results.await.map(id -> _).flatMap { r =>
			log.debug(s"Got results from append on topology $id").as(r)
		}
	}.map(_.toMap)
}

private[corama] object EvalResults extends IOLogging {
	def apply(topologies: List[Topology[?, ?]])(using baseLog: Log[IO]): IO[EvalResults] = {
		baseLog.debug(s"Creating a local result sink for ${topologies.map(_.id)}") *>
		(topologies.traverse { topology =>
			given log: Log[IO] = baseLog.addContext("topology" -> topology.id.value.toString())
			EvalSink.local[topology.Result](topology.output.vid, topology.agg, EdgeId.startingEdge(topology.id))
				.map(topology.id -> _)
		}.map { topologyResults =>
			new EvalResults(topologyResults.toMap)
		})
	}
}


case class EdgeId(id: Long) extends HexId {
	// A single ID with parent & ID XOR'd can close the parent edge and atomically spawn a child edge
	def xor(other: EdgeId) = EdgeId(id ^ other.id)
	override def toString(): String = s"EdgeId($hex)"
}

object EdgeId {
	val empty = EdgeId(0L)

	private[corama] def startingEdge(topologyId: TopologyId) =
		// edge IDs must not start at 0
		EdgeId(topologyId.value.toLong + 1)

	def generate(using rnd: Random[IO]): IO[EdgeId] = {
		rnd.nextLong.map(apply)
	}
	
	def union(ids: List[EdgeId]) = ids.foldLeft(empty)(_.xor(_))
}

trait EvalSink[R] {
	val varId: VarId

	def notify(edge: EdgeId, value: Option[R]): IO[Unit]

	def completeList(edge: EdgeId, values: List[R]): IO[Unit]
}

/* The object which lives on the origin instance for a given evaluation.
 * Performs aggregation, and tracks the event tree. The tree is extended by xoring a
 * (randomly generated) edge ID, and is complete when every edge extension
 * is canceled out (i.e. the state returns to zero).
 *
 * This technique is based on https://storm.apache.org/releases/2.6.0/Acking-framework-implementation.html
 */
private[corama] final class LocalEvalSink[R](
	val varId: VarId,
	agg: Agg[R],
	log: Log[IO],
	xorState: Ref[IO, Long],
	acc: Ref[IO, R],
	done: Deferred[IO, Try[R]],
) extends EvalSink[R] with IOLogging {
	// NOTE: this can open a new edge, close an edge, or do _both_
	// (by xoring an open edge with a new edge, the open one is closed
	// and the new one is opened atomically)
	def notify(edge: EdgeId, value: Option[R]) = {
		for {
			_ <- value.traverse_(value => acc.updateAndGet(x => agg.combine(x, value)))
			xorValue <- xorState.updateAndGet(x => x ^ edge.id)
			_ <- log.debug(s"Received notification $edge|${value.fold("None")(_ => "Some")}, state: from ${HexString(xorValue ^ edge.id)} => ${HexString(xorValue)}")
			_ <- IO.whenA(xorValue === 0L) {
				log.debug(s"eval complete") *>
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
	
	def await: IO[R] =
		log.debug("Awaiting...") *>
		done.get.flatMap(r => log.debug(s"Await returning $r").as(r)).flatMap(IO.fromTry)
}

private [corama] object EvalSink {
	def local[R](out: VarId, agg: Agg[R], start: EdgeId)(using log: Log[IO]): IO[LocalEvalSink[R]] = {
		for {
			xorState <- Ref[IO].of(start.id)
			acc <- Ref[IO].of(agg.zero)
			done <- Deferred[IO, Try[R]]
		} yield new LocalEvalSink(out, agg, log, xorState, acc, done)
	}
}
