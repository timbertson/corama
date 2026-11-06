package corama.interpreter

import scala.collection.mutable
import cats.syntax.all.*
import corama.*
import corama.interpreter.*

import scala.collection.mutable.ArrayBuffer
import cats.effect.SyncIO
import corama.internal.NodeId

// A mutable snapshot collecting updates (and serving a view), used for a single eval batch
// Unsafe (mutation), must not be accessed concurrently
// TODO: should this use Ref[SyncIO] for safety, or are the mutable collections more efficient?
final class EvalState(base: PStateView) extends PStateView {
	val outputs = new EvalState.Outputs()
	private val depotAppends = new mutable.HashMap[DepotId, mutable.ArrayBuffer[Partitioned[DepotAppend]]]()
	private val pstateUpdates = new mutable.HashMap[PStateKey, Option[Bytes]]()

	def append(depot: DepotId, partition: PartitionId, value: Bytes): SyncIO[Unit] = {
		SyncIO(depotAppends.getOrElseUpdate(depot, new ArrayBuffer())
			.append(Partitioned(partition, DepotAppend(value)))).void
	}

	def put(key: PStateKey, value: Option[Bytes]): SyncIO[Unit] = {
		SyncIO(pstateUpdates.put(key, value)).void
	}
	

	override def get(key: PStateKey) =
		pstateUpdates.get(key).fold(base.get(key))(SyncIO.pure)

	override def select(key: PStateKey, opts: SelectOptions.Raw): SyncIO[Vector[Bytes]] = SyncIO {
		???
	}

	def toUpdate(txId: TxId): ReplicationUpdate = ReplicationUpdate(
		txId,
		depotAppends = depotAppends.toList.map { (id, partitioned) =>
			DepotAppends(id, partitioned.toArray)
		},
		pstateUpdates = pstateUpdates.map { (k, v) => PStateUpdate(k, v) }.toArray
	)
}

object EvalState {
	def apply(base: PStateView, speculative: SpeculativeState): SyncIO[EvalState] = {
		speculative.snapshot.map(new EvalState(_))
	}

	// effectful outputs which are handled after the data changes are persisted
	class Outputs {
		private val notificationBuffer = new mutable.HashMap[Origin, EvalNotification]()
		private val continuationBuffer = new mutable.ArrayBuffer[EvalContinuation]()
		private val evalSinkBuffer = new mutable.ArrayBuffer[LocalEvalSink[?]]()

		def notifications: Seq[(Origin, EvalNotification)] = notificationBuffer.toSeq

		def addNotification(origin: Origin, edge: EdgeId, value: List[Any], agg: Agg[?]): SyncIO[Unit] = SyncIO {
			val updated = notificationBuffer.get(origin) match {
				case None => EvalNotification(edge, value.foldLeft(agg.zero)(agg.combineAny), agg)

				// agg and codec must be equivalent for given origin, so we can ignore them
				case Some(current) => current.merge(edge, value)
			}
			notificationBuffer.put(origin, updated)
		}.void

		// when a computation is not constrained to this topology, evaluation
		// registers an eval sink. This will be exposed to the local GRPC interface,
		// and results from other nodes will be routed to it.
		def addEvalSink(sink: LocalEvalSink[?]): SyncIO[Unit] = ???

		def evalSinks: Seq[LocalEvalSink[?]] = evalSinkBuffer.toSeq

		def addContinuation(cont: EvalContinuation): SyncIO[Unit] = SyncIO {
			continuationBuffer.append(cont)
		}.void

		def continuations: Seq[EvalContinuation] = continuationBuffer.toSeq
	}
}


case class EvalNotification(edge: EdgeId, value: Any, agg: Agg[?]) {
	def merge(newEdge: EdgeId, newValue: List[Any]) = {
		copy(edge = edge.xor(newEdge), value = newValue.foldLeft(value)(agg.combineAny))
	}
}

case class EvalContinuationTask(taskId: TaskId, edge: EdgeId, batch: PartitionBatch[Env])

case class EvalContinuation(
	origin: Origin,
	topologyId: TopologyId,
	afterNode: NodeId,
	tasks: List[EvalContinuationTask])
