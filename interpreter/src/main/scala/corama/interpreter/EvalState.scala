package corama.interpreter

import scala.collection.mutable
import cats.syntax.all.*
import corama.*
import corama.interpreter.*

import scala.collection.mutable.ArrayBuffer

// A mutable snapshot collecting updates (and serving a view), used for a single eval batch
// Unsafe (mutation), must not be accessed concurrently
// TODO: decide whether this is IO or not!
final class EvalState(base: PStateView) extends PStateView {
	private val depotAppends = new mutable.HashMap[DepotId, mutable.ArrayBuffer[Bytes]]()
	private val pstateUpdates = new mutable.HashMap[PStateKey, Option[Bytes]]()

	// TODO: trigger async notifications after eval is complete
	private val notificationBuffer = new mutable.HashMap[Origin, EvalNotification]()

	def append(depot: DepotId, value: Bytes): Unit = {
		val _ = depotAppends.getOrElseUpdate(depot, new ArrayBuffer()).append(value)
	}

	def put(key: PStateKey, value: Option[Bytes]): Unit = {
		val _ = pstateUpdates.put(key, value)
	}
	
	def notifications: Seq[(Origin, EvalNotification)] = notificationBuffer.toSeq

	def notify(origin: Origin, edge: EdgeId, value: Option[Any], agg: Agg[?], codec: AnyCodec) = {
		val updated = notificationBuffer.get(origin) match {
			case None => EvalNotification(edge, value.getOrElse(agg.zero), agg, codec)

			// agg and codec must be equivalent for given origin, so we can ignore them
			case Some(current) => current.merge(edge, value)
		}
		notificationBuffer.put(origin, updated)
	}

	override def get(key: PStateKey): Option[Bytes] = {
		pstateUpdates.get(key).getOrElse {
			base.get(key)
		}
	}

	override def select(key: PStateKey, opts: SelectOptions.Raw): Vector[Bytes] = {
		???
	}

	def toUpdate(txId: TxId): ReplicationUpdate = ReplicationUpdate(
		txId,
		depotAppends = depotAppends.toList.map { (id, buf) => DepotAppends(id, buf.toArray.map(DepotAppend.apply)) },
		pstateUpdates = pstateUpdates.map { (k, v) => PStateUpdate(k, v) }.toArray
	)
}

object EvalState {
	def apply(base: PStateView, speculative: SpeculativeState) = {
		speculative.snapshot.map(new EvalState(_))
	}
}


case class EvalNotification(edge: EdgeId, value: Any, agg: Agg[?], codec: AnyCodec) {
	def merge(newEdge: EdgeId, newValue: Option[Any]) = {
		copy(edge = edge.xor(newEdge), value = newValue.fold(value)(agg.combineAny(value, _)))
	}
}
