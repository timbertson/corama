package corama.interpreter

import scala.collection.mutable
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import corama.*
import corama.interpreter.*

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

trait PStateView {
	def get(pstate: PStateId, key: Bytes.Value): Option[Bytes]
}

/** State of speculative view - tracks replication updates that have
 * been submitted but not yet acknowledged.
 *
 * Used to build ViewSnapshot and BatchSnapshots.
 *
 * Keyed by submission order, not log index: the log index only becomes
 * known when the proposal commits, but later executions must see earlier
 * updates immediately. The seq -> index mapping is monotonic only because
 * appends are only performed from the single proposer fiber.
 */
final class SpeculativeState {
	private case class Entry(update: ReplicationUpdate, index: Option[LogIndex])
	private val pending = Ref.unsafe[IO, TreeMap[Long, Entry]](TreeMap.empty)

	def append(seq: Long, update: ReplicationUpdate): IO[Unit] = {
		pending.update(_.updated(seq, Entry(update, None)))
	}

	// remove an item that was not accepted
	def discard(seq: Long): IO[Unit] = {
		pending.update(_.removed(seq))
	}

	// Mark a sequence as committed and now associated with a ratis log index.
	// Note that committed items are still present in their speculative form,
	// until markCommitted (when the log index has been applied locally
	// on this node).
	def markCommitted(seq: Long, index: LogIndex): IO[Unit] = {
		pending.update { entries =>
			entries.updatedWith(seq)(_.map(e => e.copy(index = Some(index))))
		}
	}

	// remove entries at or below watermark, as they have been applied
	// to the local persistent store.
	def markApplied(watermark: LogIndex): IO[Unit] = {
		pending.update(entries => entries.dropWhile((_, entry) => entry.index.exists(_.value <= watermark.value)))
	}
	
	def snapshot(base: PStateView): IO[ViewSnapshot] = {
		pending.get.map { pending =>
			val updates = pending.values.flatMap(_.update.pstateUpdates).groupBy(_.id).view.mapValues { v =>
				v.flatMap(_.updates.map { entry => Bytes.Value(entry.key) -> entry.value }).toMap
			}.toMap
			new ViewSnapshot(base, updates)
		}
	}
}

// an immutable snapshot of updates on an undrelying base
final class ViewSnapshot(
	base: PStateView,
	pstateUpdates: Map[PStateId, Map[Bytes.Value, Option[Bytes]]]
) extends PStateView {
	def get(pstate: PStateId, key: Bytes.Value): Option[Bytes] = {
		pstateUpdates.get(pstate).flatMap(_.get(key)).getOrElse {
			base.get(pstate, key)
		}
	}
}

// A mutable snapshot collecting updates (and serving a view), used for a single eval batch
// Unsafe (mutation), must not be accessed concurrently
final class EvalState(base: PStateView) extends PStateView {
	val depotAppends = new mutable.HashMap[DepotId, mutable.ArrayBuffer[Bytes]]()
	val pstateUpdates = new mutable.HashMap[PStateId, mutable.Map[Bytes.Value, Option[Bytes]]]()
	
	def append(depot: DepotId, value: Bytes) = {
		depotAppends.getOrElseUpdate(depot, new ArrayBuffer()).append(value)
	}

	def put(pstate: PStateId, key: Bytes.Value, value: Option[Bytes]) = {
		pstateUpdates.getOrElseUpdate(pstate, new mutable.HashMap()).put(key, value)
	}

	def get(pstate: PStateId, key: Bytes.Value): Option[Bytes] = {
		pstateUpdates.get(pstate).flatMap(_.get(key)).getOrElse {
			base.get(pstate, key)
		}
	}
	
	def toUpdate(txId: TxId): ReplicationUpdate = ReplicationUpdate(
		txId,
		depotAppends = depotAppends.toList.map { (id, buf) => DepotAppends(id, buf.toArray.map(DepotAppend.apply)) },
		pstateUpdates = pstateUpdates.toList.map { (id, writes) => PStateUpdates(id, writes.toArray.map { (k, v) =>
			PStateUpdate(k.raw, v)
		}) }
	)
}

object EvalState {
	def apply(base: PStateView, speculative: SpeculativeState) = {
		speculative.snapshot(base).map(new EvalState(_))
	}
}
