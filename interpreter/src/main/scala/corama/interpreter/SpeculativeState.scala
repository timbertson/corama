package corama.interpreter

import scala.collection.mutable
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import corama.*
import corama.interpreter.*

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import corama.server.Keyspace
import scala.annotation.unused

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
final class SpeculativeState(base: PStateView) {
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
	
	def snapshot: IO[ViewSnapshot] = {
		pending.get.map { pending =>
			val updates = pending.values.flatMap(_.update.pstateUpdates).map { v =>
				v.key -> v.value
			}.toMap
			new ViewSnapshot(base, updates)
		}
	}
}
