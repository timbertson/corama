package corama.server

import corama.{Bytes, Codec, DepotId, PStateId}
import corama.interpreter.{DepotAppend, DepotAppends, PStateUpdate, PStateUpdates, ReplicationUpdate, TxId}
import corama.interpreter.db.RocksDb
import org.apache.ratis.protocol.{Message, RaftGroupId}
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.storage.RaftStorage
import org.apache.ratis.statemachine.{StateMachineStorage, TransactionContext}
import org.apache.ratis.statemachine.impl.{BaseStateMachine, SimpleStateMachineStorage}
import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.util.{Failure, Success, Try}

/** Wire codecs for the replog payload (`ReplicationUpdate`), built on the existing
	* `Codec` combinators (tuples / lists / length-prefixed primitives).
	*/
object ReplogCodecs {
	private def via[A, B](codec: Codec[B])(to: A => B)(from: B => A): Codec[A] = new Codec[A] {
		def encode(a: A): Bytes = codec.encode(to(a))
		def decode(bytes: Bytes): Try[A] = codec.decode(bytes).map(from)
	}

	implicit val txId: Codec[TxId] = via[TxId, Long](Codec.forLong)(_.value)(TxId.apply)
	implicit val depotId: Codec[DepotId] = via[DepotId, String](Codec.forString)(_.value)(DepotId.apply)
	implicit val pstateId: Codec[PStateId] = via[PStateId, String](Codec.forString)(_.value)(PStateId.apply)
	implicit val depotAppend: Codec[DepotAppend] = via[DepotAppend, Bytes](Codec.forBytes)(_.value)(DepotAppend.apply)

	implicit val pstateUpdate: Codec[PStateUpdate] =
		via[PStateUpdate, (Bytes, Option[Bytes])](Codec.forTuple[Bytes, Option[Bytes]])(p => (p.key, p.value)) { case (key, value) => PStateUpdate(key, value) }

	implicit val pstateUpdates: Codec[PStateUpdates] =
		via[PStateUpdates, (PStateId, List[PStateUpdate])](Codec.forTuple[PStateId, List[PStateUpdate]])(p => (p.id, p.updates.toList)) { case (id, updates) => PStateUpdates(id, updates.toVector) }

	implicit val depotAppends: Codec[DepotAppends] =
		via[DepotAppends, (DepotId, List[Bytes])](Codec.forTuple[DepotId, List[Bytes]])(d => (d.id, d.updates.map(_.value).toList)) { case (id, values) => DepotAppends(id, values.map(DepotAppend.apply).toVector) }

	implicit val replicationUpdate: Codec[ReplicationUpdate] =
		via[ReplicationUpdate, ((TxId, List[DepotAppends]), List[PStateUpdates])](Codec.forTuple[(TxId, List[DepotAppends]), List[PStateUpdates]])(r => ((r.id, r.depotAppends), r.pstateUpdates)) {
			case ((id, depotAppends), pstateUpdates) => ReplicationUpdate(id, depotAppends, pstateUpdates)
		}
}

object Keyspace {
	private val Depot = 'D'.toByte
	private val PState = 'S'.toByte
	private val Meta = 'M'.toByte

	private def lenPrefixed(s: String): Bytes = {
		val raw = Codec.forString.encode(s)
		Codec.forInt.encode(raw.length) ++ raw
	}

	private def metaKey(name: String): Bytes = Array(Meta) ++ lenPrefixed(name)

	/** One depot key per appended record. A single replog entry may append several
		* records to the same depot; they get successive `sub` indices, so scanning the
		* prefix by (index, sub) yields append order
		*/
	def depotKey(id: DepotId, index: Long, sub: Int): Bytes =
		Array(Depot) ++ lenPrefixed(id.value) ++ Codec.forLong.encode(index) ++ Codec.forInt.encode(sub)

	def pstateKey(id: PStateId, key: Bytes): Bytes =
		Array(PState) ++ lenPrefixed(id.value) ++ key

	val applyOffsetKey: Bytes = metaKey("apply_offset")
}

/** The Raft log is the replog and applying it materializes the external views.
	* Apply is idempotent by key, so replaying uncommitted entries after a
	* crash is fine.
	*/
class StateMachine extends BaseStateMachine {
	private val storage = new SimpleStateMachineStorage()
	private var db: Option[RocksDb] = None

	private def rocks: RocksDb = db.getOrElse(throw new IllegalStateException("StateMachine not initialized"))

	override def initialize(server: RaftServer, group: RaftGroupId, raftStorage: RaftStorage): Unit = {
		super.initialize(server, group, raftStorage)
		storage.init(raftStorage)
		val dir = new File(raftStorage.getStorageDir().getStateMachineDir(), "db")
		db = Some(RocksDb.openSyncUnsafe(dir))
	}

	override def getStateMachineStorage(): StateMachineStorage = storage

	override def close(): Unit = db.foreach(_.closeSync())

	override def query(request: Message): CompletableFuture[Message] = {
		// TODO: linearizable reads need readIndex; this reads local state.
		val offset = rocks.getSync(Keyspace.applyOffsetKey).flatMap(Codec.forLong.decode(_).toOption).getOrElse(0L)
		CompletableFuture.completedFuture(Message.valueOf(ByteBuffer.wrap(Codec.forLong.encode(offset))))
	}

	override def applyTransaction(trx: TransactionContext): CompletableFuture[Message] = {
		val entry = trx.getLogEntry
		try {
			val update = ReplogCodecs.replicationUpdate.decode(entry.getStateMachineLogEntry.getLogData.toByteArray) match {
				case Success(update) => update
				case Failure(e) => throw new RuntimeException(s"Failed to decode ReplicationUpdate at log index ${entry.getIndex}", e)
			}
			rocks.writeSync(StateMachine.applyWrites(update, entry.getIndex))
			updateLastAppliedTermIndex(entry.getTerm, entry.getIndex)
			CompletableFuture.completedFuture(ack(update.id))
		} catch {
			case e: Exception => CompletableFuture.failedFuture(e)
		}
	}

	private def ack(id: TxId): Message = Message.valueOf(ByteBuffer.wrap(ReplogCodecs.txId.encode(id)))
}

object StateMachine {
	private[server] def applyWrites(update: ReplicationUpdate, index: Long): Vector[(Bytes, Option[Bytes])] = {
		val depotWrites = update.depotAppends.flatMap { case DepotAppends(id, appends) =>
			appends.zipWithIndex.map { case (DepotAppend(value), sub) =>
				Keyspace.depotKey(id, index, sub) -> Some(value)
			}
		}
		val pstateWrites = update.pstateUpdates.flatMap { case PStateUpdates(id, updates) =>
			updates.map { case PStateUpdate(key, value) =>
				Keyspace.pstateKey(id, key) -> value
			}
		}
		val meta = Keyspace.applyOffsetKey -> Some(Codec.forLong.encode(index))
		(depotWrites ++ pstateWrites).toVector :+ meta
	}
}
