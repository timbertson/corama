package corama.server

import corama.{Bytes, Codec, DepotId}
import corama.interpreter.{DepotAppend, DepotAppends, ReplicationUpdate, TxId}
import corama.interpreter.db.RocksDb
import org.apache.ratis.protocol.{Message, RaftGroupId}
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.storage.RaftStorage
import org.apache.ratis.statemachine.{StateMachineStorage, TransactionContext}
import org.apache.ratis.statemachine.impl.{BaseStateMachine, SimpleStateMachineStorage}
import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.util.{Failure, Success}
import corama.interpreter.TaskId
import cats.effect.SyncIO
import corama.interpreter.Partitioned
import corama.PartitionId


object Keyspace {
	private val Depot = 'D'.toByte
	private [corama] val PState = 'S'.toByte
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
	def depotKey(partition: PartitionId, id: DepotId, index: Long, sub: Int): Bytes =
		Array(Depot) ++ Codec.forInt.encode(partition.id) ++ lenPrefixed(id.value) ++ Codec.forLong.encode(index) ++ Codec.forInt.encode(sub)

	// def pstateKey(id: PStateId, key: Bytes): Bytes =
	// 	Array(PState) ++ lenPrefixed(id.value) ++ key

	val applyOffsetKey: Bytes = metaKey("apply_offset")
}

/** The Raft log is the replog and applying it materializes the external views.
	* Apply is idempotent by key, so replaying uncommitted entries after a
	* crash is fine.
	*/
class StateMachine(task: TaskId) extends BaseStateMachine {
	private val storage = new SimpleStateMachineStorage()
	private var db: Option[RocksDb] = None
	private var release: Option[SyncIO[Unit]] = None

	// private var db: Option[RocksDb] = None

	private def rocks: RocksDb = db.getOrElse(throw new IllegalStateException("StateMachine not initialized"))

	override def initialize(server: RaftServer, group: RaftGroupId, raftStorage: RaftStorage): Unit = {
		super.initialize(server, group, raftStorage)
		storage.init(raftStorage)
		val dir = new File(raftStorage.getStorageDir().getStateMachineDir(), s"db-${task.id}")
		val (db, release) = RocksDb(dir.getPath()).allocated.unsafeRunSync()
		this.db = Some(db)
		this.release = Some(release)
	}

	override def getStateMachineStorage(): StateMachineStorage = storage

	override def close(): Unit = {
		release.foreach(_.unsafeRunSync())
		db = None
		release = None
	}

	override def query(request: Message): CompletableFuture[Message] = {
		// TODO: linearizable reads need readIndex; this reads local state.
		val offset = rocks.get(Keyspace.applyOffsetKey).unsafeRunSync()
			.flatMap(Codec.forLong.decode(_).toOption)
			.getOrElse(0L)

		val result = Message.valueOf(ByteBuffer.wrap(Codec.forLong.encode(offset)))
		CompletableFuture.completedFuture(result)
	}

	override def applyTransaction(trx: TransactionContext): CompletableFuture[Message] = {
		val entry = trx.getLogEntry
		try {
			val update = ServerCodecs.replicationUpdate.decode(entry.getStateMachineLogEntry.getLogData.toByteArray) match {
				case Success(update) => update
				case Failure(e) => throw new RuntimeException(s"Failed to decode ReplicationUpdate at log index ${entry.getIndex}", e)
			}
			// TODO: why is this a future if we're writing sync?
			rocks.writeSync(StateMachine.applyWrites(update, entry.getIndex))
			updateLastAppliedTermIndex(entry.getTerm, entry.getIndex)
			CompletableFuture.completedFuture(ack(update.id))
		} catch {
			case e: Exception => CompletableFuture.failedFuture(e)
		}
	}

	private def ack(id: TxId): Message = Message.valueOf(ByteBuffer.wrap(ServerCodecs.txId.encode(id)))
}

object StateMachine {
	private[server] def applyWrites(update: ReplicationUpdate, index: Long): Vector[(Bytes, Option[Bytes])] = {
		val depotWrites = update.depotAppends.flatMap { case DepotAppends(id, appends) =>
			appends.zipWithIndex.map { case (Partitioned(partitionId, DepotAppend(value)), sub) =>
				Keyspace.depotKey(partitionId, id, index, sub) -> Some(value)
			}
		}
		val pstateWrites = update.pstateUpdates.map(up => up.key.raw.raw -> up.value)
		val meta = Keyspace.applyOffsetKey -> Some(Codec.forLong.encode(index))
		(depotWrites ++ pstateWrites).toVector :+ meta
	}
}
