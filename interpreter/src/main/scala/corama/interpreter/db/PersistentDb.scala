package corama.interpreter.db

import cats.effect.*
import cats.effect.kernel.Resource
import corama.Bytes
import fs2.Stream
import corama._

import cats.*
import scala.util.Try
import org.rocksdb
import java.io.File

// object PersistentDb extends IOLogging {
// 	type Base = RocksDb

// 	object Ops extends RawDb.Ops[Base] {
// 		override def get(base: Base, key: Bytes): IO[Option[Bytes]] = base.get(key)

// 		override def put(base: Base, key: Bytes, value: Option[Bytes]): IO[Base] = {
// 			base.put(key, value).as(base)
// 		}

// 		override def putAll(base: Base, updates: Iterable[RawUpdate]): IO[Base] = {
// 			// TODO could surely be more efficient
// 			updates.toSeq.traverse_(base.put.tupled).as(base)
// 		}

// 		override def iterator(base: Base, prefix: Bytes, startKey: Bytes): Stream[IO, (Bytes, Bytes)] = {
// 			Stream.resource(base.iteratorR).flatMap { it =>
// 				base.scanFromPrefix(it, prefix, startKey)
// 			}
// 		}
// 	}

// 	def fromRocksDb[K, V](name: DbName[K,V], db: RocksDb, bufferStrategy: BufferStrategy)(implicit tracer: Tracer[IO]): Resource[IO, RawDb] = {
// 		RawDb.impl(name, bufferStrategy, Ops, db)
// 	}
	
// 	def atPath[K, V](name: DbName[K,V], path: String, bufferStrategy: BufferStrategy)(implicit tracer: Tracer[IO]): Resource[IO, RawDb] =
// 		RocksDb(path).flatMap(db => fromRocksDb(name, db, bufferStrategy))

// 	def forNode[K, V](dataRoot: DataRoot, nodeId: TaskId, partition: TaskId, name: DbName[K, V], bufferStrategy: BufferStrategy)(implicit tracer: Tracer[IO]): Resource[IO, RawDb] = {
// 		val dbRoot = os.Path(dataRoot.value.toAbsolutePath())/"db"/s"${nodeId.value}-${partition.value}-${name.name}"
// 		Resource.eval(IO.delay(os.makeDir.all(dbRoot))) *> atPath(name, dbRoot.toString, bufferStrategy)
// 	}
// }

final class RocksDb private (val client: rocksdb.RocksDB) {
	import org.rocksdb.*

	def getSync(key: Bytes): Option[Bytes] = Option(client.get(key))

	/** Atomic batched write, fsynced — one WAL fsync spans the whole batch. */
	def writeSync(updates: Seq[(Bytes, Option[Bytes])]): Unit = {
		val writeOptions = new rocksdb.WriteOptions()
		val batch = new rocksdb.WriteBatch()
		try {
			updates.foreach { case (key, value) =>
				value.fold(batch.delete(key))(batch.put(key, _))
			}
			client.write(writeOptions, batch)
		} finally {
			batch.close()
			writeOptions.close()
		}
	}

	def closeSync(): Unit = client.close()

	def get(key: Bytes): IO[Option[Bytes]] = IO.delay(getSync(key))
	
	def iteratorR = Resource.fromAutoCloseable(IO.delay(client.newIterator()))
	
	def scanFromPrefix(it: RocksIterator, prefix: Bytes, startKey: Bytes): Stream[IO, (Bytes, Bytes)] = {
		val setup = IO.delay(it.seek(startKey))
		val next = IO.defer {
			if (it.isValid()) { // points at an entry
				val key = it.key()
				if (key.startsWith(prefix)) {
					val value = it.value()
					it.next()
					IO.pure(Some((key, value)))
				} else {
					IO.pure(None) // end of prefix
				}
			} else {
				// !isValid, either we reached end of DB (None) or there was an error
				IO.fromTry(Try(it.status())).as(None)
			}
		}
		Stream.exec(setup) ++ Stream.repeatEval(next).unNoneTerminate
	}

	def put(key: Bytes, value: Option[Bytes]): IO[Unit] = IO.delay {
		value.fold(client.delete(key))(value => client.put(key, value))
	}
	
	def close: IO[Unit] = IO.delay(closeSync())
}

object RocksDb {
	private[corama] def apply(path: String): Resource[IO, RocksDb] = {
		optionsR.flatMap { options =>
			options.setCreateIfMissing(true)
			Resource.make(applyF(path, options))(_.close)
		}
	}

	// resource management is the responsibility of the caller
	def openSyncUnsafe(path: File): RocksDb = {
		org.rocksdb.RocksDB.loadLibrary()
		val options = new rocksdb.Options()
		options.setCreateIfMissing(true)
		try new RocksDb(rocksdb.RocksDB.open(options, path.getAbsolutePath()))
		finally options.close()
	}

	private def applyF(
		path: String,
		options: rocksdb.Options,
	): IO[RocksDb] =
		IO.delay(rocksdb.RocksDB.loadLibrary()) *>
			IO.delay(rocksdb.RocksDB.open(options, path))
			.map(new RocksDb(_))

	def optionsR: Resource[IO, rocksdb.Options] = Resource.fromAutoCloseable(IO.delay(new rocksdb.Options()))
}

