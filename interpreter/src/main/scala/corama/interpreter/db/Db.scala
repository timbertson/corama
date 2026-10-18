package corama.interpreter.db

import cats.effect.*
import cats.*
import corama.Codec
import corama.Bytes
import corama.error.Util.*
import scala.util.Try
import scala.util.Failure
import scala.util.Success

// type PaginatedSet[K, V] = Paginated[Record.Set, K, V]
// type PaginatedItems[K, V] = Paginated[Record.Item, K, V]

// case class Paginated[R, K, V](values: List[V], cursor: Option[Cursor[R, K]]) {
// 	def tuple = (values, cursor)

// 	def evalMap[V2](fn: V => IO[V2]): IO[Paginated[R, K, V2]] = {
// 		values.traverse(fn).map { mappedValues =>
// 			new Paginated[R, K, V2](mappedValues, cursor)
// 		}
// 	}
// }

// object Paginated {
// 	private type Serialized[R, K, V] = (List[V], Option[Cursor[R, K]])

// 	implicit def encoder[R, K, V](implicit encoder: Encoder[V]): Encoder[Paginated[R, K, V]] = ps =>
// 		implicitly[Encoder[Serialized[R, K, V]]].encode(ps.tuple)

// 	implicit def decoder[R, K, V](implicit decoder: Decoder[V]): Decoder[Paginated[R, K, V]] = bytes =>
// 		implicitly[Decoder[Serialized[R, K, V]]].decode(bytes).map((coll, cursor) => Paginated(coll, cursor))

// 	implicit def eq[R, K, V: Eq]: Eq[Paginated[R, K, V]] = Eq.by(_.tuple)
// }

object DbValue {
	type DecodePair[T] = ((Bytes, Bytes)) => Try[T]

	// each entry is prefixed with a byte to distinguish type
	private val ITEM_MAGIC = 0.byteValue()
	private val SET_ENTRY_MAGIC = 1.byteValue()
	private [corama] val MAGIC_LEN = 1
	
	def encodeItemKey[K](k: K)(implicit encoder: Codec[K]): Bytes = {
		Array(ITEM_MAGIC) ++ encoder.encode(k)
	}

	def encodeSetKey[K](k: K)(implicit encoder: SetKey[K]): Bytes = {
		Array(SET_ENTRY_MAGIC) ++ encoder.encode(k)
	}

	def encodeSetEntry[K, V](k: K, v: V)(implicit KE: SetKey[K], VE: Codec[V]): (Bytes, Option[Bytes]) = {
		val keyOnly = encodeSetKey(k)
		val keyLen = keyOnly.length - 1 // 1 magic byte isn't included in length
		(keyOnly ++ VE.encode(v), Some(Codec.forInt.encode(keyLen)))
	}

	def decode[T](kv: (Bytes, Bytes), decodePair: DecodePair[T]): IO[T] = {
		val (k, v) = kv
		val bytePair = k.headOption match {
			case Some(`ITEM_MAGIC`) => Success((k.tail, v))

			// setEntries are encoded with the values concatenated onto the key, for uniqueness.
			// In order to decode unambiguously, the value is the key's serialized length as an int
			case Some(`SET_ENTRY_MAGIC`) => Codec.forInt.decode(v).map(keyLen => k.tail.splitAt(keyLen))

			case _ => Failure(err(s"Couldn't decode DB value with magic byte ${k.headOption}"))
		}
		IO.fromTry(bytePair.flatMap(decodePair))
	}
}

// class Db[K, V](db: RawDb, dv: DataVisibility)(implicit
// 	KE: Encoder[K],
// 	VE: Encoder[V],
// 	KD: Decoder[K],
// 	VD: Decoder[V]
// 	) {
// 	import DbValue.*

// 	def get(key: K): IO[Option[V]] =
// 		db.get(encodeItemKey(key)).flatMap(bytes => IO.fromTry(bytes.traverse(VD.decode)))

// 	def put(key: K, value: V): IO[Unit] = db.put(dv, encodeItemKey(key), Some(VE.encode(value)))

// 	def delete(key: K): IO[Unit] = db.put(dv, encodeItemKey(key), None)

// 	// TODO: curry
// 	def modify[R](key: K, fn: Option[V] => (Option[V], R)): IO[R] =
// 		db.modify(dv, encodeItemKey(key), (bytes) => {
// 			bytes.traverse(VD.decode).map { currentOpt =>
// 				val (updated, ret) = fn(currentOpt)
// 				(updated.map(VE.encode), ret)
// 			}
// 		})

// 	// TODO curry
// 	def update(key: K, fn: Option[V] => Option[V]): IO[Unit] = modify[Unit](key, x => (fn(x), ()))
	
// 	private def decodeKey(kv: (Bytes, Bytes)) = decode(kv, { case (k, _) => KD.decode(k) })
// 	private def decodeValue(kv: (Bytes, Bytes)) = decode(kv, { case (_, v) => VD.decode(v) })
// 	private def decodeKeyValue(kv: (Bytes, Bytes)) = decode(kv, { case (k,v) =>
// 		for {
// 			kv <- KD.decode(k)
// 			vv <- VD.decode(v)
// 		} yield (kv, vv)
// 	})
	
// 	def itemKeysPrefixed(cursor: ItemCursor[K], limit: Int = Db.defaultPageSize): IO[PaginatedItems[K, K]] = {
// 		scanPage(cursor, limit, decodeKey)
// 	}

// 	def allItemKeysPrefixed(cursor: ItemCursor[K]): IO[List[K]] = {
// 		scanFull(cursor, decodeKey)
// 	}

// 	def itemsPrefixed(cursor: ItemCursor[K], limit: Int = Db.defaultPageSize): IO[PaginatedItems[K, (K, V)]] = {
// 		scanPage(cursor, limit, decodeKeyValue)
// 	}

// 	def allItemsPrefixed(cursor: ItemCursor[K]): IO[List[(K, V)]] = {
// 		scanFull(cursor, decodeKeyValue)
// 	}

// 	def getSet(key: K)(implicit sk: SetKey[K]): IO[List[V]] = {
// 		scanFull(Cursor.setEntries(key), decodeValue)
// 	}

// 	def getSetPage(setCursor: SetCursor[K], limit: Int): IO[PaginatedSet[K, V]] = {
// 		scanPage(setCursor, limit, decodeValue)
// 	}

// 	private def scanFull[T, R](cursor: Cursor[R, K], decoder: ((Bytes, Bytes)) => IO[T]): IO[List[T]] = {
// 		db.scanFromPrefix(cursor.prefix, cursor.prefix, stream =>
// 			stream
// 			.evalMap(decoder)
// 			.compile.toList
// 		)
// 	}

// 	private def scanPage[R, T](cursor: Cursor[R, K], limit: Int, decoder: ((Bytes, Bytes)) => IO[T]): IO[Paginated[R, K, T]] = {
// 		db.scanFromPrefix(cursor.prefix, cursor.value, _.take(limit + 1).compile.toList).flatMap { rawEntries =>
// 			if (rawEntries.length > limit) {
// 				// there were more than `limit` items
// 				val nextKey = rawEntries(limit)._1
// 				for {
// 					nextCursor <- cursor.startAfterBytes(nextKey)
// 					values = rawEntries.take(limit)
// 					decoded <- values.traverse(decoder)
// 				} yield Paginated[R, K, T](decoded, Some(nextCursor))
// 			} else {
// 				rawEntries.traverse(decoder).map { Paginated(_, None) }
// 			}
// 		}
// 	}

// 	def removeFromSet(key: K, value: V)(implicit SK: SetKey[K]): IO[Unit] = {
// 		db.put(dv, encodeSetEntry(key, value)._1, None)
// 	}

// 	def addToSet(key: K, value: V)(implicit SK: SetKey[K]): IO[Unit] = {
// 		val (k, v) = encodeSetEntry(key, value)
// 		db.put(dv, k, v)
// 	}
// }

// object Db {
// 	val defaultPageSize: Int = 128

// 	class Viewer[K, V](name: DbName[K, V], val rawDb: RawDb) {
// 		def bind(dv: DataVisibility): Db[K, V] = {
// 			implicit val KE: Encoder[K] = name.KE
// 			implicit val VE: Encoder[V] = name.VE
// 			implicit val KD: Decoder[K] = name.KD
// 			implicit val VD: Decoder[V] = name.VD
// 			new Db(rawDb, dv)
// 		}
// 	}
// }

// type DbMap = Map[String, Db.Viewer[Any, Any]]

// type PartitionedDbMap = Map[TaskId, DbMap]
