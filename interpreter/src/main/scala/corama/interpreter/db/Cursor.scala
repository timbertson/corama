package corama.interpreter.db

import cats.effect.*
import cats.implicits.*
import cats.*
import corama.{Decoder, Encoder}
import corama.Bytes
import corama.error.Util.*
import scala.util.Try
import scala.util.Failure

// phantom type to ensure distinction between item and set keys, which
// have different encoded prefixes
sealed trait Record
object Record {
	final class Set extends Record
	final class Item extends Record
}


class Cursor[Record, Key]private (private [db] val value: Bytes, private val prefixLength: Int) {
	def prefix = value.slice(0, prefixLength)
	
	private [db] def startAfterBytes(newValue: Bytes): IO[Cursor[Record, Key]] = {
		if (newValue.startsWith(prefix)) {
			IO.pure(new Cursor[Record, Key](newValue, prefixLength))
		} else {
			IO.raiseError(err("New cursor must start with the same prefix"))
		}
	}
}

type SetCursor[K] = Cursor[Record.Set, K]
type ItemCursor[K] = Cursor[Record.Item, K]

object Cursor {
	import Bytes.eqInstance

	implicit def encoder[R, K]: Encoder[Cursor[R, K]] = c =>
		Encoder.forTuple[Bytes, Int].encode((c.value, c.prefixLength))

	implicit def decoder[R, K]: Decoder[Cursor[R, K]] = b =>
		Decoder.forTuple[Bytes, Int].decode(b).map { (value, len) => new Cursor(value, len) }

	implicit def eq[R, K]: Eq[Cursor[R, K]] = Eq.by(c => (c.value, c.prefixLength))

	def allItems[K]: ItemCursor[K] = {
		// unit is encoded as Array.empty
		val raw = itemsWithPrefix[Unit](())(using Encoder.forUnit)
		new Cursor[Record.Item, K](raw.prefix, raw.prefixLength)
	}
	
	// get a cursor representing the given key prefix
	def itemsWithPrefix[K](key: K)(implicit E: Encoder[K]): ItemCursor[K] = {
		val bytes = DbValue.encodeItemKey(key)
		new Cursor[Record.Item, K](bytes, bytes.length)
	}

	def setEntries[K](key: K)(implicit sk: SetKey[K]): SetCursor[K] = {
		val prefix = DbValue.encodeSetKey(key)
		new Cursor[Record.Set, K](prefix, prefix.length)
	}
}


// Set keys have additional constraints, because we need an unambiguous prefix that can't
// also be the prefix of a different key. For strings we append a null byte (which never appears
// in a utf8 string), and for numbers we rely on a fixed-length encoding.
// TODO: explore 
trait SetKey[K] extends Encoder[K] {
	def decodePrefix(bytes: Bytes): Try[(K, Bytes)]
}

object SetKey {
	implicit val forString: SetKey[String] = new SetKey[String] {
		override def decodePrefix(bytes: Bytes): Try[(String, Bytes)] = {
			val nullBytePos = bytes.indexOf(Bytes.NULL_BYTE)
			if (nullBytePos < 0) {
				Failure(err("null byte not found in encoded key"))
			} else {
				val (strBytes, rest) = bytes.splitAt(nullBytePos)
				Decoder.forString.decode(strBytes).map(_ -> rest.tail)
			}
		}

		override def encode(k: String) = {
			val bytes = Encoder.forString.encode(k)
			if (bytes.contains(Bytes.NULL_BYTE)) {
				// should be impossible for utf8 encoding
				throw new RuntimeException(s"key encoder for ${k.getClass()} produced a null byte; this is disallowed for set keys")
			}
			bytes.appended(Bytes.NULL_BYTE)
		}
	}

	implicit val forInt: SetKey[Int] = new SetKey[Int] {
		override def decodePrefix(bytes: Bytes): Try[(Int, Bytes)] = {
			val (k, rest) = bytes.splitAt(4)
			Decoder.forInt.decode(k).map(_ -> rest)
		}

		override def encode(k: Int) = Encoder.forInt.encode(k)
	}
}
