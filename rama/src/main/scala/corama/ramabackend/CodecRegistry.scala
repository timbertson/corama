package corama.rama

import cats.syntax.all.*
import com.rpl.rama.RamaCustomSerialization
import corama.*
import java.io.DataInput
import java.io.DataOutput
import izumi.reflect.Tag
import izumi.reflect.macrortti.LightTypeTag
import scala.collection.mutable
import cats.Eq
import corama.internal.HexString

// Used to wrap raw types for serialization in the rama backend.
// Where possible we use LightTypeTag to improve debuggability,
// but tags within rama closures must use raw bytes
// TODO: use only bytes for efficiency?
class Tagged[A](val value: A, val tag: Either[Bytes.Value, LightTypeTag]) {
	override def toString(): String = s"Tagged[$tag]($value)"
	def untag = value

	override def equals(obj: Any): Boolean = obj match {
		case other: Tagged[?] => value.equals(other.value)
		case _ => false
	}

	override def hashCode(): Int = value.hashCode
}

object Tagged {
	def explicit[A](value: A, tag: LightTypeTag) = new Tagged(value, Right(tag))
	def explicit[A](value: A, tag: Bytes.Value) = new Tagged(value, Left(tag))
	def apply[A](value: A)(using tag: Tag[A]) = new Tagged(value, Right(tag.tag))
	def untagAny(value: Any): Any = value.asInstanceOf[Tagged[Any]].untag
}

class RamaCodec(registry: CodecRegistry) extends RamaCustomSerialization[Tagged[?]] {
	private val pairCodec = Codec.forTuple[Bytes,Bytes]

	override def serialize(o: Tagged[?], out: DataOutput): Unit = {
		// println(s"RamaCodec: serialize Tagged ${o}")
		val entry = registry.forTag(o.tag)
		val valueBytes = (entry.codec.asInstanceOf[Codec[Any]]).encode(o.value.asInstanceOf[Any])
		val pair = pairCodec.encode(entry.id.raw, valueBytes)
		out.writeInt(pair.length)
		out.write(pair)
	}

	override def deserialize(in: DataInput): Tagged[?] = {
		val len = in.readInt()
		// TODO: reuse this buffer?
		val bytes: Bytes = new Array[Byte](len)
		in.readFully(bytes)
		// println(s"reading deserialized: $len // ${bytes.map(b => String.format("%02x", b)).mkString}")
		try {
			val (id, payload) = pairCodec.decode(bytes).get
			val entry = registry.forId(Bytes.Value(id))
			val decoded = entry.codec.asInstanceOf[Codec[Any]].decode(payload).get
			Tagged.explicit(decoded, entry.tag)
		} catch {
			case e => {
				// rama swallows all deserialization errors
				println(s"Deserialize error: $e")
				throw(e)
			}
		}
	}

	// Get the plain class erased type
	override def targetType(): Class[?] = classOf[Tagged[?]]
	// override def targetType(): Class[?] = Class.forName("corama.rama.Tagged")

}

object RamaCodec {
	private var globalRegistry = Option.empty[CodecRegistry]

	def setGlobal(registry: CodecRegistry): Unit = {
		val last = globalRegistry
		globalRegistry = Some(registry)
		last.foreach { last =>
			println(s"Warn: replacing global registry ${last} with ${registry}")
		}
	}

	def getGlobal: CodecRegistry = {
		globalRegistry.getOrElse {
			throw new RuntimeException("RamaCodec global is not set")
		}
	}
}

class RamaCodecSingleton() extends RamaCodec(RamaCodec.getGlobal) {
}

object CodecRegistry {
	case class Entry[A](id: Bytes.Value, tag: LightTypeTag, codec: Codec[A])

	def id(tag: LightTypeTag): Bytes.Value = {
		val id = tag.serialize().hash
		val idBytes = BigInt(id).toByteArray
		Bytes.Value(idBytes)
	}
	implicit val tagEq: Eq[LightTypeTag] = Eq.fromUniversalEquals
}

class CodecRegistry() {
	import CodecRegistry.*
	private val typeMap: mutable.Map[LightTypeTag, Entry[?]] = mutable.Map.empty
	private val idMap: mutable.Map[Bytes.Value, Entry[?]] = mutable.Map.empty

	override def toString(): String = s"CodecRegistry(size: ${typeMap.size})"

	def register[A](tag: Tag[A], codec: Codec[A]) = {
		registerRaw(tag.tag, codec)
	}

	def registerRaw[A](tag: LightTypeTag, codec: Codec[A]) = {
		val id = CodecRegistry.id(tag)
		idMap.get(id) match {
			case Some(entry) => if (entry.tag =!= tag) {
				throw new Error(s"CodecRegistry: type collision - both ${entry.tag} and ${tag} have the same hash")
			}
			case None => {
				val entry = Entry(id, tag, codec)
				idMap.update(id, entry)
				typeMap.update(tag, entry)
			}
		}
	}
	
	private def describeAll: String = {
		typeMap.keys.map(t => s" - ${t} [${t.serialize().hash} / ${HexString.slowDebug(CodecRegistry.id(t).raw)}]").toList.sorted.mkString("\n")
	}

	def forTag(tag: LightTypeTag): Entry[?] = typeMap.getOrElse(tag,
		throw new RuntimeException(s"$this: Unregistered type tag: $tag. Registry contents:\n$describeAll")
	)

	def forTag(tag: Either[Bytes.Value, LightTypeTag]): Entry[?] = tag match {
		case Right(value) => forTag(value)
		case Left(value) => forId(value)
	}

	def forId(id: Bytes.Value): Entry[?] = idMap.getOrElse(id,
		throw new RuntimeException(s"$this: Unregistered type hash: ${HexString.slowDebug(id.raw)}. Registry contents:\n$describeAll")
	)
}
