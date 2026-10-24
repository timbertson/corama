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

// Used to wrap raw types for serialization in the rama backend
class Tagged[A](val value: A, val tag: LightTypeTag) {
	def untag = value
}
object Tagged {
	def explicit[A](value: A, tag: LightTypeTag) = new Tagged(value, tag)
	def apply[A](value: A)(using tag: Tag[A]) = new Tagged(value, tag.tag)
	def untagAny(value: Any): Any = value.asInstanceOf[Tagged[Any]].untag
}

class RamaCodec(registry: CodecRegistry) extends RamaCustomSerialization[Tagged[?]] {
	private val pairCodec = Codec.forTuple[Bytes,Bytes]

	override def serialize(o: Tagged[?], out: DataOutput): Unit = {
		val entry = registry.forTag(o.tag)
		val valueBytes = (entry.codec.asInstanceOf[Codec[Any]]).encode(o.value.asInstanceOf[Any])
		val pair = pairCodec.encode(entry.id.raw, valueBytes)
		out.write(pair)
	}

	override def deserialize(in: DataInput): Tagged[?] = {
		// TODO: reuse
		val bytes: Bytes = Array.empty[Byte]
		in.readFully(bytes)
		val (id, payload) = pairCodec.decode(bytes).get
		val entry = registry.forId(Bytes.Value(id))
		val decoded = entry.codec.asInstanceOf[Codec[Any]].decode(payload)
		Tagged(decoded, entry.tag)
	}

	override def targetType(): Class[?] = classOf[Tagged[?]]
}

object CodecRegistry {
	case class Entry[A](id: Bytes.Value, tag: LightTypeTag, codec: Codec[A])

	def id(tag: LightTypeTag): Bytes.Value = {
		val id = tag.serialize().hash
		val idBytes = BigInt(id).toByteArray
		Bytes.Value(idBytes)
	}
	implicit val tagEq: Eq[LightTypeTag] = Eq.fromUniversalEquals
	private val anyTag = implicitly[Tag[Any]].tag
	
	def tagAny(value: Any): Tagged[Any] = new Tagged(value, anyTag)
}

class CodecRegistry() {
	import CodecRegistry.*
	private val typeMap: mutable.Map[LightTypeTag, Entry[?]] = mutable.Map.empty
	private val idMap: mutable.Map[Bytes.Value, Entry[?]] = mutable.Map.empty

	def register[A](tag: Tag[A], codec: Codec[A]) = {
		registerRaw(tag.tag, codec)
	}

	def registerRaw[A](tag: LightTypeTag, codec: Codec[A]) = {
		val id = CodecRegistry.id(tag)
		idMap.get(id) match {
			case Some(entry) => if (entry.tag =!= tag) {
				throw new Error(s"CodecRegistry type collision - both ${entry.tag} and ${tag} have the same hash")
			}
			case None => {
				val entry = Entry(id, tag, codec)
				idMap.update(id, entry)
				typeMap.update(tag, entry)
			}
		}
	}

	def forTag(tag: LightTypeTag): Entry[?] = typeMap.getOrElse(tag, throw new RuntimeException(s"Unregistered type tag: $tag"))

	def forId(id: Bytes.Value): Entry[?] = idMap.getOrElse(id, throw new RuntimeException("Unregistered type hash"))
}
