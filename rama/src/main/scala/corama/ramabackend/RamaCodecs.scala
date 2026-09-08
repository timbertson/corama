package corama.rama

import com.rpl.rama.RamaCustomSerialization
import java.io.DataOutput
import java.io.ObjectOutputStream
import java.io.{DataOutput, OutputStream}
import java.io.DataInput
import java.io.{DataInput, EOFException, InputStream}
import java.io.ObjectInputStream

class ListCodec extends RamaCustomSerialization[List[Serializable]] {
	override def serialize(obj: List[Serializable], out: DataOutput) = {
		val oo: ObjectOutputStream = new ObjectOutputStream(new DataOutputOutputStream(out))
		oo.writeObject(obj)
	}

	override def deserialize(input: DataInput): List[Serializable] = {
		val oi: ObjectInputStream = new ObjectInputStream(new DataInputInputStream(input))
		oi.readObject().asInstanceOf[List[Serializable]]
	}

	// The class we register in rama is just `List`, due to erasure
	override def targetType() = classOf[List[Serializable]]
}

class OptionCodec extends RamaCustomSerialization[Option[Serializable]] {
	override def serialize(obj: Option[Serializable], out: DataOutput) = {
		val oo: ObjectOutputStream = new ObjectOutputStream(new DataOutputOutputStream(out))
		oo.writeObject(obj)
	}

	override def deserialize(input: DataInput): Option[Serializable] = {
		val oi: ObjectInputStream = new ObjectInputStream(new DataInputInputStream(input))
		oi.readObject().asInstanceOf[Option[Serializable]]
	}

	// The class we register in rama is just `Option`, due to erasure
	override def targetType() = classOf[Option[Serializable]]
}

class DataOutputOutputStream(private val out: DataOutput) extends OutputStream {
  override def write(b: Int): Unit = out.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)
}

class DataInputInputStream(private val in: DataInput) extends InputStream {
  override def read(): Int =
    try {
      // DataInput reads an unsigned byte (0-255)
      in.readUnsignedByte()
    } catch {
      case _: EOFException => -1
    }

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    try {
      in.readFully(b, off, len)
      len
    } catch {
      case _: EOFException => -1
    }
}

