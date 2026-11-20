package corama

import java.util.Arrays
import cats.Eq
import cats.kernel.Order
import scala.reflect.ClassTag
import scala.util.Try

type Bytes = Array[Byte]
object Bytes {
	// A wrapper type with proper equality / hashcode
	class Value(val raw: Array[Byte]) extends Serializable {
		override def equals(obj: Any): Boolean = obj match {
			case v: Value => eqInstance.eqv(v.raw, raw)
			case _ => false
		}
		override def hashCode(): Int = Arrays.hashCode(raw)
	}
	object Value {
		def apply(raw: Bytes) = new Value(raw)
	}

	private [corama] val NULL_BYTE = 0.byteValue()

	private [corama] val MAX_BYTE = ~NULL_BYTE
	
	def eq(a: Bytes, b: Bytes) = Arrays.equals(a, b)

	implicit val eqInstance: Eq[Bytes] = Eq.instance(eq)

	implicit val orderInstance: Order[Bytes] = Order.from { (a,b) =>
		Arrays.compare(a, b)
	}

	implicit val orderingInstance: Ordering[Bytes] = Order.catsKernelOrderingForOrder[Bytes](orderInstance)
	
	def empty: Bytes = Array.empty
}

// Carry a value along with its bytes
class Encoded[A](val value: A, val bytes: Bytes)
object Encoded {
	def apply[A](value: A)(using codec: Codec[A]) = new Encoded(value, codec.encode(value))

	def fromBytes[A](bytes: Bytes)(using codec: Codec[A]): Try[Encoded[A]] =
		codec.decode(bytes).map(new Encoded(_, bytes))
}
