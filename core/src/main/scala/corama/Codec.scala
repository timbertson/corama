package corama

import cats.syntax.all.*
import io.circe as circe
import java.nio.charset.StandardCharsets
import scala.util.Try
import scala.util.Success
import com.google.protobuf
import scala.util.Failure
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.util.Arrays
import cats.Eq
import streamdsl.rpc.Either.Value.Empty
import scalapb.GeneratedMessageCompanion
import scalapb.GeneratedMessage
import cats.kernel.Order
import corama.error.Util._

type Bytes = Array[Byte]
object Bytes {
	// A wrapper type with proper equality / hashcode
	class Value(val raw: Array[Byte]) {
		override def equals(obj: Any): Boolean = obj match {
			case arr: Array[Byte] => eqInstance.eqv(arr, raw)
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
}

trait Codec[A] {
	def encode(a: A): Bytes
	def decode(bytes: Bytes): Try[A]
}

object Codec {
	// this one's explicit, otherwise we may pick up unintended instances
	def json[T](implicit cd: circe.Decoder[T], ce: circe.Encoder[T]): Codec[T] = new Codec[T] {
		import io.circe.syntax.*
		def decode(bytes: Bytes): Try[T] = {
			Try(new String(bytes, StandardCharsets.UTF_8)).flatMap { str =>
				(for {
					json <- circe.parser.parse(str).toTry
					t <- cd.decodeJson(json).toTry
				} yield t).adaptErr(cause => new RuntimeException(s"Failed to decode JSON: ${str}", cause))
			}
		}
		def encode(t: T): Bytes = t.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
	}

	implicit val forString: Codec[String] = new Codec {
		def decode(bytes: Bytes): Try[String] = Try(new String(bytes, StandardCharsets.UTF_8))
		def encode(a: String): Bytes = a.getBytes(StandardCharsets.UTF_8)
	}

	implicit def forEither[L, R](implicit cl: Codec[L], cr: Codec[R]): Codec[Either[L, R]] = new Codec[Either[L, R]] {
		override def decode(bytes: Bytes) = {
			Try(streamdsl.rpc.Either.parseFrom(bytes)).flatMap { parsed =>
				parsed.value match {
					case Empty => Failure(err("Either is empty"))
					case streamdsl.rpc.Either.Value.Left(l) => cl.decode(l.toByteArray()).map(Left.apply)
					case streamdsl.rpc.Either.Value.Right(r) => cr.decode(r.toByteArray()).map(Right.apply)
				}
			}
		}
		
		override def encode(v: Either[L, R]): Bytes = {
			val bytestr = v match {
				case Left(l) => streamdsl.rpc.Either(streamdsl.rpc.Either.Value.Left(ByteString.copyFrom(cl.encode(l))))
				case Right(r) => streamdsl.rpc.Either(streamdsl.rpc.Either.Value.Right(ByteString.copyFrom(cr.encode(r))))
			}
			bytestr.toByteArray
		}
	}

	implicit val forUnit: Codec[Unit] = new Codec {
		def decode(bytes: Bytes): Try[Unit] = Success(())
		def encode(a: Unit): Bytes = Array.empty[Byte]
	}

	implicit val forInt: Codec[Int] = new Codec {
		def decode(bytes: Bytes): Try[Int] = Try(ByteBuffer.wrap(bytes).getInt())
		def encode(a: Int): Bytes = viaByteBuffer { b => b.putInt(a); () }
	}

	implicit val forLong: Codec[Long] = new Codec {
		def decode(bytes: Bytes): Try[Long] = Try(ByteBuffer.wrap(bytes).getLong())
		def encode(a: Long): Bytes = viaByteBuffer { b => b.putLong(a); () }
	}

	implicit val forBytes: Codec[Bytes] = new Codec {
		def decode(bytes: Bytes) = Success(bytes)
		def encode(bytes: Bytes) = bytes
	}

	implicit def forSet[A](implicit c: Codec[A]): Codec[Set[A]] = new Codec {
		def decode(bytes: Bytes) = forList[A].decode(bytes).map(_.toSet)
		def encode(a: Set[A]): Bytes = forList[A].encode(a.toList)
	}

	implicit def forList[A](implicit c: Codec[A]): Codec[List[A]] = new Codec {
		def decode(bytes: Bytes) =
			Try(streamdsl.rpc.RepeatedBytes.parseFrom(bytes)).flatMap(_.value.toList.traverse(b => c.decode(b.toByteArray())))

		def encode(a: List[A]): Bytes =
			streamdsl.rpc.RepeatedBytes.of(a.map(i => ByteString.copyFrom(c.encode(i)))).toByteArray
	}

	implicit def forOption[A](implicit c: Codec[A]): Codec[Option[A]] = new Codec[Option[A]] {
		// TODO: this fails on Option[Unit] or other zero-length type
		def encode(a: Option[A]) = a.fold(Array.empty[Byte])(c.encode)
		def decode(bytes: Bytes) = {
			if (bytes == null || bytes.isEmpty) {
				Success(None)
			} else {
				c.decode(bytes).map(Some.apply)
			}
		}
	}

	implicit def forTuple[A, B](implicit A: Codec[A], B: Codec[B]): Codec[(A,B)] = new Codec {
		def encode(t: (A, B)): Bytes =
			forList[Bytes].encode(List(A.encode(t._1), B.encode(t._2)))
		
		def decode(bytes: Bytes): Try[(A, B)] = {
			forList[Bytes].decode(bytes).flatMap {
				case List(ab, bb) => for {
					a <- A.decode(ab)
					b <- B.decode(bb)
				} yield (a,b)
				case other => Failure(err(s"Expected 2 items, got ${other.size}"))
			}
		}
	}

	implicit def forProtobufMessage[M <: GeneratedMessage](implicit C: GeneratedMessageCompanion[M]): Codec[M] = new Codec {
		def decode(bytes: Bytes): Try[M] = Try(C.parseFrom(bytes))
		def encode(a: M): Bytes = a.toByteArray
	}

	private def viaByteBuffer(fn: ByteBuffer => Unit, sizeHint: Int = 64): Bytes = {
		val b = ByteBuffer.allocate(sizeHint)
		fn(b)
		b.flip()
		b.array()
	}

}
