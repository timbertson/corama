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
	private [corama] val NULL_BYTE = 0.byteValue()

	private [corama] val MAX_BYTE = ~NULL_BYTE
	
	def eq(a: Bytes, b: Bytes) = Arrays.equals(a, b)

	implicit val eqInstance: Eq[Bytes] = Eq.instance(eq)

	implicit val orderInstance: Order[Bytes] = Order.from { (a,b) =>
		Arrays.compare(a, b)
	}

	implicit val orderingInstance: Ordering[Bytes] = Order.catsKernelOrderingForOrder[Bytes](orderInstance)
}

trait Decoder[A] { self =>
	def decode(bytes: Bytes): Try[A]
}

object Decoder {
	// this one's explicit, otherwise we may pick up unintended instances
	def json[T](implicit cd: circe.Decoder[T]): Decoder[T] =
		new Decoder[T] {
			def decode(bytes: Bytes): Try[T] = {
				Try(new String(bytes, StandardCharsets.UTF_8)).flatMap { str =>
					(for {
						json <- circe.parser.parse(str).toTry
						t <- cd.decodeJson(json).toTry
					} yield t).adaptErr(cause => new RuntimeException(s"Failed to decode JSON: ${str}", cause))
				}
			}
		}

	implicit def forString: Decoder[String] = bytes => Try(new String(bytes, StandardCharsets.UTF_8))

	implicit def forOption[A](implicit d: Decoder[A]): Decoder[Option[A]] = { bytes =>
		if (bytes == null || bytes.isEmpty) {
			Success(None)
		} else {
			d.decode(bytes).map(Some.apply)
		}
	}

	implicit def forEither[L, R](implicit dl: Decoder[L], dr: Decoder[R]): Decoder[Either[L, R]] = { bytes =>
		Try(streamdsl.rpc.Either.parseFrom(bytes)).flatMap { parsed =>
			parsed.value match {
				case Empty => Failure(err("Either is empty"))
				case streamdsl.rpc.Either.Value.Left(l) => dl.decode(l.toByteArray()).map(Left.apply)
				case streamdsl.rpc.Either.Value.Right(r) => dr.decode(r.toByteArray()).map(Right.apply)
			}
		}
	}

	implicit def forSet[A](implicit d: Decoder[A]): Decoder[Set[A]] = bytes => forList[A].decode(bytes).map(_.toSet)

	implicit def forList[A](implicit d: Decoder[A]): Decoder[List[A]] = b =>
		Try(streamdsl.rpc.RepeatedBytes.parseFrom(b)).flatMap(_.value.toList.traverse(b => d.decode(b.toByteArray())))

	implicit def forUnit: Decoder[Unit] = b => Success(())

	implicit def forInt: Decoder[Int] = b => Try(ByteBuffer.wrap(b).getInt())

	implicit def forLong: Decoder[Long] = b => Try(ByteBuffer.wrap(b).getLong())

	implicit def forBytes: Decoder[Bytes] = (bytes) => Success(bytes)

	implicit def forTuple[A, B](implicit A: Decoder[A], B: Decoder[B]): Decoder[(A,B)] =
		(bytes) => forList[Bytes].decode(bytes).flatMap {
			case List(ab, bb) => for {
				a <- A.decode(ab)
				b <- B.decode(bb)
			} yield (a,b)
			case other => Failure(err(s"Expected 2 items, got ${other.size}"))
		}

	implicit def forProtobufMessage[M <: GeneratedMessage](implicit C: GeneratedMessageCompanion[M]): Decoder[M] =
		(bytes) => Try(C.parseFrom(bytes))
}

trait Encoder[A] { self =>
	def encode(a: A): Bytes
}

object Encoder {
	def json[T](implicit cd: circe.Encoder[T]): Encoder[T] =
		import io.circe.syntax.*
		new Encoder[T] {
			def encode(t: T): Bytes = t.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
		}

	implicit def forBytes: Encoder[Bytes] = bytes => bytes

	implicit def forOption[A](implicit e: Encoder[A]): Encoder[Option[A]] = a =>
		a.fold(Array.empty[Byte])(e.encode)

	implicit def forEither[L, R](implicit el: Encoder[L], er: Encoder[R]): Encoder[Either[L, R]] = { v =>
		(v match {
			case Left(l) => streamdsl.rpc.Either(streamdsl.rpc.Either.Value.Left(ByteString.copyFrom(el.encode(l))))
			case Right(r) => streamdsl.rpc.Either(streamdsl.rpc.Either.Value.Right(ByteString.copyFrom(er.encode(r))))
		}).toByteArray
	}

	implicit def forSet[A](implicit e: Encoder[A]): Encoder[Set[A]] = a => forList[A].encode(a.toList)

	implicit def forList[A](implicit e: Encoder[A]): Encoder[List[A]] = a =>
		streamdsl.rpc.RepeatedBytes.of(a.map(i => ByteString.copyFrom(e.encode(i)))).toByteArray

	implicit def forTuple[A, B](implicit A: Encoder[A], B: Encoder[B]): Encoder[(A,B)] = t =>
		forList[Bytes].encode(List(A.encode(t._1), B.encode(t._2)))

	implicit def forString: Encoder[String] = a => a.getBytes(StandardCharsets.UTF_8)

	private def viaByteBuffer(fn: ByteBuffer => Unit, sizeHint: Int = 64): Bytes = {
		val b = ByteBuffer.allocate(sizeHint)
		fn(b)
		b.flip()
		b.array()
	}

	implicit def forInt: Encoder[Int] = i => viaByteBuffer { b => b.putInt(i); () }

	implicit def forLong: Encoder[Long] = i => viaByteBuffer { b => b.putLong(i); () }

	implicit def forUnit: Encoder[Unit] = _ => Array.empty
	
	implicit def forProtobufMessage[M <: GeneratedMessage]: Encoder[M] = _.toByteArray
}

