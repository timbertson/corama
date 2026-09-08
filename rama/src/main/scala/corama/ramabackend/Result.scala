package corama

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid

// Serializable wrapper type for Either[String, T]
case class Result[T](value: Either[String, T]) extends RamaSerializable
object Result {
	type Void = Result[Unit]
	def empty[T : Monoid]: Result[T] = Result(Right(Monoid[T].empty))
	
	def failure[T](s: String): Result[T] = Result[T](Left(s))

	def success[T](v: T): Result[T] = Result(Right(v))

	def unit: Result[Unit] = success[Unit](())

	def agg[T](implicit M: Monoid[T]): Agg[Result[T]] =
		Agg(Result(Right(M.empty)), { (a,b) =>
			Result(a.value.flatMap(a => b.value.map(M.combine(a, _))))
		})
}

