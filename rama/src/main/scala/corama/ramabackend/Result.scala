package corama

import cats.Monoid

// Utilities for Either[String, T]
type Result[T] = Either[String, T]
object Result {
	type Void = Either[String,Unit]

	def empty[T : Monoid]: Either[String,T] = Right(Monoid[T].empty)
	
	def failure[T](s: String): Either[String,T] = Left(s)

	def success[T](v: T): Either[String,T] = Right(v)

	def unit: Either[String,Unit] = Right(())

	def agg[T](implicit M: Monoid[T]): Agg[Either[String,T]] =
		Agg(Right(M.empty), { (a,b) =>
			a.flatMap(a => b.map(M.combine(a, _)))
		})
}
