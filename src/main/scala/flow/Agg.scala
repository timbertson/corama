package flow

import cats.{Monoid, Semigroup}
import cats.syntax.all._
import cats.Functor
import cats.Applicative
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import cats.data.NonEmptyList

final class Agg[A] private[flow] (
	private[flow] val zero: A,
	private[flow] val combine: (A, A) => A,
) {
	private[flow] def merge(values: List[A]): A = values.foldLeft(zero)(combine)
	private[flow] def mergeAny(values: List[Any]): A = merge(values.asInstanceOf[List[A]])
	private[flow] def combineAny(a: Any, b: Any): A = combine(a.asInstanceOf[A], b.asInstanceOf[A])
}

object Agg {
	def apply[A](zero: A, combine: (A, A) => A): Agg[A] = new Agg(zero, combine)

	// Collect any monoid
	def collect[A, M[_]](using app: Applicative[M], m: Monoid[M[A]]): Agg[M[A]] = {
		new Agg[M[A]](m.empty, m.combine)
	}

	// convenience overload of collect to hardcode the List type
	def list[A]: Agg[List[A]] = collect

	// return nothing, accumulate nothing
	def unit: Agg[Unit] = apply((), (_, _) => ())

	def error[E]: Agg[Either[E, Unit]] = apply(Right(()), { (curr, next) =>
		curr.flatMap(_ => next)
	})

	def either[E, T](initial: T): Agg[Either[E, T]] = apply(Right(initial), { (curr, next) =>
		curr.flatMap(_ => next)
	})
}
