package corama

import cats.Applicative
import cats.Monoid
import cats.Semigroup
import cats.syntax.all.*
import cats.Eq

final class Agg[A: Eq] private[corama] (
	private[corama] val zero: A,
	private[corama] val combine: (A, A) => A,
) {
	private[corama] val eq = Eq[A]
	private[corama] def merge(values: List[A]): A = values.foldLeft(zero)(combine)
	private[corama] def mergeAny(values: List[Any]): A = merge(values.asInstanceOf[List[A]])
	private[corama] def combineAny(a: Any, b: Any): A = combine(a.asInstanceOf[A], b.asInstanceOf[A])
}

object Agg {
	def apply[A: Eq](zero: A, combine: (A, A) => A): Agg[A] = new Agg(zero, combine)

	// Collect any monoid
	def collect[A, M[_]](using app: Applicative[M], m: Monoid[M[A]], eq: Eq[M[A]]): Agg[M[A]] = {
		new Agg[M[A]](m.empty, m.combine)
	}

	// convenience overload of collect to hardcode the List type
	def list[A: Eq]: Agg[List[A]] = collect

	// return nothing, accumulate nothing
	def unit: Agg[Unit] = apply((), (_, _) => ())

	def error[E: Eq]: Agg[Either[E, Unit]] = apply(Right(()), { (curr, next) =>
		curr.flatMap(_ => next)
	})

	def either[E: Eq, T: Eq](initial: T): Agg[Either[E, T]] = apply(Right(initial), { (curr, next) =>
		curr.flatMap(_ => next)
	})
}
