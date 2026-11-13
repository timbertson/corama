package corama

import cats.Applicative
import cats.Monoid
import cats.Semigroup
import cats.syntax.all.*
import cats.Eq

final class Agg[A: Codec] private[corama] (
	private[corama] val zero: A,
	private[corama] val combine: (A, A) => A,
) {
	private[corama] val codec = Codec[A]
	private[corama] def merge(values: List[A]): A = values.foldLeft(zero)(combine)
	private[corama] def mergeAny(values: List[Any]): A = merge(values.asInstanceOf[List[A]])
	private[corama] def combineAny(a: Any, b: Any): A = combine(a.asInstanceOf[A], b.asInstanceOf[A])
	private[corama] def asAny: Agg[Any] = this.asInstanceOf[Agg[Any]]
}

object Agg {
	def apply[A: Codec](zero: A, combine: (A, A) => A): Agg[A] = new Agg(zero, combine)

	// Collect any monoid
	def collect[A, M[_]](using app: Applicative[M], m: Monoid[M[A]], codec: Codec[M[A]]): Agg[M[A]] = {
		new Agg[M[A]](m.empty, m.combine)
	}

	// convenience overload of collect to hardcode the List type
	def list[A: Codec]: Agg[List[A]] = collect

	// return nothing, accumulate nothing
	def unit: Agg[Unit] = apply((), (_, _) => ())
	
	// accumulate zero or more booleans, return true if any are true
	def any: Agg[Boolean] = apply(false, _ || _)

	// accumulate zero or more booleans, return false if any are false
	def all: Agg[Boolean] = apply(true, _ && _)

	def either[E: Codec, T: Eq : Codec](initial: T): Agg[Either[E, T]] = apply(Right(initial), { (curr, next) =>
		curr.flatMap(_ => next)
	})
}
