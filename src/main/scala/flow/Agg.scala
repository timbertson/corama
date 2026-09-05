package flow

import cats.{Monoid, Semigroup}
import cats.syntax.all._
import cats.Functor
import cats.Applicative
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import cats.data.NonEmptyList

sealed trait Agg[A, B] {
	def map[C](fn: B => C): Aggregator[A, C]
}

// The simple combiner type, an aggregation where input / state / output are all the same type
final class Combiner[A] private[flow] (
	private[flow] val zero: A,
	private[flow] val combine: (A, A) => A,
) extends Agg[A, A] {
	private[flow] def merge(values: List[A]): A = values.foldLeft(zero)(combine)
	private[flow] def mergeAny(values: List[Any]): A = merge(values.asInstanceOf[List[A]])
	private[flow] def combineAny(a: Any, b: Any): A = combine(a.asInstanceOf[A], b.asInstanceOf[A])

	private[flow] def generic: Aggregator[A, A] = new Aggregator(this, identity)
	
	override def map[B](fn: A => B): Aggregator[A, B] = generic.map(fn)
}

// Full aggregation type, with `mapOut`
final class Aggregator[A, B] private[flow] (
	private[flow] val combiner: Combiner[A],
	private[flow] val mapOut: A => B
) extends Agg[A, B] {

	def map[C](fn: B => C): Aggregator[A, C] = new Aggregator(combiner, mapOut = x => fn(this.mapOut(x)))
}

object Agg {
	private[flow] def asAggregator[A, B](agg: Agg[A, B]): Aggregator[A, B] = agg match {
		case c: Combiner[?] => c.generic
		case a: Aggregator[?, ?] => a
	}

	def apply[A](zero: A, combine: (A, A) => A): Combiner[A] = new Combiner(zero, combine)

	// Collect any monoid
	def collect[A, M[_]](using app: Applicative[M], m: Monoid[M[A]]): Combiner[M[A]] = {
		new Combiner[M[A]](m.empty, m.combine)
	}

	// convenience overload of collect to hardcode the List type
	def list[A]: Combiner[List[A]] = collect

	// return nothing, accumulate nothing
	def unit: Combiner[Unit] = apply((), (_, _) => ())

	def error[E]: Combiner[Either[E, Unit]] = apply(Right(()), { (curr, next) =>
		curr.flatMap(_ => next)
	})

	def either[E, T](initial: T): Combiner[Either[E, T]] = apply(Right(initial), { (curr, next) =>
		curr.flatMap(_ => next)
	})
}
