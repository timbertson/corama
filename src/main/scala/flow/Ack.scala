package flow

import cats.{Monoid, Semigroup}
import cats.syntax.all._
import cats.Functor
import cats.Applicative
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import cats.data.NonEmptyList

sealed trait AckLevel[R]

object AckLevel {

	// Fire and forget, data has been accepted but not durably (not yet replicated)
	case object NoAck extends AckLevel[Unit]

	// Return once the data has been replicated
	case object AppendAck extends AckLevel[Unit]

	// Wait for all associated topologies to return
	case object Ack extends AckLevel[AckReturns]
}

final class Agg[A] private[flow] (
	private[flow] val zero: A,
	private[flow] val combine: (A, A) => A
) {
	private[flow] def merge(values: List[A]): A = values.foldLeft(zero)(combine)
	private[flow] def mergeAny(values: List[Any]): A = merge(values.asInstanceOf[List[A]])
	private[flow] def combineAny(a: Any, b: Any): A = combine(a.asInstanceOf[A], b.asInstanceOf[A])
}

object Agg {
	def apply[A](zero: A, combine: (A, A) => A): Agg[A] = new Agg(zero, combine)

	// Collect act returns into a sequence (monoid)
	def collect[A, M[_]](using app: Applicative[M], m: Monoid[M[A]]): Agg[M[A]] = {
		new Agg[M[A]](m.empty, m.combine)
	}

	// convenience overload of collect to hardcode the List type
	def list[A]: Agg[List[A]] = collect

	// return nothing, accumulate nothing
	def unit: Agg[Unit] = apply((), (_, _) => ())
}

/** The ack returns from one depot append, keyed by the topology that produced each.
  *
  * Values arrive as a list per topology. TODO: could these be pre-combined with agg?
  */
final class AckReturns private[flow] (
	private val values: Map[TopologyId, List[Any]],
	private val topologies: Topologies) {

	// TODO: assert topology is a Stream, not a Microbatch
	def result[S, R](t: Topology[S, R]): R = {
		// TODO: make this impossible via types?
		require(
			t.builder.source.namespace == IdentityNamespace.Depot,
			s"${t.builder.source} is not a stream topology, so it cannot produce an ack return")
		require(
			topologies.declared(t),
			s"${t.builder.source} was not declared in the module this append went to")

		values.getOrElse(t.builder.id, Nil).foldLeft(t.agg.zero)(t.agg.combineAny)
	}

	def unique[S, T, M <: Seq[T]](t: Topology[S, M]): Try[T] = {
		val seq = result[S, M](t)
		if (seq.length == 1) {
			Success(seq.head)
		} else {
			Failure(new RuntimeException(s"Expected ${t.builder.source} to produce 1 element, but received ${seq.length}"))
		}
	}

	override def toString: String = s"AckReturns(${values.size})"
}
