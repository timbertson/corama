package flow

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid

// Allocates var ids within one topology.
private[flow] class VarRegistry {
	private var defined = 0

	def add[A](pos: SourcePos, hint: String): Var[A] = {
		val v = new Var[A](new VarId(defined, hint), pos)
		defined += 1
		println(s"Added var: $v")
		v
	}

	def count: Int = defined
}

private[flow] final class VarId(private[flow] val id: Int, private[flow] val hint: String) {
	override def equals(other: Any): Boolean = other match {
		case that: VarId => that.id == id
		case _           => false
	}
	override def hashCode: Int = id
	override def toString: String = s"$hint#$id"
}

object VarId {
	implicit val eq: Eq[VarId] = Eq.fromUniversalEquals
}

final class Var[A] private[flow] (private[flow] val vid: VarId, pos: SourcePos) {
	private def asAny: Var[Any] = this.asInstanceOf[Var[Any]]

	def map: Var.Map[A] = new Var.Map(this)

	// TODO this could probably be optimised
	def mapFilter[B](f: A => Option[B])(implicit pos: SourcePos): Flow[B, Any] = for {
		opt <- this.map(f)
		_ <- opt.filter(_.isDefined)
		result <- opt.map(_.get)
	} yield result

	def foreach(f: A => Unit)(implicit pos: SourcePos) = map[Unit](f)

	def into[F[_]](using f: Applicative[F])(implicit pos: SourcePos): Flow[F[A], Any] = this.map(f.pure)

	/** Fan out: emit one value per element, running everything downstream per element. */
	def explode[B](f: A => Seq[B])(implicit pos: SourcePos): Flow[B, Any] = Flow.ExplodeOp(pos, this, f)

	def filter: Var.Filter[A] = new Var.Filter(this)

	def void: Flow[Unit, Any] = Flow.Pure(Var.unit)

	override def toString: String = s"Var($vid, $pos)"
	override def equals(other: Any): Boolean = other match {
		case that: Var[?] => that.vid == vid
		case _            => false
	}
	override def hashCode: Int = vid.hashCode
}

private def impossible(reason: String) = throw new IllegalStateException(reason)

object Var {
	// Virtual vars have a unique id < 0 and are never given a Rama name
	val unit: Var[Unit] = new Var[Unit](new VarId(-2, "unit"), implicitly[SourcePos])

	private [flow] val internal = SourcePos("internal", 1)

	extension [A](v: Var[Option[A]]) {
		def fold[B, C](ifNone: Flow[B, C])(ifSome: Var[A] => Flow[B, C]): Flow[B, C] =
			Flow.If[B, Any, C, C](
				v.map(_.isEmpty),
				ifNone,
				v.map(_.get).flatMap(ifSome)
			)
	}

	class Map[A](v: Var[A]) {
		def apply[B](f: A => B)(implicit pos: SourcePos): Flow[B, Any] = Flow.MapN(pos, List(v.asAny), {
			case List(v) => f(v.asInstanceOf[A])
			case other => impossible(s"map1 with ${other.length}")
		})

		def zip[B, C](b: Var[B])(f: (A, B) => C)(implicit pos: SourcePos): Flow[C, Any] = Flow.MapN(pos, List(v.asAny, b.asAny), {
			case List(a, b) => f(a.asInstanceOf[A], b.asInstanceOf[B])
			case other => impossible(s"map2 with ${other.length}")
		})

		def zip[B, C, D](b: Var[B], c: Var[C])(f: (A, B, C) => D)(implicit pos: SourcePos): Flow[D, Any] = Flow.MapN(pos, List(v.asAny, b.asAny, c.asAny), {
			case List(a, b, c) => f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C])
			case other => impossible(s"map3 with ${other.length}")
		})
	}

	class Filter[A](v: Var[A]) {
		def apply(p: A => Boolean)(implicit pos: SourcePos): Flow[A, Any] = Flow.FilterOp(pos, v, p).as(v)

		def zip[B](b: Var[B])(p: (A, B) => Boolean)(implicit pos: SourcePos): Flow[A, Any] = for {
			bool <- v.map.zip(b)(p)
			_ <- bool.filter(identity)
		} yield v
	}
}
