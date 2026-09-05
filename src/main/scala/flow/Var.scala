package flow

import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid
import izumi.reflect.Tag

// Allocates var ids within one topology.
private[flow] class VarRegistry {
	private var defined = 0

	def add[A](hint: String, tag: Tag[A], pos: SourcePos): Var[A] = {
		val v = new Var[A](new VarId(defined, hint), tag, pos)
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

// Some parts of the API require either a literal or a var,
// in particular paths. This trait allows you to accept
// either a var or anything else, and you'll summon the
// appropriate instance.
// TODO: put a constraint on `serializable` for the any instance,
// since Var should never be serializable.
private [flow] sealed trait MaybeVar[A] {
	val tag: Tag[?]
}

trait IntoVar[A] {
	// type T will either be `A` or `Var[A]`
	type T
	def intoVar(value: A): MaybeVar[T]
}

trait DefaultIntoVar {
	implicit def anyIntoVar[A](implicit tag: Tag[A]): IntoVar[A] = new IntoVar[A] {
		type T = A
		def intoVar(value: A): MaybeVar[T] = Var.Literal(value, tag)
	}
}

object IntoVar extends DefaultIntoVar {
	implicit def varIntoVar[A]: IntoVar[Var[A]] = new IntoVar[Var[A]] {
		type T = A
		def intoVar(value: Var[A]): MaybeVar[T] = value
	}
}

final class Var[A] private[flow] (private[flow] val vid: VarId, val tag: Tag[A], pos: SourcePos) extends MaybeVar[A] {
	private [flow] def asAny: Var[Any] = this.asInstanceOf[Var[Any]]

	def map[B](f: A => B)(implicit pos: SourcePos, tag: Tag[B]): Flow[B, Any] = Flow.MapN(tag, pos, List(this.asAny), {
		case List(v) => f(v.asInstanceOf[A])
		case other => impossible(s"map with ${other.length}")
	})

	def mapWith[B, C](b: Var[B])(f: (A, B) => C)(implicit pos: SourcePos, tag: Tag[C]): Flow[C, Any] = Flow.MapN(tag, pos, List(this.asAny, b.asAny), {
		case List(a, b) => f(a.asInstanceOf[A], b.asInstanceOf[B])
		case other => impossible(s"mapWith[2] with ${other.length}")
	})

	def mapWith[B, C, D](b: Var[B], c: Var[C])(f: (A, B, C) => D)(implicit pos: SourcePos, tag: Tag[D]): Flow[D, Any] = Flow.MapN(tag, pos, List(this.asAny, b.asAny, c.asAny), {
		case List(a, b, c) => f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C])
		case other => impossible(s"mapWith[3] with ${other.length}")
	})

	def filter(p: A => Boolean)(implicit pos: SourcePos): Flow[A, Any] =
		Flow.FilterOp(pos, List(this.asAny), {
			case List(a) => p(a.asInstanceOf[A])
			case other => impossible(s"filter with ${other.length}")
		}).as(this)

	def filterWith[B](b: Var[B])(p: (A, B) => Boolean)(implicit pos: SourcePos): Flow[A, Any] =
		Flow.FilterOp(pos, List(this.asAny, b.asAny), {
			case List(a,b) => p(a.asInstanceOf[A],b.asInstanceOf[B])
			case other => impossible(s"filter[2] with ${other.length}")
		}).as(this)

	// TODO this could probably be optimised
	def mapFilter[B](f: A => Option[B])(implicit pos: SourcePos, tag: Tag[B]): Flow[B, Any] = for {
		opt <- this.map(f)
		_ <- opt.filter(_.isDefined)
		result <- opt.map(_.get)
	} yield result

	def foreach(f: A => Unit)(implicit pos: SourcePos) = map[Unit](f)

	def into[F[_]](using f: Applicative[F])(implicit pos: SourcePos, tag: Tag[F[A]]): Flow[F[A], Any] = this.map(f.pure)

	/** Fan out: emit one value per element, running everything downstream per element. */
	def explode[B](implicit pos: SourcePos, tag: Tag[B], ev: A <:< Seq[B]): Flow[B, Any] = explodeBy(identity)

	def explodeBy[B](f: A => Seq[B])(implicit pos: SourcePos, tag: Tag[B]): Flow[B, Any] = Flow.ExplodeOp(tag, pos, List(this.asAny), {
		case List(a) => {
			println(s"Exploding value: ${a}")
			f(a.asInstanceOf[A])
		}
		case other => impossible(s"explode with ${other.length}")
	})

	def void: Flow[Unit, Any] = Flow.Pure(Var.unit)

	override def toString: String = s"Var($vid @ $pos)"
	override def equals(other: Any): Boolean = other match {
		case that: Var[?] => that.vid == vid
		case _            => false
	}
	override def hashCode: Int = vid.hashCode
}

private def impossible(reason: String) = throw new IllegalStateException(reason)

object Var {
	case class Literal[A](value: A, tag: Tag[A]) extends MaybeVar[A]

	// Virtual vars have a unique id < 0 and are never given a Rama name
	val unit: Var[Unit] = new Var[Unit](new VarId(-2, "unit"), implicitly[Tag[Unit]], implicitly[SourcePos])

	private [flow] val internal = SourcePos("internal", 1)

	extension [A](v: Var[Option[A]]) {
		def fold[B, C](ifNone: Flow[B, C])(ifSome: Var[A] => Flow[B, C])(implicit taga: Tag[A], tagb: Tag[B]): Flow[B, C] = {
			Flow.If[B, Any, C, C](
				tagb,
				v.map(_.isEmpty),
				ifNone,
				v.map(_.get).flatMap(ifSome)
			)
		}
	}
}
