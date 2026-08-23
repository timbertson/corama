package flow

import cats.syntax.all._
import cats.data.NonEmptyList
import org.tpolecat.sourcepos._
import cats.Functor
import cats.Applicative
import scala.annotation.unused
import cats.Eq
import com.rpl.rama.RamaSerializable
import cats.Monoid
import izumi.reflect.Tag

import flow.internal._

// TODO: how can we encode path components? They can be arbitrary
// binary, and we need to distinguish e.g. [ab, cd] from [a, bc, d]
// consider https://bford.info/draft/cbe/ or escape255
class PathComponent[A](val value: MaybeVar[?], val tag: Tag[A]) {
	override def toString(): String = value.toString()
}

object PathComponent {
	def apply[A](value: MaybeVar[A])(implicit tag: Tag[A]) = new PathComponent(value, tag)
}

case class Path private(private[flow] raw: NonEmptyList[PathComponent[?]]) {
	def /[A](v: A)(implicit I: IntoVar[A]): Path = copy(raw.append(
		PathComponent[I.T](I.intoVar(v))
	))
}

object Path {
	def apply[A](value: A)(implicit I: IntoVar[A]) =
		new Path(NonEmptyList.one(PathComponent(I.intoVar(value))))
}

object CompiledPath {
	sealed trait Component
	case class Literal(inner: Any) extends Component
	case class Var(id: VarId) extends Component
}

private [flow] case class CompiledPath(path: NonEmptyList[CompiledPath.Component]) {
	def ins: List[VarId] = ???
}

