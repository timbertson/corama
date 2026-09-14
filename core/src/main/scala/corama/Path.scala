package corama

import cats.data.NonEmptyList
import cats.syntax.all.*
import izumi.reflect.Tag

// TODO: how can we encode path components? They can be arbitrary
// binary, and we need to distinguish e.g. [ab, cd] from [a, bc, d]
// consider https://bford.info/draft/cbe/ or escape255
class PathComponent[A](val value: MaybeVar[?]) {
	override def toString(): String = s"(${value.toString()}: ${value.tag.tag.repr})"
}

object PathComponent {
	def apply[A](value: MaybeVar[A]) = new PathComponent(value)
}

case class Path private(private[corama] raw: NonEmptyList[PathComponent[?]]) {
	def /[A](v: A)(implicit I: IntoVar[A], tag: Tag[I.T]): Path = copy(raw.append(
		new PathComponent[I.T](I.intoVar(v))
	))
}

object Path {
	def apply[A](value: A)(implicit I: IntoVar[A]) =
		new Path(NonEmptyList.one(PathComponent(I.intoVar(value))))
}

object CompiledPath {
	sealed trait Component
	case class Literal(inner: Any) extends Component
	case class Variable(id: VarId) extends Component
	
	// strip down to minimum information needed for serialization,
	// in particular keeping only the varId for vars
	def from(path: Path): CompiledPath = {
		CompiledPath(path.raw.map { part =>
			part.value match {
				case lit: Var.Literal[?] => Literal(lit.value)
				case v: Var[?] => Variable(v.vid)
			}
		})
	}
}

private [corama] case class CompiledPath(path: NonEmptyList[CompiledPath.Component]) {
	import CompiledPath._
	def ins: List[VarId] = path.toList.mapFilter {
		case Literal(inner) => None
		case Variable(id) => Some(id)
	}
}

