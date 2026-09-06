package flow

import cats.data.NonEmptyList
import izumi.reflect.Tag
import com.rpl.rama.impl.SubSourceImpl

sealed trait Subindex
object Subindex {
	private[flow] case object NonIndexed extends Subindex
	private[flow] case class Indexed(sizeTracking: Boolean) extends Subindex
	val default = NonIndexed
	val indexed = NonIndexed
	def indexedWith(sizeTracking: Boolean) = Indexed(sizeTracking)
}

sealed trait Schema
sealed trait LeafSchema[A] extends Schema
sealed trait IndexableSchema extends Schema {
	def index: Subindex
}

// A schema controls how data is laid out in a pstate. Most importantly,
// subindexed fields can be queried without loading the entire object/set/etc into
// memory
object Schema {
	// static shape
	case class ClassSchema[A] private[flow](tag: Tag[A]) extends LeafSchema[A]
	case class FixedKeySchema private[flow](shape: Map[String, Schema]) extends Schema

	// sub-indexable shapes
	case class MapSchema[A] private[flow](k: ClassSchema[A], v: Schema, index: Subindex) extends IndexableSchema
	case class SetSchema[A] private[flow](v: ClassSchema[A], index: Subindex, setTag: Tag[Set[A]], ord: Ordering[A]) extends IndexableSchema with LeafSchema[A]
	
	def apply[T](implicit tag: Tag[T]) = ClassSchema(tag)
	val string = apply[String]
	def fixed(pairs: (String, Schema)*) = FixedKeySchema(pairs.toMap)

	def mapOf[K](v: Schema, subindex: Subindex = Subindex.default)(implicit kTag: Tag[K]) = MapSchema(ClassSchema(kTag), v, subindex)
	def set[A](subindex: Subindex = Subindex.default)(implicit atag: Tag[A], setTag: Tag[Set[A]], ord: Ordering[A]) =
		SetSchema[A](apply[A](using atag), subindex, setTag, ord)
}
