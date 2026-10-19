package corama

import izumi.reflect.Tag

sealed trait Subindex
object Subindex {
	private[corama] case object NonIndexed extends Subindex
	private[corama] case class Indexed(sizeTracking: Boolean) extends Subindex
	val default = NonIndexed
	val nonIndexed = NonIndexed
	val indexed = Indexed(sizeTracking = true)
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
	case class ClassSchema[A] private[corama](codec: Codec[A], tag: Tag[A], optionTag: Tag[Option[A]]) extends LeafSchema[A]
	case class FixedKeySchema private[corama](shape: Map[String, Schema]) extends Schema

	// sub-indexable shapes
	case class MapSchema[A] private[corama](k: ClassSchema[A], v: Schema, index: Subindex) extends IndexableSchema
	case class SetSchema[A] private[corama](v: ClassSchema[A], index: Subindex, itemTag: Tag[A], codec: Codec[A], listTag: Tag[List[A]], ord: Ordering[A]) extends IndexableSchema with LeafSchema[A]
	
	def apply[T](implicit codec: Codec[T], tag: Tag[T], optTag: Tag[Option[T]]) = ClassSchema(codec, tag, optTag)
	val string = apply[String]
	def fixed(pairs: (String, Schema)*) = FixedKeySchema(pairs.toMap)

	def mapOf[K](v: Schema, subindex: Subindex)(implicit kCodec: Codec[K], kTag: Tag[K], kTagOpt: Tag[Option[K]]) = MapSchema(ClassSchema(kCodec, kTag, kTagOpt), v, subindex)
	def set[A](subindex: Subindex)(implicit atag: Tag[A], codec: Codec[A], listTag: Tag[List[A]], ord: Ordering[A]) =
		SetSchema[A](apply[A], subindex, atag, codec, listTag, ord)
}
