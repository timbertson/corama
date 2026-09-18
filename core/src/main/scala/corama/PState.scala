package corama

import cats.data.NonEmptyList
import cats.syntax.all.*
import izumi.reflect.Tag
import org.tpolecat.sourcepos.*

// A lens into some arbitrary path of a pstate, containing the given leaf schema
sealed trait Lens[A]

case class ValueLens[A](pstate: PState, path: Path, schema: Schema.ClassSchema[A]) extends Lens[A] {
	private [corama] def tag: Tag[A] = schema.tag
	def get: Flow[Option[A], Any] = Flow.Get(this)
	def getOrElse(dfl: A): Flow[A, Any] = Flow.Get(this).flatMap(v => v.map(_.getOrElse(dfl))(using implicitly, tag))
	def put(value: Var[A]): Flow[Unit, Etl] = Flow.Put(this, value)
	def delete(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Update(pos, this, Nil, (_, _) => None)
	def update(f: Option[A] => Option[A])(implicit pos: SourcePos): Flow[Unit, Etl] = ???
	def updateWith[B](b: Var[B])(f: (Option[A], B) => Option[A])(implicit pos: SourcePos): Flow[Unit, Etl] =
		Flow.Update(pos, this, List(b.asAny), { (a, args) =>
			args match {
				case List(b) => f(a.asInstanceOf[Option[A]], b.asInstanceOf[B])
				case other => impossible(s"update with ${other.length}")
			}
		})
}

case class SetLens[A](pstate: PState, path: Path, schema: Schema.SetSchema[A]) extends Lens[A] {
	def add(value: Var[A]): Flow[Unit, Etl] = Flow.Add(this, value)
	def remove(value: Var[A])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Remove(this, value)
	def selectAll(implicit pos: SourcePos): Flow[List[A], Any] = Flow.Select(this, SelectOptions.all[A](using schema.ord))
	def selectRange(afterItem: Var[Option[A]], maxItems: Int)(implicit pos: SourcePos): Flow[List[A], Any] =
		Flow.Select(this, SelectOptions(Some(afterItem), maxItems = Some(maxItems), schema.ord))
}

// object PState {

// 	// class Update[K, V](pstate: PState[K,V], key: Var[K]) {
// 	// 	def apply(f: Option[V] => Option[V])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Update(pos, pstate, key, f)
// 	// 	def zip[A](arg: Var[A])(f: (Option[V], A) => Option[V])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.UpdateWith(pos, pstate, key, arg, f)
// 	// }
// 	//
// }

case class PStateId(value: String)

// Pstate with nested schemas. Not fully type safe, validity of paths is verified at runtime
final case class PState(id: Identity, schema: Schema) {
	println(s"created pstate ${id.name} with schema $schema")
	private def checkType(value: PathComponent[?], schema: Schema): Either[String, Unit] =
		def requireValueOf(schemaTag: Tag[?]) = {
			if (value.value.tag =:= schemaTag) {
				Right(())
			} else {
				Left(s"Incompatible path component (${value}) for schema (${schema}) on pstate ${id.name}")
			}
		}
		schema match {
			case Schema.ClassSchema(tag) => requireValueOf(tag)
			case _: Schema.FixedKeySchema => requireValueOf(Schema.string.tag)
			case map: Schema.MapSchema[?] => requireValueOf(map.k.tag)
			case set: Schema.SetSchema[?] => Left(s"Path component ${value} attempted to enter a set schema")
		}

	private def matchingClass[A](schema: Schema.ClassSchema[?], tag: Tag[?]): Either[String, Schema.ClassSchema[A]] =
		if (schema.tag =:= tag) {
			Right(schema.asInstanceOf[Schema.ClassSchema[A]])
		} else {
			Left(s"Incompatible class schema: ${schema.tag} != ${tag}")
		}

	private def matchingSet[A](schema: Schema.SetSchema[?], tag: Tag[?]): Either[String, Schema.SetSchema[A]] =
		if (schema.v.tag =:= tag) {
			Right(schema.asInstanceOf[Schema.SetSchema[A]])
		} else {
			Left(s"Incompatible set schema: ${schema.v.tag} != ${tag}")
		}

	private def accessMismatch(path: Path)(msg: String) = {
		throw new RuntimeException(s"Error resolving path: ${path}:\n$msg")
	}

	// TODO: should this throw?
	def valueAt[A](path: Path)(implicit tag: Tag[A]): ValueLens[A] = {
		access(path, tag).fold(accessMismatch(path), identity).asInstanceOf[ValueLens[A]]
	}

	def setAt[A](path: Path)(implicit tag: Tag[A]): SetLens[A] = {
		// TODO: validate set-ness
		access(path, tag).fold(accessMismatch(path), identity).asInstanceOf[SetLens[A]]
	}

	private def access[A](rootPath: Path, expected: Tag[A]): Either[String, Lens[A]] = {
		val rootSchema = this.schema
		def zip(path: NonEmptyList[PathComponent[?]], schema: Schema): Either[String, Lens[A]] = {
			def continue(schema: Schema): Either[String, Lens[A]] = {
				NonEmptyList.fromList(path.tail).fold {
					// we've hit a leaf. We can deal with classes and sets
					schema match {
						case cls: Schema.ClassSchema[?] => matchingClass[A](cls, expected).flatMap { schema => Right(ValueLens[A](this, rootPath, schema)) }
						case set: Schema.SetSchema[?] => matchingSet[A](set, expected).flatMap { schema => Right(SetLens[A](this, rootPath, schema)) }
						case other => Left(s"Found incompatible schema at leaf path ${path}: ${other} (leaf schema must be Class or Set)")
					}
				} { tail =>
					zip(tail, schema)
				}
			}
			schema match {
				case Schema.MapSchema(k: Schema, v: Schema, _) => checkType(path.head, k) *> continue(v)
				case Schema.FixedKeySchema(shape: Map[String, Schema]) =>
					for {
						_ <- checkType(path.head, Schema.string)
						// reject dynamic path components for fixed schemas
						key <- path.head.value match {
							case lit: Var.Literal[?] => Right(lit.value.asInstanceOf[String])
							case _: Var[?] => Left(s"Path component within a FixedKeySchema must be a literal: ${path}")
						}
						valueSchema <- shape.get(key).toRight(s"${path.head} not found in fixed keys: ${shape.keys}")
						result <- continue(valueSchema)
					} yield result

				case _: LeafSchema[?] => Left(s"Path attempted to enter leaf schema: ${path}")
			}
		}
		zip(rootPath.raw, rootSchema)
	}
}

// A convenience API with additional type safety for simple K/V PStates
final case class PStateKV[K: Tag, V: Tag](id: Identity) {
	private [corama] var inner: PState = PState(id, Schema.mapOf[K](Schema[V], Subindex.nonIndexed))
	def key(k: Var[K]) = inner.valueAt[V](Path(k)(using IntoVar.varIntoVar[K]))
}

final case class PStateKVSet[K: Tag, V: Tag : Ordering](id: Identity, subindex: Subindex) {
	private [corama] var inner: PState = PState(id, Schema.mapOf[K](Schema.set[V](subindex), Subindex.nonIndexed))
	def key(k: Var[K]) = inner.setAt[V](Path(k)(using IntoVar.varIntoVar[K]))
}

final case class SelectOptions[A](after: Option[Var[Option[A]]], maxItems: Option[Int], ord: Ordering[A])
object SelectOptions {
	def all[A](implicit ord: Ordering[A]): SelectOptions[A] = SelectOptions(None, None, ord)
}
