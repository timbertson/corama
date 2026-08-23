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

// A lens into some arbitrary path of a pstate, containing the given leaf schema
sealed trait Lens[A]

case class ValueLens[A](pstate: PState, path: Path, schema: Schema.ClassSchema[A]) extends Lens[A] {
	def get: Flow[Option[A], Any] = Flow.Get(this)
	def getOrElse(dfl: A): Flow[A, Any] = Flow.Get(this).flatMap(v => v.map(_.getOrElse(dfl)))
	def put(value: Var[A]): Flow[Unit, Etl] = Flow.Put(this, value)
	def delete(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Update(pos, this, Nil, (_, _) => None)
	def update(f: Option[A] => Option[A])(implicit pos: SourcePos): Flow[Unit, Etl] = ???
	def updateWith[B](b: Var[B])(f: (Option[A], B) => Option[A])(implicit pos: SourcePos): Flow[Unit, Etl] = ???
}

case class SetLens[A](pstate: PState, path: Path, schema: Schema.SetSchema[A]) extends Lens[A] {
	def add(value: Var[A]): Flow[Unit, Etl] = Flow.Add(this, value)
	def remove(value: Var[A])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Remove(this, value)
	def selectAll(implicit pos: SourcePos): Flow[List[A], Any] = Flow.Select(this)
}

// object PState {

// 	// class Update[K, V](pstate: PState[K,V], key: Var[K]) {
// 	// 	def apply(f: Option[V] => Option[V])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.Update(pos, pstate, key, f)
// 	// 	def zip[A](arg: Var[A])(f: (Option[V], A) => Option[V])(implicit pos: SourcePos): Flow[Unit, Etl] = Flow.UpdateWith(pos, pstate, key, arg, f)
// 	// }
// 	//
// }

// Pstate with nested schemas. Not fully type safe, validity of paths is verified at runtime
final case class PState(id: Identity, schema: Schema) {
	private def checkType(value: PathComponent[?], schema: Schema): Either[String, Unit] = ???

	private def matchingClass[A](schema: Schema.ClassSchema[?], tag: Tag[?]): Either[String, Schema.ClassSchema[A]] = ???

	private def matchingSet[A](schema: Schema.SetSchema[?], tag: Tag[?]): Either[String, Schema.SetSchema[A]] = ???

	// TODO: should this throw?
	def valueAt[A](path: Path)(implicit tag: Tag[A]): ValueLens[A] = {
		access(path, tag).getOrElse((m: String) => throw new RuntimeException(m)).asInstanceOf[ValueLens[A]]
	}

	def setAt[A](path: Path)(implicit tag: Tag[A]): SetLens[A] = {
		access(path, tag).getOrElse((m: String) => throw new RuntimeException(m)).asInstanceOf[SetLens[A]]
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
							case Var.Literal(key) => Right(key.asInstanceOf[String])
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
	private var internal: PState = PState(id, Schema.map(Schema[K], Schema[V]))
	def apply(k: Var[K]) = internal.valueAt[V](Path(k)(using IntoVar.varIntoVar[K]))
}

final case class PStateKVSet[K: Tag, V: Tag](id: Identity, subindex: Subindex) {
	private var internal: PState = PState(id, Schema.map(Schema[K], Schema.set[V](subindex)))
	def apply(k: Var[K]) = internal.setAt[V](Path(k)(using IntoVar.varIntoVar[K]))
}
