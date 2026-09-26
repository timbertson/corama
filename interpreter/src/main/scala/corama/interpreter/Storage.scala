package corama.interpreter

import cats.syntax.all.*
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe
import corama.*
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.SignallingRef

object Storage {
	class PState() {
		val inner = Ref.unsafe[IO, Map[Any,Any]](Map.empty)
		def modify[R](fullPath: NonEmptyList[Any])(fn: (Option[Any]) => (Option[Any], R)): IO[R] = {
			inner.modify { map =>
				def recurse(path: NonEmptyList[Any], map: Map[Any, Any]): (Map[Any, Any], R) = {
					NonEmptyList.fromList(path.tail) match {
						case Some(tail) => {
							val child = map.get(path.head).fold(Map.empty[Any, Any])(_.asInstanceOf[Map[Any, Any]])
							val (newMap, result) = recurse(tail, child)
							(map.updated(path.head, newMap), result)
						}
						case None => {
							val current = map.get(path.head)
							println(s"modifying @ $fullPath: ${current}")
							val (newValue, result) = fn(current)
							println(s"-> $newValue")
							val newMap = newValue match {
								case Some(x) => map.updated(path.head, x)
								case None => map.removed(path.head)
							}
							(newMap, result)
						}
					}
				}
				recurse(fullPath, map)
			}
		}
	}

	/** An appended value, plus a slot for acks to accumulate.
		*
		* `tracker` is `None` for `emit`, where nobody is waiting.
		*/
	private[corama] final case class Record(value: Any, tracker: Option[EvalResults])

	class Depot() {
		val inner = SignallingRef.of[IO, Vector[Record]](Vector.empty).unsafeRunSync()(using unsafe.IORuntime.global)
		def stream: Stream[IO, Record] = inner.discrete.mapAccumulate(0) { (seen, values) =>
			val additions = values.drop(seen)
			(seen + additions.length, additions)
		}.mapChunks { ch => ch.flatMap(tup => Chunk.from(tup._2)) }
	}

	class Lens[V](pstate: PState, path: NonEmptyList[Any]) {
		def put(v: V): IO[Unit] = pstate.modify(path) { _ => (Some(v), ()) }
		def get: IO[Option[V]] = pstate.modify(path) { x => (x, x.asInstanceOf[Option[V]]) }
	}
}
