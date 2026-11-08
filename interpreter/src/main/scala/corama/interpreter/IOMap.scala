package corama.interpreter

import cats.data.NonEmptyList
import cats.effect.IO

class IOMap[K,V](val inner: Map[K, V]) {
	def get(key: K): Either[Throwable, V] = {
		inner.get(key).toRight(
			new RuntimeException(s"Key not present in map: ${key}")
		)
	}
	def use[T](key: K)(block: V => IO[T]): IO[T] = {
		IO.fromEither(get(key)).flatMap(block)
	}
}

object IOMap {
	def apply[K, V](map: Map[K,V]): IOMap[K,V] = new IOMap(map)
	def apply[K, V](map: NonEmptyList[(K,V)]): IOMap[K,V] = new IOMap(map.toList.toMap)
}
