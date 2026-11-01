package corama.interpreter

import cats.data.NonEmptyList
import cats.effect.IO

class IOMap[K,V](map: Map[K, V]) {
	def use[T](key: K)(block: V => IO[T]): IO[T] = {
		IO.fromEither(map.get(key).toRight(
			new RuntimeException(s"Key not present in map: ${key}")
		)).flatMap(block)
	}
}

object IOMap {
	def apply[K, V](map: Map[K,V]): IOMap[K,V] = new IOMap(map)
	def apply[K, V](map: NonEmptyList[(K,V)]): IOMap[K,V] = new IOMap(map.toList.toMap)
}
