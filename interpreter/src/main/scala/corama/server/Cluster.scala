package corama.server

import corama.ActiveResource
import cats.effect.IO

object Cluster {
	// A real cluster, with all nodes running on random localhost ports
	def local: ActiveResource[IO, Unit] = ???
	
	// An in-memory cluster involving no real IO, so that
	// it can be deterministically executed in a test scheduler
	def inMemory: ActiveResource[IO, Unit] = ???
}
