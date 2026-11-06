package corama.server

import cats.effect.IO

trait TaskLeadership {
	def isLeader: IO[Boolean]
}
