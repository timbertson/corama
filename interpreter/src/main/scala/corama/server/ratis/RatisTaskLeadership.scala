package corama.server.ratis

import org.apache.ratis.server.RaftServer
import cats.effect.IO
import corama.server.TaskLeadership

class RatisTaskLeadership(division: RaftServer.Division) extends TaskLeadership {
	def isLeader: IO[Boolean] = IO(division.getInfo().isLeader())
}
