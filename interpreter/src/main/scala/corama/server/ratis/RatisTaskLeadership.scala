package corama.server.ratis

import org.apache.ratis.server.RaftServer
import cats.effect.IO
import corama.server.InstanceTaskLeadership
import corama.InstanceId
import corama.interpreter.TaskId

class RatisTaskLeadership(val taskId: TaskId, division: RaftServer.Division) extends InstanceTaskLeadership {
	def isLeader: IO[Boolean] = IO(division.getInfo().isLeader())

	def currentLeader: IO[Option[InstanceId]] = IO {
		Option(division.getInfo().getLeaderId()).map(peer => InstanceId(peer.toString()))
	}
}
