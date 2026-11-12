package corama.server.ratis

import org.apache.ratis.server.RaftServer
import cats.effect.IO
import corama.server.InstanceTaskLeadership
import corama.InstanceId
import corama.interpreter.TaskId
import org.apache.ratis.protocol.RaftPeerId

class RatisTaskLeadership(val taskId: TaskId, division: RaftServer.Division) extends InstanceTaskLeadership {
	import RatisTaskLeadership.*

	def isLeader: IO[Boolean] = IO(division.getInfo().isLeader())

	def currentLeader: IO[Option[InstanceId]] = IO {
		instanceOfNullableLeader(division.getInfo().getLeaderId())
	}
}

object RatisTaskLeadership {
	def instanceOfNullableLeader(leader: RaftPeerId): Option[InstanceId] =
		Option(leader).map(peerId => InstanceId(peerId.toString))
}
