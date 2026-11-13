package corama.server

import cats.syntax.all.*
import corama.InstanceId
import cats.effect.Ref
import cats.effect.IO
import corama.interpreter.IOMap
import org.apache.ratis.protocol.RaftGroupId
import corama.server.ratis.LocalProposer
import corama.interpreter.TaskId


class InMemoryGroup(private val taskId: TaskId, state: Ref[IO, InMemoryGroup.State]) {
	def currentLeader = state.get.map(_.leader)

	private val group = this

	def leadership(instance: InstanceId): InstanceTaskLeadership = new InstanceTaskLeadership {
		override val taskId: TaskId = group.taskId

		override def isLeader: IO[Boolean] = group.currentLeader.map(_ === Some(instance))

		override def currentLeader = group.currentLeader
	}
}

object InMemoryGroup {
	def apply(taskId: TaskId): IO[InMemoryGroup] = for {
		state <- Ref[IO].of(State(
			leader = None
		))
	} yield new InMemoryGroup(taskId, state)

	case class State(
		val leader: Option[InstanceId]
	)
}

class InMemoryGroups(val groups: IOMap[RaftGroupId, InMemoryGroup])
