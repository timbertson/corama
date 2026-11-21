package corama.server

import cats.syntax.all.*
import corama.InstanceId
import cats.effect.Ref
import cats.effect.IO
import org.apache.ratis.protocol.RaftGroupId
import cats.data.NonEmptyList

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
	def apply(config: ClusterConfig, taskId: TaskId): IO[InMemoryGroup] = for {
		instances <- IO.fromTry(config.groupInstances(taskId))
		instanceIds = instances.map(_.id)
		state <- Ref[IO].of(State(
			leader = Some(instanceIds.head)
		))
	} yield new InMemoryGroup(taskId, state)

	case class State(
		val leader: Option[InstanceId]
	)
}

class InMemoryGroups(val groups: IOMap[RaftGroupId, InMemoryGroup])
