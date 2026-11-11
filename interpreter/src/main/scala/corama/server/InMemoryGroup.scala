package corama.server

import corama.InstanceId
import cats.effect.Ref
import cats.effect.IO
import corama.interpreter.IOMap
import org.apache.ratis.protocol.RaftGroupId
import corama.server.ratis.LocalProposer


class InMemoryGroup(val state: Ref[IO, InMemoryGroup.State]) {
	def currentLeader: IO[Option[InstanceId]] = state.get.map(_.leader)
	def proposer(instance: InstanceId): LocalProposer = ???
	def leadership(instance: InstanceId): TaskLeadership = ???
}

object InMemoryGroup {
	def apply: IO[InMemoryGroup] = for {
		state <- Ref[IO].of(State(
			leader = None
		))
	} yield new InMemoryGroup(state)

	case class State(
		val leader: Option[InstanceId]
	)
}

class InMemoryGroups(val groups: IOMap[RaftGroupId, InMemoryGroup])
