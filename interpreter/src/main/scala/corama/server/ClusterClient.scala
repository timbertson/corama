package corama.server
import streamdsl.rpc
import corama.InstanceId
import cats.effect.IO
import io.grpc.Metadata
import corama.interpreter.IOMap
import cats.effect.Ref
import corama.interpreter.TaskId
import streamdsl.rpc.CoramaInstanceFs2Grpc

trait ClusterClient {
	def invokeGrpc[A](instanceId: InstanceId)(block: rpc.CoramaInstanceFs2Grpc[IO, Metadata] => IO[A]): IO[A]
	def currentLeader(task: TaskId): IO[Option[InstanceId]]
	def requireCurrentLeader(task: TaskId): IO[InstanceId] =
		InstanceTaskLeadership.requireCurrentLeader(task, currentLeader(task))
}

object ClusterClient {
	def inMemory(
		groups: IOMap[TaskId, InMemoryGroup],
	): IO[InMemoryClusterClient] = for {
		instances <- Ref[IO].of(IOMap.empty[InstanceId, InstanceController])
	} yield new InMemoryClusterClient(groups, instances)
}

class InMemoryClusterClient(
	groups: IOMap[TaskId, InMemoryGroup],
	instances: Ref[IO, IOMap[InstanceId, InstanceController]],
) extends ClusterClient {
	def setInstances(updated: IOMap[InstanceId, InstanceController]): IO[Unit] = instances.set(updated)
	override def invokeGrpc[A](instanceId: InstanceId)(block: CoramaInstanceFs2Grpc[IO, Metadata] => IO[A]): IO[A] = {
		instances.get.flatMap(_.use(instanceId)(block))
	}
	
	def currentLeader(task: TaskId): IO[Option[InstanceId]] = groups.use(task)(_.currentLeader)
}
