package corama.server
import streamdsl.rpc
import corama.InstanceId
import cats.effect.IO
import io.grpc.Metadata
import corama.interpreter.IOMap
import cats.effect.Ref
import corama.interpreter.TaskId
import streamdsl.rpc.CoramaNodeFs2Grpc

trait ClusterClient {
	def invokeGrpc[A](instanceId: InstanceId)(block: rpc.CoramaNodeFs2Grpc[IO, Metadata] => IO[A]): IO[A]
}

object ClusterClient {
	def inMemory(
		groups: IOMap[TaskId, InMemoryGroup],
	): IO[InMemoryClusterClient] = for {
		instances <- Ref[IO].of(IOMap.empty[InstanceId, CoramaController])
	} yield new InMemoryClusterClient(groups, instances)
}

class InMemoryClusterClient(
	groups: IOMap[TaskId, InMemoryGroup],
	instances: Ref[IO, IOMap[InstanceId, CoramaController]],
) extends ClusterClient {
	def setInstances(updated: IOMap[InstanceId, CoramaController]): IO[Unit] = instances.set(updated)
	override def invokeGrpc[A](instanceId: InstanceId)(block: CoramaNodeFs2Grpc[IO, Metadata] => IO[A]): IO[A] = {
		instances.get.flatMap(_.use(instanceId)(block))
	}
	
	def currentLeader(task: TaskId): IO[Option[InstanceId]] = groups.use(task)(_.currentLeader)
}
