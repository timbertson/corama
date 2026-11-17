package corama.interpreter

import scala.concurrent.duration.*
import corama.InstanceId
import corama.server.ClusterConfig
import cats.data.NonEmptyList
import corama.server.PerInstance
import corama.server.NetworkConfig

object Fixtures {
	val taskId = TaskId(1)
	val leaderId = InstanceId("leader")
	val followerId1 = InstanceId("follower-1")
	val followerId2 = InstanceId("follower-2")
	
	val clusterConfig = ClusterConfig.make(
		dataRoot = "/does-not-exist",
		instances = NonEmptyList.of("i1", "i2", "i3").map(InstanceId.apply),
		numTasks = 6,
		numPartitions = 12,
		networkConfig = PerInstance.global(NetworkConfig(
			// network config is unused for in-memory cluster
			bindAddress = "localhost",
			advertisedAddress = "localhost",
			port = 1,
			adminPort = 1,
			grpcPort = 1,
		)),
		evalTimeout = 2.seconds,
	).get
}
