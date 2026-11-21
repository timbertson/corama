package corama.server

import scala.concurrent.duration.*
import corama.InstanceId
import cats.data.NonEmptyList
import org.scalacheck.Gen
import cats.Show

object Fixtures {
	implicit def showAny[A]: Show[A] = Show.fromToString
	val taskId = TaskId(1)
	val leaderId = InstanceId("leader")
	val followerId1 = InstanceId("follower-1")
	val followerId2 = InstanceId("follower-2")
	def instanceIdForOrdinal(ordinal: Int) = InstanceId(s"i${ordinal}")

	def genClusterConfig(
		numInstances: Gen[Int] = Gen.const(3),
		tasksFromInstances: Int => Gen[Int] = x => Gen.const(x * 2),
	): Gen[ClusterConfig] = for {
		numInstancesVal <- numInstances
		numTasksVal <- tasksFromInstances(numInstancesVal)
		instances = NonEmptyList.fromListUnsafe(Range.inclusive(1, numInstancesVal).map(instanceIdForOrdinal).toList)
	} yield ClusterConfig.make(
		dataRoot = "/does-not-exist",
		instances = instances,
		numTasks = numTasksVal,
		numPartitions = numTasksVal * 2,
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

	val clusterConfig: ClusterConfig = genClusterConfig().sample.get
	val maxClusterSize = 50
	val genClusterSize = Gen.choose(1, maxClusterSize)
}
