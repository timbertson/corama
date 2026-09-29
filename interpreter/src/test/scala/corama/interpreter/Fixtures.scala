package corama.interpreter

import corama.InstanceId

object Fixtures {
	val leaderConfig = LeaderConfig.default
	val leaderId = InstanceId("leader")
	val taskId = TaskId(1)
	val followerId1 = InstanceId("follower-1")
	val followerId2 = InstanceId("follower-2")
	
	val clusterInstances = Vector("i1", "i2", "i3").map(InstanceId.apply)
}
