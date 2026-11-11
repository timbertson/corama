package corama.server

import corama.ActiveResource
import cats.effect.IO
import corama.Topologies
import flogger.Log
import cats.data.NonEmptyList
import cats.effect.Resource
import corama.server.ratis.Server
import corama.interpreter.IOMap

object Cluster {
	def runningCluster(config: ClusterConfig, topologies: Topologies)(using log: Log[IO]): ActiveResource[IO, NonEmptyList[Server]] = {
		ActiveResource.parTraverse(config.instances) { instance =>
			val instanceLog = log.addContext("instance" -> instance.id.id.toString())
			Server.running(config, topologies, instance)(using instanceLog)
		}
	}

	def inMemoryCluster(config: ClusterConfig, topologies: Topologies)(using log: Log[IO]): ActiveResource[IO, InMemoryClusterClient] = {
		val res = for {
			groups <- Resource.eval(config.tasks.traverse { taskId =>
				InMemoryGroup(taskId).map(taskId -> _)
			}.map(IOMap.apply))

			clusterClient <- Resource.eval(ClusterClient.inMemory(groups))

			(instances, bg) <- ActiveResource.parTraverse(config.instances) { instance =>
				val instanceLog = log.addContext("instance" -> instance.id.id.toString())
				Server.inMemory(config, topologies, clusterClient, instance)(using instanceLog)
			}.resource

			instanceMap = IOMap(instances.map(i => i.instanceId -> i.controller))
			_ <- Resource.eval(clusterClient.setInstances(instanceMap))
		} yield (clusterClient, bg)
		ActiveResource(res)
	}
}
