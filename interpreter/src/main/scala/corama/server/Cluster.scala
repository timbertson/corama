package corama.server

import corama.ActiveResource
import cats.effect.IO
import corama.Topologies
import flogger.Log
import cats.data.NonEmptyList
import cats.effect.Resource
import corama.server.ratis.Server
import corama.interpreter.IOMap
import corama.server.ratis.RatisClusterClient

object Cluster {
	// Run a full cluster on localhost
	def runningCluster(config: ClusterConfig, topologies: Topologies)(using log: Log[IO]): ActiveResource[IO, ClusterClient] = {
		{
			ActiveResource(for {
				client <- RatisClusterClient(config)
				(_servers, bg) <- ActiveResource.parTraverse(config.instances) { instance =>
					val instanceLog = log.addContext("instance" -> instance.id.id.toString())
					Server(config, topologies, instance, client)(using instanceLog)
				}.resource
			} yield (client, bg))
		}
	}

	// standalone server. Use runningCluster when running a full cluster
	def runningInstance(config: ClusterConfig, topologies: Topologies, instance: ClusterInstance)(using log: Log[IO]):
		ActiveResource[IO, Server] =
	{
		val instanceLog = log.addContext("instance" -> instance.id.id.toString())
		ActiveResource(for {
			client <- RatisClusterClient(config)
			pair <- Server(config, topologies, instance, client)(using instanceLog).resource
		} yield pair)
	}

	// Run a mock cluster without real IO (stubs out ratis, grpc, network & disk)
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
