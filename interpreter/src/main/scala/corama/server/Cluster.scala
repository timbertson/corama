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
import flogger.IOLogging
import corama.client.TopologyClient
import corama.client.ApiClient

object Cluster extends IOLogging {
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
			_ <- Resource.eval(IO.whenA(config.numInstances < 3)(
				log.warn(s"Running a cluster with only ${config.numInstances} is not robust; at least 3 are required for HA + durability")
			))
			client <- RatisClusterClient(config)
			pair <- Server(config, topologies, instance, client)(using instanceLog).resource
		} yield pair)
	}

	// Run a mock cluster without real IO (stubs out ratis, grpc, network & disk)
	def inMemoryCluster(config: ClusterConfig, topologies: Topologies)(using log: Log[IO]): ActiveResource[IO, TopologyClient] = {
		val res = for {
			groups <- Resource.eval(config.tasks.traverse { taskId =>
				InMemoryGroup(taskId).map(taskId -> _)
			}.map(IOMap.apply))

			clusterClient <- Resource.eval(ClusterClient.inMemory(groups))

			(instances, bg) <- ActiveResource.parTraverse(config.instances) { instance =>
				val instanceLog = log.addContext("instance" -> instance.id.id.toString())
				Server.inMemory(config, topologies, clusterClient, instance)(using instanceLog)
			}.resource

			// connect instance & API clients directly to the controller implementation,
			// bypassing GRPC.
			// TODO this doesn't let us inject faults into the API layer, nor does it test
			// GRPC level handling like headers etc.
			instanceMap = IOMap(instances.map(i => i.instanceId -> i.instanceController))
			_ <- Resource.eval(clusterClient.setInstances(instanceMap))
			apiClient = new ApiClient(instances.head.apiController)
		} yield (new TopologyClient(config, topologies, apiClient), bg)
		ActiveResource(res)
	}
}
