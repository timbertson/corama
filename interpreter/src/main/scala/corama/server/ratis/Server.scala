package corama.server.ratis

import streamdsl.rpc
import corama.interpreter.IOMap
import corama.*
import corama.server.*
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all.*
import fs2.io.file.Files
import fs2.io.file.Path
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.protocol.exceptions.AlreadyExistsException
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.RaftServerConfigKeys

import java.io.File
import scala.concurrent.blocking
import scala.jdk.CollectionConverters.*
import flogger.Log
import flogger.IOLogging
import corama.server.ClusterConfig
import java.net.InetSocketAddress
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import corama.ActiveResource
import cats.data.NonEmptyList
import corama.error.Util
import corama.interpreter.SpeculativeState
import corama.interpreter.IncomingEvalRouter
import corama.interpreter.RepLog
import corama.interpreter.PStateView
import corama.server.InMemoryGroup

trait Server {
	def instanceId: InstanceId
}

class RatisServer(val instanceId: InstanceId, val server: RaftServer, val client: RatisClusterClient) extends Server

object Server extends IOLogging {
	class InMemoryServer(
		val instanceId: InstanceId,
		val controller: InstanceController,
	) extends Server

	def inMemory(
		config: ClusterConfig,
		topologies: Topologies,
		clusterClient: InMemoryClusterClient,
		instance: ClusterInstance
	)(using log: Log[IO]): ActiveResource[IO, InMemoryServer] = {
		val res = for {
			incomingEvalRouter <- Resource.eval(IncomingEvalRouter.apply)
			taskResources <- instance.tasks.traverse { taskId =>
				for {
					pstate <- Resource.eval(PStateView.inMemory)
					speculativeState = new SpeculativeState(pstate)
					group <- Resource.eval(InMemoryGroup(taskId))
					replog = RepLog.inMemory(instance.id, group)
					proposer = group.proposer(instance.id)
					(serverTask, bg) <- ServerTask.resource(
						instance.id,
						taskId,
						topologies,
						clusterClient,
						new RatisRepLog(proposer),
						speculativeState,
						config,
						group.leadership(instance.id),
						incomingEvalRouter,
						maxInFlight = 10
					).resource
				} yield (
					(taskId, serverTask), // serverTask KV
					bg
				)
			}
			serverTaskMap = IOMap(taskResources.map(_._1))
			controller = new InstanceController(config, topologies, incomingEvalRouter, serverTaskMap)
			bg = taskResources.parTraverse_(_.last) *> Util.neverReached
			server = new InMemoryServer(instance.id, controller)
		} yield (server, bg)
		ActiveResource(res)
	}
	
	private [corama] def apply(config: ClusterConfig, topologies: Topologies, instance: ClusterInstance, clusterClient: RatisClusterClient)(using log: Log[IO]):
		ActiveResource[IO, Server] =
	{
		val storagePath = Path(s"${config.dataRoot}/node-${instance.id.id}")

		val res: Resource[IO, (Server, IO[Nothing])] = for {
			networkConfig <- Resource.eval(config.networkConfig.getIO(instance.id))
			_ <- Resource.eval(log.debug(s"Creating $storagePath"))
			_ <- Resource.eval(Files.forIO.createDirectories(storagePath))


			firstGroup <- Resource.eval(IO.fromTry(config.raftGroup(instance.tasks.head)))

			props = {
				val props = new RaftProperties()
				GrpcConfigKeys.Server.setPort(props, networkConfig.port)
				GrpcConfigKeys.Admin.setPort(props, networkConfig.adminPort)
				RaftServerConfigKeys.setStorageDir(props, List(storagePath.toNioPath.toFile()).asJava)
				props
			}

			stateMachineMap = IOMap(instance.tasks.map(task => (config.groupId(task), new StateMachine(task))))
			server <- Resource.fromAutoCloseable(IO(RaftServer.newBuilder()
				.setServerId(instance.peerId)
				.setGroup(firstGroup)
				.setProperties(props)
				.setStateMachineRegistry(groupId => stateMachineMap.get(groupId).toTry.get)
				.build()
			))
			
			incomingEvalRouter <- Resource.eval(IncomingEvalRouter.apply)

			taskResources <- instance.tasks.traverse { taskId =>
				val groupId = config.groupId(taskId)
				for {
					stateMachine <- Resource.eval(IO.fromEither(stateMachineMap.get(groupId)))
					speculativeState = new SpeculativeState(StateMachine.view(stateMachine))
					proposer <- Resource.eval(LocalProposer(server, groupId))
					(serverTask, bg) <- ServerTask.resource(
						instance.id,
						taskId,
						topologies,
						clusterClient,
						new RatisRepLog(proposer),
						speculativeState,
						config,
						new RatisTaskLeadership(taskId, server.getDivision(config.groupId(taskId))),
						incomingEvalRouter,
						maxInFlight = 10
					).resource
				} yield (
					(taskId, serverTask), // serverTask KV
					bg
				)
			}
			taskResourcesBg = taskResources.parTraverse(_.last) *> Util.neverReached
			
			_ <- Resource.eval(log.info(s"Starting server on ${networkConfig}"))
			_ <- Resource.eval(IO.blocking {
				server.start()
			})

			// server builder only takes one group; must add the rest after startup
			_ <- Resource.eval(joinGroups(clusterClient, instance))
			
			serverTaskMap = IOMap(taskResources.map(_._1))
			controller = new InstanceController(config, topologies, incomingEvalRouter, serverTaskMap)
			apiController = new ApiController(config, clusterClient)

			_ <- grpcServer(networkConfig.bindGrpcSocket, controller, apiController)

		} yield (new RatisServer(instance.id, server, clusterClient), taskResourcesBg)
		
		ActiveResource(res)
	}

	// add any groups not already registered (e.g. recovered from storage on restart)
	private def joinGroups(client: RatisClusterClient, instance: ClusterInstance)(using log: Log[IO]): IO[Unit] = {
		log.debug(s"joining additional groups") >>
		instance.tasks.traverse_ { taskId =>
			client.invokeRaft(taskId) { client =>
				IO(blocking {
					client.raft.getGroupManagementApi(instance.peerId).add(client.group, true)
				})
			}.void.recover {
				case e: AlreadyExistsException => ()
			}
		}
	}

	private def grpcServer(
		address: InetSocketAddress,
		controller: InstanceController,
		apiController: ApiController,
	): Resource[IO, io.grpc.Server] = {
		import fs2.grpc.syntax.all._
		for {
			instanceService <- rpc.CoramaInstanceFs2Grpc.bindServiceResource(controller)
			apiService <- rpc.CoramaFs2Grpc.bindServiceResource(apiController)
			server <- NettyServerBuilder
				.forAddress(address)
				.addService(instanceService)
				.addService(apiService)
				.resource[IO]
			_ <- Resource.eval(IO(server.start()))
		} yield server
	}
}
