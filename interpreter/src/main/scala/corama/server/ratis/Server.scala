package corama.server.ratis

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

class Server(
	val raft: RaftServer,
	val client: ClusterClient,
)

object Server extends IOLogging {
	def running(config: ClusterConfig, topologies: Topologies, instance: ClusterInstance)(using log: Log[IO]) = {
		val storagePath = Path(s"${config.dataRoot}/node-${instance.id.id}")

		val res: Resource[IO, (Server, IO[Nothing])] =
		for {
			networkConfig <- Resource.eval(config.networkConfig.getIO(instance.id))
			_ <- Resource.eval(log.debug(s"Creating $storagePath"))
			_ <- Resource.eval(Files.forIO.createDirectories(storagePath))

			client <- ClusterClient(config)

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
						client,
						new RatisRepLog(proposer),
						speculativeState,
						config,
						new RatisTaskLeadership(server.getDivision(config.groupId(taskId))),
						identity[IncomingEvalRouter](???),
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
			_ <- Resource.eval(joinGroups(client, instance))
			
			serverTaskMap = IOMap(taskResources.map(_._1))
			controller = new CoramaController(config, topologies, serverTaskMap)

			_ <- grpcServer(networkConfig.bindGrpcSocket, controller)

		} yield (new Server(server, client), taskResourcesBg)
		
		ActiveResource(res)
	}

	// add any groups not already registered (e.g. recovered from storage on restart)
	private def joinGroups(client: ClusterClient, instance: ClusterInstance)(using log: Log[IO]): IO[Unit] = {
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

	def runningCluster(config: ClusterConfig, topologies: Topologies)(using log: Log[IO]): ActiveResource[IO, NonEmptyList[Server]] = {
		val resources = config.instances.parTraverse { instance =>
			val instanceLog = log.addContext("instance" -> instance.id.id.toString())
			running(config, topologies, instance)(using instanceLog).resource
		}
		ActiveResource(resources.map { tuples =>
			(tuples.map(_._1), tuples.map(_._2).parSequence *> Util.neverReached)
		})
	}
	
	private def grpcServer(address: InetSocketAddress, controller: CoramaController): Resource[IO, io.grpc.Server] = {
		import fs2.grpc.syntax.all._
		streamdsl.rpc.CoramaNodeFs2Grpc.bindServiceResource(controller).flatMap { service =>
			NettyServerBuilder
				.forAddress(address)
				.addService(service)
				.resource[IO]
				.evalMap(server => IO(server.start()))
		}
	}
}
