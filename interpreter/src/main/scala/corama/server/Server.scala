package corama.server

// import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import corama.interpreter.IOMap
import corama.*
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
import corama.interpreter.TaskId

class Server(
	raft: RaftServer,
	proposers: IOMap[TaskId, LocalProposer],
	// peers: IOMap[InstanceId, streamdsl.rpc.CoramaFs2Grpc]
)

object Server extends IOLogging {
	def running(config: ClusterConfig, instance: ClusterInstance)(using log: Log[IO]) = {
		val storagePath = Path(s"${config.dataRoot}/node-${instance.id.id}")
		for {
			networkConfig <- Resource.eval(config.networkConfig.getIO(instance.id))
			_ <- Resource.eval(log.debug(s"Creating $storagePath"))
			_ <- Resource.eval(Files.forIO.createDirectories(storagePath))
			instanceGroups <- Resource.eval(instance.tasks.traverse { taskId =>
				IO.fromTry(config.raftGroup(taskId).map { group =>
					(taskId, group, new StateMachine(taskId))
				})
			})
			props = {
				val props = new RaftProperties()
				GrpcConfigKeys.Server.setPort(props, networkConfig.port)
				GrpcConfigKeys.Admin.setPort(props, networkConfig.adminPort)
				RaftServerConfigKeys.setStorageDir(props, List(storagePath.toNioPath.toFile()).asJava)
				props
			}
			stateMachineMap = instanceGroups.map { (_, group, sm) => group.getGroupId() -> sm }.toList.toMap
			server <- Resource.fromAutoCloseable(IO(RaftServer.newBuilder()
				.setServerId(instance.peerId)
				.setGroup(instanceGroups.head._2)
				.setProperties(props)
				.setStateMachineRegistry(groupId => stateMachineMap.getOrElse(groupId, {
					throw new RuntimeException(s"Unknown group: ${groupId}")
				}))
				.build()
			))
			proposers <- Resource.eval(instanceGroups.traverse { (task, group, _) =>
				LocalProposer(server, group.getGroupId()).map(task -> _)
			})
			_ <- Resource.eval(log.info(s"Starting server on ${networkConfig}"))
			_ <- Resource.eval(IO.blocking(server.start()))

			// Note: client only used in setup, so `use` instead of nesting the resource
			// (a resource would stay active until the server gets released)
			_ <- Resource.eval(ClusterClient(config).use { client =>
				joinGroups(client, instance)
			})

			// setGroup only registers one group; must add the rest after startup
		} yield new Server(server, IOMap(proposers))
	}

	// add any groups not already registered (e.g. recovered from storage on restart)
	private def joinGroups(client: ClusterClient, instance: ClusterInstance)(using log: Log[IO]): IO[Unit] = {
		log.debug(s"joining additional groups") >>
		instance.tasks.traverse_ { taskId =>
			client.invoke(taskId) { client =>
				IO(blocking {
					client.raft.getGroupManagementApi(instance.peerId).add(client.group, true)
				})
			}.void.recover {
				case e: AlreadyExistsException => ()
			}
		}
	}

	def runningCluster(config: ClusterConfig)(using log: Log[IO]) = {
		config.instances.parTraverse { instance =>
			val instanceLog = log.addContext("instance" -> instance.id.id.toString())
			running(config, instance)(using instanceLog)
		}
	}
	
	// private def grpcService: Resource[streamdsl.rpc.CoramaFs2Grpc] = {
	// 	NettyServerBuilder
	// 		.forPort(9999)
	// 		.addService(service)
	// 		.resource[IO]
	// 		.evalMap(server => IO(server.start()))
	// 		.useForever
	// }
}
