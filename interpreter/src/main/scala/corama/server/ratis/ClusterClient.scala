package corama.server.ratis

import streamdsl.rpc
import corama.interpreter.IOMap
import cats.syntax.all.*
import cats.effect.IO
import cats.effect.kernel.Resource
import corama.interpreter.TaskId
import org.apache.ratis.client.RaftClient
import org.apache.ratis.protocol.RaftClientReply
import org.apache.ratis.protocol.RaftGroup
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.conf.Parameters
import flogger.IOLogging
import flogger.Log
import corama.server.ClusterConfig
import io.grpc.Metadata
import corama.InstanceId
import corama.interpreter.GrpcChannel
import cats.effect.std.Dispatcher

case class GroupClient(raft: RaftClient, group: RaftGroup)

class ClusterClient(
	val raftClients: IOMap[TaskId, GroupClient],
	val instanceClients: IOMap[InstanceId, rpc.CoramaNodeFs2Grpc[IO, Metadata]],
) {
	def invokeRaft(taskId: TaskId)(block: GroupClient => IO[RaftClientReply]): IO[RaftClientReply] = {
		raftClients.use(taskId)(block).flatMap { reply =>
			IO.fromEither(Option(reply.getException()).toLeft(reply))
		}
	}
	
	def invokeGrpc[A](instanceId: InstanceId)(block: rpc.CoramaNodeFs2Grpc[IO, Metadata] => IO[A]) = {
		instanceClients.use(instanceId)(block)
	}
}

object ClusterClient extends IOLogging {
	// one RaftClient per task in a trenchcoat
	def apply(config: ClusterConfig)(using log: Log[IO]): Resource[IO, ClusterClient] = {
		for {
			dispatcher <- Dispatcher.sequential[IO]
			raftClients <- config.tasks.parTraverse { task =>
				groupClient(config, task).map(task -> _)
			}
			instanceClients <- config.instances.parTraverse { instance =>
				Resource.eval(IO.fromTry(config.networkConfig.get(instance.id))).flatMap { nc =>
					GrpcChannel(nc.advertisedAddress, nc.grpcPort).map { channel =>
						rpc.CoramaNodeFs2Grpc.stub(dispatcher, channel)
					}
				}.map(instance.id -> _)
			}
		} yield new ClusterClient(IOMap(raftClients), IOMap(instanceClients))
	}

	private def groupClient(config: ClusterConfig, taskId: TaskId)(using log: Log[IO]): Resource[IO, GroupClient] = {
		for {
			group <- Resource.eval(IO.fromTry(config.raftGroup(taskId)))
			_ <- Resource.eval(log.debug(s"Building client for $taskId"))
			client <- Resource.fromAutoCloseable(
				IO.blocking {
					RaftClient.newBuilder()
						.setRaftGroup(group)
						.setProperties(new RaftProperties())
						.setParameters(new Parameters())
						.build()
				}
			)
		} yield GroupClient(client, group)
	}
}
