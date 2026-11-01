package corama.server

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

case class GroupClient(raft: RaftClient, group: RaftGroup)

class ClusterClient(clients: IOMap[TaskId, GroupClient]) {
	def invoke(taskId: TaskId)(block: GroupClient => IO[RaftClientReply]): IO[RaftClientReply] = {
		clients.use(taskId)(block).flatMap { reply =>
			IO.fromEither(Option(reply.getException()).toLeft(reply))
		}
	}
}

object ClusterClient extends IOLogging {
	// one RaftClient per task in a trenchcoat
	def apply(config: ClusterConfig)(using log: Log[IO]): Resource[IO, ClusterClient] = {
		config.tasks.parTraverse { task =>
			groupClient(config, task).map(task -> _)
		}.map { clients =>
			new ClusterClient(IOMap(clients))
		}
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
