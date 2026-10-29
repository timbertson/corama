package corama.server

import corama.InstanceId
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.protocol.RaftPeerId
import org.apache.ratis.server.RaftServer
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.server.RaftServerConfigKeys
import scala.jdk.CollectionConverters.*
import fs2.io.file.Files
import java.nio.file.Paths
import java.io.File
import fs2.io.file.Path
import org.apache.ratis.protocol.RaftGroup
import org.apache.ratis.protocol.RaftGroupId
import java.util.UUID
import org.apache.ratis.protocol.RaftPeer
import cats.effect.kernel.Resource

object Server {
	case class Config(
		groupId: UUID,
		port: Int,
		adminPort: Int,
		address: String,
		instance: InstanceId,
	)

	def resource(config: Config) = {
		val peerId = RaftPeerId.valueOf(config.instance.id)
		val props = new RaftProperties()
		GrpcConfigKeys.Server.setPort(props, config.port)
		GrpcConfigKeys.Admin.setPort(props, config.adminPort)
		
		val stateMachine = new StateMachine()

		val storagePath = Path(s"data/node-${config.instance.id}")
		RaftServerConfigKeys.setStorageDir(props, List(storagePath.toNioPath.toFile()).asJava)
		val group = RaftGroup.valueOf(RaftGroupId.valueOf(config.groupId))

		val acquire = for {
			_ <- Files.forIO.createDirectories(storagePath)
		} yield RaftServer.newBuilder()
			.setServerId(peerId)
			.setGroup(group)
			.setStateMachine(stateMachine)
			.build()

		Resource.fromAutoCloseable(acquire)
	}
}
