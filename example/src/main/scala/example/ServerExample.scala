package example

import corama.server.ClusterConfig
import corama.server.Server
import cats.effect.IOApp
import cats.effect.ExitCode
import cats.effect.IO
import corama.server.ClusterClient
import flogger.Log


object ServerExample extends IOApp {
	private implicit val log: Log[IO] = Log.empty
	override def run(args: List[String]): IO[ExitCode] = {
		val res = for {
			config <- ClusterConfig.ephemeral()
			servers <- Server.runningCluster(config)
			client <- ClusterClient(config)
		} yield (config)
		
		res.use { config =>
			val instance = config.instances.head
			for {
				nc <- config.networkConfig.getIO(instance.id)
			} yield println(s"TODO: connect to local server on ${nc.advertisedAdminSocket}")
		}.as(ExitCode.Success)
	}
}
