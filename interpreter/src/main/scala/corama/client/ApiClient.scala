package corama.client

import streamdsl.rpc
import io.grpc.Metadata
import corama.server.ClusterConfig
import cats.effect.kernel.Resource
import cats.effect.IO
import cats.effect.std.Dispatcher
import flogger.IOLogging
import flogger.Log

class ApiClient(val stub: rpc.CoramaFs2Grpc[IO, Metadata])

object ApiClient extends IOLogging {
	def apply(config: ClusterConfig)(using log: Log[IO]): Resource[IO, ApiClient] = {
		for {
			addresses <- Resource.eval(IO.fromTry(config.instances.traverse { instance =>
				config.networkConfig.get(instance.id)
			}))
			// TODO: load balance multiple instances
			address = addresses.head
			dispatcher <- Dispatcher.sequential[IO]
			_ <- Resource.eval(log.debug(s"connecting to GRPC service on ${address.advertisedAddress}:${address.grpcPort}"))
			channel <- GrpcChannel(address.advertisedAddress, address.grpcPort)
			stub = rpc.CoramaFs2Grpc.stub(dispatcher, channel)
		} yield new ApiClient(rpc.CoramaFs2Grpc.stub(dispatcher, channel))
	}
}
