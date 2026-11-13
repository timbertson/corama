package corama.client

import streamdsl.rpc
import io.grpc.Metadata
import corama.server.ClusterConfig
import cats.effect.kernel.Resource
import cats.effect.IO
import cats.effect.std.Dispatcher

class ApiClient(val stub: rpc.CoramaFs2Grpc[IO, Metadata])

object ApiClient {
	def apply(config: ClusterConfig): Resource[IO, ApiClient] = {
		for {
			addresses <- Resource.eval(IO.fromTry(config.instances.traverse { instance =>
				config.networkConfig.get(instance.id)
			}))
			// TODO: load balance multiple instances
			address = addresses.head
			dispatcher <- Dispatcher.sequential[IO]
			channel <- GrpcChannel(address.advertisedAddress, address.port)
			stub = rpc.CoramaFs2Grpc.stub(dispatcher, channel)
		} yield new ApiClient(rpc.CoramaFs2Grpc.stub(dispatcher, channel))
	}
}
