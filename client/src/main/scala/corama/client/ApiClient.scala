package corama.client

import streamdsl.rpc
import io.grpc.Metadata
import cats.effect.kernel.Resource
import cats.effect.IO
import cats.effect.std.Dispatcher
import flogger.IOLogging
import flogger.Log

class ApiClient(val stub: rpc.CoramaFs2Grpc[IO, Metadata])

object ApiClient extends IOLogging {
	def apply(config: ClientConfig)(using log: Log[IO]): Resource[IO, ApiClient] = {
		for {
			dispatcher <- Dispatcher.sequential[IO]
			_ <- Resource.eval(log.debug(s"connecting to GRPC service on ${config.address}:${config.port}"))
			channel <- GrpcChannel(config.address, config.port)
			stub = rpc.CoramaFs2Grpc.stub(dispatcher, channel)
		} yield new ApiClient(rpc.CoramaFs2Grpc.stub(dispatcher, channel))
	}
}
