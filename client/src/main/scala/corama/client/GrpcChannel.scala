package corama.client

import scala.concurrent.blocking
import io.grpc.ManagedChannel
import cats.effect.IO
import cats.effect.Resource
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit

object GrpcChannel {
	def apply(address: String, port: Int): Resource[IO, ManagedChannel] = {
		val makeChannel = IO.delay(ManagedChannelBuilder.forAddress(address, port).usePlaintext().build)
		def closeChannel(channel: ManagedChannel) = {
			IO.delay(channel.shutdownNow()) *> IO.delay {
				blocking {
					channel.awaitTermination(10, TimeUnit.SECONDS)
				}
			}.void
		}
		Resource.make(makeChannel)(closeChannel)
	}
}
