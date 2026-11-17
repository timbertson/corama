package corama.client

import corama.Codec
import corama.Depot
import cats.effect.Resource
import cats.effect.IO
import com.google.protobuf.ByteString
import io.grpc.Metadata
import corama.QueryTopology
import streamdsl.rpc
import corama.error.Util
import corama.Topologies
import corama.AckReturns
import flogger.IOLogging
import flogger.Log

// wraps an ApiClient with a typed API & codec support
class TopologyClient(topologies: Topologies, base: ApiClient) extends IOLogging {
	private val ackCodec = AckReturns.codec(topologies)

	def append[A](depot: Depot[A], value: A)(using codec: Codec[A])(using log: Log[IO]): IO[AckReturns] = {
		val request = rpc.AppendRequest.of(
			depot = depot.id.value,
			value = ByteString.copyFrom(codec.encode(value)),
			partition = None
		)

		log.debug(s"appending to ${depot.id}") *>
		base.stub.append(request, Metadata()).flatMap { response =>
			for {
				bytes <- Util.require("response.value", response.value)
				value <- IO.fromTry(ackCodec.decode(bytes.toByteArray()))
			} yield value
		}
	}

	def query[S, R](query: QueryTopology[S, R], value: S)
		(using sourceCodec: Codec[S])
		(using responseCodec: Codec[R])
		(using log: Log[IO]): IO[R] =
	{
		val request = rpc.QueryRequest.of(
			query = query.id.value,
			value = ByteString.copyFrom(sourceCodec.encode(value)),
			partition = None,
		)

		log.debug(s"querying ${query.id}") *>
		base.stub.query(request, Metadata()).flatMap { response =>
			for {
				bytes <- Util.require("response.value", response.value)
				value <- IO.fromTry(responseCodec.decode(bytes.toByteArray()))
			} yield value
		}
	}
}

object TopologyClient {
	def apply(config: ClientConfig, topologies: Topologies)(using log: Log[IO]): Resource[IO, TopologyClient] = {
		ApiClient(config).map(new TopologyClient(topologies, _))
	}
}
