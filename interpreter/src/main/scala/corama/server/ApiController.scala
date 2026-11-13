package corama.server

import cats.effect.IO
import corama.PartitionId
import corama.error.Util
import corama.interpreter.TaskId
import io.grpc.Metadata
import streamdsl.rpc
import streamdsl.rpc.AppendRequest
import streamdsl.rpc.QueryRequest
import streamdsl.rpc.Response
import corama.client.Retry
import flogger.Log
import flogger.IOLogging

class ApiController(
	clusterConfig: ClusterConfig,
	clusterClient: ClusterClient,
)(using log: Log[IO]) extends rpc.CoramaFs2Grpc[IO, Metadata] with IOLogging {
	// TODO: accept requests without an explicit partition
	override def append(request: AppendRequest, ctx: Metadata): IO[Response] = {
		delegateTo(request.partition.map(PartitionId.apply)) { instance =>
			instance.append(request, ctx)
		}
	}

	override def query(request: QueryRequest, ctx: Metadata): IO[Response] = {
		delegateTo(request.partition.map(PartitionId.apply)) { instance =>
			instance.query(request, ctx)
		}
	}
	
	// TODO: retry if group leadership was stale
	private def delegateTo[A](partition: Option[PartitionId])
		(block: rpc.CoramaInstanceFs2Grpc[IO, Metadata] => IO[A])
		: IO[A] =
	{
		for {
			partition <- Util.require("partition", partition)
			taskId = clusterConfig.taskFor(partition)
			fetch = for {
				leader <- clusterClient.requireCurrentLeader(taskId)
				_ <- log.debug(s"Resolved partition task $taskId ($partition) to leader $leader")
				response <- clusterClient.invokeGrpc(leader)(block)
			} yield response
			result <- Retry(fetch)
		} yield result
	}
}

