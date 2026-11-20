package corama.server

import cats.effect.IO
import corama.PartitionId
import io.grpc.Metadata
import streamdsl.rpc
import streamdsl.rpc.AppendRequest
import streamdsl.rpc.QueryRequest
import streamdsl.rpc.Response
import flogger.Log
import flogger.IOLogging
import scala.util.Success
import scala.util.Try

class ApiController(
	clusterConfig: ClusterConfig,
	clusterClient: ClusterClient,
	requestResolver: RequestResolver
)(using log: Log[IO]) extends rpc.CoramaFs2Grpc[IO, Metadata] with IOLogging {
	// TODO: accept requests without an explicit partition
	override def append(request: AppendRequest, ctx: Metadata): IO[Response] = {
		val tryPartition = request.partition.map(PartitionId.apply) match {
			case None => requestResolver.resolveAppend(request).map(_.partition)
			case Some(p) => Success(p)
		}
		delegateTo(tryPartition)(_.append(request, ctx))
	}

	override def query(request: QueryRequest, ctx: Metadata): IO[Response] = {
		val tryPartition = request.partition.map(PartitionId.apply) match {
			case None => requestResolver.resolveQuery(request).map(_.partition)
			case Some(p) => Success(p)
		}
		delegateTo(tryPartition)(_.query(request, ctx))
	}
	
	// TODO: retry if group leadership was stale
	// TODO: redirect internally if this instance is the leader for this task?
	private def delegateTo[A](partition: Try[PartitionId])
		(block: rpc.CoramaInstanceFs2Grpc[IO, Metadata] => IO[A])
		: IO[A] =
	{
		GrpcError.wrap(for {
			partition <- IO.fromTry(partition)
			taskId = clusterConfig.taskFor(partition)
			fetch = for {
				leader <- clusterClient.requireCurrentLeader(taskId)
				_ <- log.debug(s"Resolved partition task $taskId ($partition) to leader $leader")
				response <- clusterClient.invokeGrpc(leader)(block)
			} yield response
			// result <- Retry(fetch)
			result <- fetch
		} yield result)
	}
}
