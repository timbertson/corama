package corama.server

import io.grpc.Metadata
import cats.effect.IO
import streamdsl.rpc.ContinueRequest
import com.google.protobuf.empty.Empty
import streamdsl.rpc.Response
import corama.Topologies
import corama.error.Util
import corama.DepotId
import corama.interpreter.ClusterMetadata
import corama.AckLevel
import corama.interpreter.TaskId
import corama.interpreter.IOMap
import streamdsl.rpc.AppendRequest
import streamdsl.rpc.QueryRequest
import corama.PartitionId
import corama.Topology

class CoramaController(
	metadata: ClusterMetadata,
	topologies: Topologies,
	tasks: IOMap[TaskId, ServerTask],
) extends streamdsl.rpc.CoramaNodeFs2Grpc[IO, Metadata] {
	private val depots = topologies.depots
	
	private def requireLeader(id: TaskId): IO[ServerTask] = tasks.use(id) { serverTask =>
		serverTask.requireLeader.as(serverTask)
	}

	override def append(request: AppendRequest, ctx: Metadata): IO[Response] = {
		val depotId = DepotId(request.depot)
		for {
			depot <- IO.fromEither(topologies.depots.get(depotId).toRight(Util.err("No such depot")))
			partition <- IO.fromEither(request.partition.toRight(Util.err("Partition required"))).map(PartitionId.apply)
			taskId = metadata.taskFor(partition)
			task <- requireLeader(taskId)
			result <- task.append(depot, partition, request.value.toByteArray, AckLevel.Ack) // TODO configurable ack level
			// TODO: how to manage EvalStates
		} yield ???
	}

	override def query(request: QueryRequest, ctx: Metadata): IO[Response] = ???

	override def continue(request: ContinueRequest, ctx: Metadata): IO[Empty] = ???


	// TODO: precompute a map
	private def subscribers(depot: DepotId): List[Topology[?, ?]] =
		topologies.topologies.filter(_.builder.source == depot)
}
