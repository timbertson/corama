package corama.server

import cats.syntax.all._
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
import corama.AckReturns
import com.google.protobuf.ByteString
import corama.TopologyId
import corama.server.TaskCommand.Append
import corama.interpreter.EvalId
import corama.interpreter.Origin
import corama.InstanceId
import corama.VarId
import corama.interpreter.Partitioned
import corama.interpreter.EdgeId
import corama.internal.NodeId

class CoramaController(
	metadata: ClusterMetadata,
	topologies: Topologies,
	tasks: IOMap[TaskId, ServerTask],
) extends streamdsl.rpc.CoramaNodeFs2Grpc[IO, Metadata] {
	private def requireLeader(id: TaskId): IO[ServerTask] = tasks.use(id) { serverTask =>
		serverTask.requireLeader.as(serverTask)
	}

	override def append(request: AppendRequest, ctx: Metadata): IO[Response] = {
		val depotId = DepotId(request.depot)
		for {
			depot <- IO.fromEither(topologies.depots.get(depotId).toRight(Util.err("No such depot")))
			bytes = request.value.toByteArray
			// TODO: we could skip this, but it gives some protection from invalid bytes. Pass both through?
			decoded <- IO.fromTry(depot.codec.decode(bytes))
			partition <- IO.fromEither(request.partition.toRight(Util.err("Partition required"))).map(PartitionId.apply)
			taskId = metadata.taskFor(partition)
			task <- requireLeader(taskId)
			result <- task.append(TaskCommand.Append(depot, partition, bytes, decoded, AckLevel.Ack)) // TODO configurable ack level
			serialized = AckReturns.codec(topologies).encode(result)
		} yield Response(Response.Value.Success(ByteString.copyFrom(serialized)))
	}

	override def query(request: QueryRequest, ctx: Metadata): IO[Response] = ???
	
	private def require[A](name: String, v: Option[A]): IO[A] =
		IO.fromEither(v.toRight(new RuntimeException(s"Missing property: $name")))

	override def continue(request: ContinueRequest, ctx: Metadata): IO[Empty] = {
		val topologyId = TopologyId(request.topologyId)
		val taskId = TaskId(request.taskId)
		for {
			task <- requireLeader(taskId)
			topology <- IO.fromTry(topologies.get(topologyId))
			originProto <- require("origin", request.origin)
			origin = Origin(InstanceId(originProto.instanceId), EvalId(originProto.evalId))
			partitions <- IO.fromTry {
				request.partitions.toList.traverse { partitionEnvs =>
					partitionEnvs.envs.toList.traverse { env =>
						env.items.toList.traverse { item =>
							val vid: VarId = ???
							vid.codec.decode(item.value.toByteArray()).map(vid -> _)
						}.map(_.toMap)
					}.map { envs =>
						val edgeId = EdgeId(partitionEnvs.edgeId)
						val partitionId = PartitionId(partitionEnvs.partitionId)
						Partitioned(partitionId, (edgeId, envs))
					}
				}
			}
			continuation <- IO.fromTry(topology.continuationAfter(NodeId(request.afterNode)))
			_ <- task.continue(TaskCommand.Continue(
				origin,
				topology.id,
				continuation,
				partitions
			))
		} yield Empty()
	}
}
