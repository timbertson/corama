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
import corama.interpreter.Partitioned
import corama.interpreter.EdgeId
import corama.internal.NodeId
import streamdsl.rpc.NotifyRequest
import corama.interpreter.IncomingEvalRouter

class CoramaController(
	config: ClusterConfig,
	topologies: Topologies,
	incomingEvalRouter: IncomingEvalRouter,
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
			taskId = config.taskFor(partition)
			task <- requireLeader(taskId)
			result <- task.append(TaskCommand.Append(depot, partition, bytes, decoded, AckLevel.Ack)) // TODO configurable ack level
			serialized = AckReturns.codec(topologies).encode(result)
		} yield Response(Response.Value.Success(ByteString.copyFrom(serialized)))
	}

	override def query(request: QueryRequest, ctx: Metadata): IO[Response] = ???

	override def notify(request: NotifyRequest, ctx: Metadata): IO[Empty] = {
		incomingEvalRouter.invoke(
			EvalId(request.evalId),
			EdgeId(request.edge),
			request.value.map(_.toByteArray())
		).as(Empty())
	}
	
	override def continue(request: ContinueRequest, ctx: Metadata): IO[Empty] = {
		import Util.*
		val topologyId = TopologyId(request.topologyId)
		val taskId = TaskId(request.taskId)
		for {
			task <- requireLeader(taskId)
			topology <- IO.fromTry(topologies.get(topologyId))
			originProto <- require("origin", request.origin)
			origin = Origin(InstanceId(originProto.instanceId), EvalId(originProto.evalId))
			partitions <- IO.fromTry {
				requireNel("partitions", request.partitions.toList).flatMap { partitions =>
					partitions.traverse { partitionEnvs =>
						requireNel("partitionEnvs", partitionEnvs.envs.toList).flatMap { envs =>
							envs.traverse { env =>
								env.items.toList.traverse { item =>
									topology.builder.vars.lookup(item.varId).flatMap { v =>
										v.codec.decode(item.value.toByteArray()).map(v.vid -> _)
									}
								}.map(_.toMap)
							}.map { envs =>
								val edgeId = EdgeId(partitionEnvs.edgeId)
								val partitionId = PartitionId(partitionEnvs.partitionId)
								Partitioned(partitionId, (edgeId, envs))
							}
						}
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
