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
import streamdsl.rpc.AppendRequest
import streamdsl.rpc.QueryRequest
import corama.PartitionId
import corama.AckReturns
import com.google.protobuf.ByteString
import corama.TopologyId
import corama.server.TaskCommand.Append
import corama.InstanceId
import corama.internal.NodeId
import streamdsl.rpc.NotifyRequest
import flogger.Log
import flogger.IOLogging
import java.time.Instant

class InstanceController(
	config: ClusterConfig,
	topologies: Topologies,
	incomingEvalRouter: IncomingEvalRouter,
	tasks: IOMap[TaskId, ServerTask],
)(using log: Log[IO]) extends streamdsl.rpc.CoramaInstanceFs2Grpc[IO, Metadata] with IOLogging {
	private def requireLeader(id: TaskId): IO[ServerTask] = tasks.use(id) { serverTask =>
		serverTask.requireLeader.as(serverTask)
	}
	
	private def logErrors[A](desc: => String)(io: IO[A]): IO[A] = {
		io.handleErrorWith { e =>
			log.warn(s"error $desc", e) *> IO.raiseError(e)
		}
	}

	override def append(request: AppendRequest, ctx: Metadata): IO[Response] = {
		logErrors(s"appending to ${request.depot}") {
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
			} yield Response.of(value = Some(ByteString.copyFrom(serialized)))
		}
	}

	override def query(request: QueryRequest, ctx: Metadata): IO[Response] = ???

	override def notify(request: NotifyRequest, ctx: Metadata): IO[Empty] = {
		logErrors(s"notifying evalId ${request.evalId}") {
			incomingEvalRouter.invoke(
				EvalId(request.evalId),
				EdgeId(request.edge),
				request.value.map(_.toByteArray())
			).as(Empty())
		}
	}
	
	override def continue(request: ContinueRequest, ctx: Metadata): IO[Empty] = {
		logErrors(s"contining evalId ${request.origin.map(_.evalId)}") {
			import Util.*
			val topologyId = TopologyId(request.topologyId)
			val taskId = TaskId(request.taskId)
			for {
				task <- requireLeader(taskId)
				topology <- IO.fromTry(topologies.get(topologyId))
				originProto <- require("origin", request.origin)
				deadlineProto <- require("deadline", request.deadline)
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
					Instant.ofEpochSecond(deadlineProto.seconds, deadlineProto.nanos),
					origin,
					topology.id,
					continuation,
					partitions
				))
			} yield Empty()
		}
	}
}
