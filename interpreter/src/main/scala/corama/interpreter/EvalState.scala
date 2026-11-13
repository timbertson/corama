package corama.interpreter

import scala.collection.mutable
import cats.syntax.all.*
import corama.*
import corama.interpreter.*

import scala.collection.mutable.ArrayBuffer
import cats.effect.SyncIO
import corama.internal.NodeId
import com.google.protobuf.ByteString
import corama.server.ServerTask
import cats.data.NonEmptyList

// A mutable snapshot collecting updates (and serving a view), used for a single eval batch
// Unsafe (mutation), must not be accessed concurrently
// TODO: should this use Ref[SyncIO] for safety, or are the mutable collections more efficient?
final class EvalState(base: PStateView, val pendingAck: Option[ServerTask.PendingAck]) extends PStateView {
	val outputs = new EvalState.Outputs()
	private val depotAppends = new mutable.HashMap[DepotId, mutable.ArrayBuffer[Partitioned[DepotAppend]]]()
	private val pstateUpdates = new mutable.HashMap[PStateKey, Option[Bytes]]()

	def append(depot: DepotId, partition: PartitionId, value: Bytes): SyncIO[Unit] = {
		SyncIO(depotAppends.getOrElseUpdate(depot, new ArrayBuffer())
			.append(Partitioned(partition, DepotAppend(value)))).void
	}

	def put(key: PStateKey, value: Option[Bytes]): SyncIO[Unit] = {
		SyncIO(pstateUpdates.put(key, value)).void
	}
	

	override def get(key: PStateKey) =
		pstateUpdates.get(key).fold(base.get(key))(SyncIO.pure)

	override def select(key: PStateKey, opts: SelectOptions.Raw): SyncIO[Vector[Bytes]] = SyncIO {
		???
	}

	def toUpdate(txId: LogIndex): ReplicationUpdate = ReplicationUpdate(
		txId,
		depotAppends = depotAppends.toList.map { (id, partitioned) =>
			DepotAppends(id, partitioned.toArray)
		},
		pstateUpdates = pstateUpdates.map { (k, v) => PStateUpdate(k, v) }.toArray
	)
}

object EvalState {
	def apply(speculative: SpeculativeState, pendingAck: Option[ServerTask.PendingAck]): SyncIO[EvalState] = {
		speculative.snapshot.map(new EvalState(_, pendingAck))
	}

	// effectful outputs which are handled after the data changes are persisted
	class Outputs {
		private val notificationBuffer = new mutable.HashMap[Origin, EvalNotification]()
		private val continuationBuffer = new mutable.ArrayBuffer[EvalContinuation]()
		private val evalSinkBuffer = new mutable.ArrayBuffer[LocalEvalSink[?]]()
		private val localResultsBuffer = new mutable.ArrayBuffer[(LocalEvalSink[Any], EdgeId, List[Any])]

		def notifications: Seq[(Origin, EvalNotification)] = notificationBuffer.toSeq

		def addNotification(source: EvalSource, edge: EdgeId, values: Option[NonEmptyList[Any]], agg: Agg[?]): SyncIO[Unit] =
			source match {
				case origin: Origin => SyncIO {
					val updated = EvalNotification.merge(notificationBuffer.get(origin), edge, values, agg.asAny)
					notificationBuffer.put(origin, updated)
				}.void

				// returning to ourself, just complete the result directly.
				// We can't complete the deferred immediately because it's in IO, just register for later
				// TODO this seems like a roundabout way to return values. Could we return them
				// from Eval directly?
				case local: LocalEvalSink[?] => SyncIO {
					// TODO should this store List or Option[Nel]?
					localResultsBuffer.addOne((local.asAny, edge, values.fold(Nil)(_.toList)))
				}
			}
		
		def localResults: Seq[(LocalEvalSink[Any], EdgeId, List[Any])] = localResultsBuffer.toSeq

		// when a computation is not constrained to this topology, evaluation
		// registers an eval sink. This will be exposed to the local GRPC interface,
		// and results from other nodes will be routed to it.
		def registerEvalSink(sink: LocalEvalSink[?]): SyncIO[Unit] = SyncIO {
			val _ = evalSinkBuffer.addOne(sink)
		}

		def registeredEvalSinks: Seq[LocalEvalSink[?]] = evalSinkBuffer.toSeq

		def addContinuation(cont: EvalContinuation): SyncIO[Unit] = SyncIO {
			continuationBuffer.addOne(cont)
		}.void

		def continuations: Seq[EvalContinuation] = continuationBuffer.toSeq
	}
}


case class EvalNotification(edge: EdgeId, value: Option[Any], agg: Agg[Any]) {
	def merge(newEdge: EdgeId, newValue: Option[NonEmptyList[Any]]) = {
		copy(edge = edge.xor(newEdge), value = EvalNotification.mergeAgg(agg, value, newValue))
	}
}

object EvalNotification {
	def merge(current: Option[EvalNotification], edge: EdgeId, value: Option[NonEmptyList[Any]], agg: Agg[Any]) = {
		current.fold(
			EvalNotification(edge = edge, value = mergeAgg(agg, None, value), agg = agg)
		) { current =>
			// agg / codec must be equivalent for given origin, so we can ignore them
			current.copy(edge = current.edge.xor(edge), value = mergeAgg(agg, current.value, value))
		}
	}

	private def mergeAgg[A](agg: Agg[A], current: Option[A], values: Option[NonEmptyList[A]]): Option[A] = {
		values.fold(current) { nel =>
			Some(nel.foldLeft(current.getOrElse(agg.zero))(agg.combineAny))
		}
	}
}

case class EvalContinuation(
	taskId: TaskId,
	origin: Origin,
	topologyId: TopologyId,
	afterNode: NodeId,
	partitions: NonEmptyList[Partitioned[(EdgeId, NonEmptyList[Env])]]
)

object EvalContinuation {
	import streamdsl.rpc
	def toProtobuf(c: EvalContinuation) = rpc.ContinueRequest.of(
		taskId = c.taskId.id,
		origin = Some(rpc.EvalOrigin.of(instanceId = c.origin.instanceId.id, evalId = c.origin.evalId.id)),
		topologyId = c.topologyId.value,
		afterNode = c.afterNode.id,
		partitions = c.partitions.toList.map { t =>
			val (edge, envs) = t.value
			rpc.PartitionEnvs.of(
				partitionId = t.partition.id,
				edgeId = edge.id,
				envs = envs.toList.map { env =>
					rpc.Env.of(
						items = env.toList.map { (key, value) =>
							rpc.EnvItem.of(varId = key.id, value = ByteString.copyFrom(key.codec.asAny.encode(value)))
						}
					)
				}
			)
		}
	)
}
