package corama.server

import streamdsl.rpc
import scala.collection.mutable
import cats.syntax.all.*
import corama.*
import corama.server.*

import scala.collection.mutable.ArrayBuffer
import cats.effect.SyncIO
import corama.internal.NodeId
import com.google.protobuf.ByteString
import cats.data.NonEmptyList
import java.time.Instant

// A mutable snapshot collecting updates (and serving a view), used for a single eval batch
// Unsafe (mutation), must not be accessed concurrently
// TODO: should this use Ref[SyncIO] for safety, or are the mutable collections more efficient?
final class EvalState(base: PStateView, val pendingAck: Option[ServerTask.PendingAck], val deadline: Instant) extends PStateView {
	val outputs = new EvalState.Outputs(Some(deadline))
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

	def toUpdate: ReplicationUpdate = ReplicationUpdate(
		depotAppends = depotAppends.toList.map { (id, partitioned) =>
			DepotAppends(id, partitioned.toArray)
		},
		pstateUpdates = pstateUpdates.map { (k, v) => PStateUpdate(k, v) }.toArray
	)
}

object EvalState {
	def apply(speculative: SpeculativeState, pendingAck: Option[ServerTask.PendingAck], deadline: Instant): SyncIO[EvalState] = {
		speculative.snapshot.map(new EvalState(_, pendingAck, deadline))
	}

	// effectful outputs which are handled after the data changes are persisted
	class Outputs(val deadline: Option[Instant]) {
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

		def addFailureNotification(origin: Origin, edge: EdgeId): SyncIO[Unit] = SyncIO {
			notificationBuffer.addOne(origin, new EvalNotification(edge, None, Agg.unit.asAny, failure = true))
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


case class EvalNotification(edge: EdgeId, value: Option[Any], agg: Agg[Any], failure: Boolean) {
	def merge(newEdge: EdgeId, newValue: Option[NonEmptyList[Any]]) = {
		copy(edge = edge.xor(newEdge), value = EvalNotification.mergeAgg(agg, value, newValue))
	}
}

object EvalNotification {
	def merge(current: Option[EvalNotification], edge: EdgeId, value: Option[NonEmptyList[Any]], agg: Agg[Any]) = {
		current.fold(
			EvalNotification(edge = edge, value = mergeAgg(agg, None, value), agg = agg, failure = false)
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
	import com.google.protobuf.timestamp.Timestamp
	def toProtobuf(c: EvalContinuation, deadline: Option[Instant]) = rpc.ContinueRequest.of(
		deadline = deadline.map(deadline => Timestamp.of(seconds = deadline.getEpochSecond(), nanos = deadline.getNano())),
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
						items = env.toList.map { (varId, value) =>
							println(s"Encoding ${value} with ${varId.codec} @ ${varId}")
							rpc.EnvItem.of(varId = varId.id, value = ByteString.copyFrom(varId.codec.asAny.encode(value)))
						}
					)
				}
			)
		}
	)
}
