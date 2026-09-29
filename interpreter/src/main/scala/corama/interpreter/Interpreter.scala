package corama.interpreter

import cats.syntax.all.*
import cats.data.NonEmptyList
import cats.effect.IO
import corama.*
import corama.error.Util.*
import corama.internal.*
import corama.interpreter.Env
import izumi.reflect.Tag

import flogger.*
import cats.effect.Resource
import cats.effect.kernel.Deferred

class Interpreter(
	val topologies: Topologies,
	metadata: ClusterMetadata,
	val getInstances: IO[Vector[ClusterInstance]]
)(using Log[IO]) extends Router with IOLogging {
	import Interpreter._

	def leader(taskId: TaskId): IO[ClusterInstance] = {
		getInstances.map(_(taskId.id))
	}
	
	def partition(partitionId: PartitionId): IO[Partition] = {
		val taskId = metadata.taskFor(partitionId)
		leader(taskId).flatMap(_.get(taskId, partitionId))
	}

	def instance(instanceId: InstanceId): IO[ClusterInstance] = {
		getInstances.flatMap { instances =>
			IO.fromEither(instances.find(_.instance === instanceId).toRight(err(s"Unknown instance: $instanceId")))
		}
	}

	val run: IO[Nothing] = {
		getInstances.flatMap(_.parTraverse_(_.run)) *> neverReached
	}
	
	private def accessPath[V](pstate: corama.PState, path: NonEmptyList[Any]): IO[Storage.Lens[V]] =
		partition(metadata.partitionKey(path.head)).map(_.access(pstate, path))

	def access[K, V](pstate: corama.PStateKV[K, V], key: K): IO[Storage.Lens[V]] =
		accessPath[V](pstate.inner, NonEmptyList.one(key))

	def access[K, V](pstate: corama.PStateKVSet[K, V], key: K): IO[Storage.Lens[Set[V]]] =
		accessPath[Set[V]](pstate.inner, NonEmptyList.one(key))

	def access[V: Tag](pstate: corama.PState, root: Any, path: Any*): IO[Storage.Lens[V]] =
		accessPath[V](pstate, NonEmptyList(root, path.toList))

	def getKeys[K, V](pstate: corama.PStateKV[K, V]): IO[Set[K]] =
		metadata.allPartitions.toList.flatTraverse { partitionId =>
			partition(partitionId).flatMap(_.getKeys(pstate).map(_.toList))
		}.map(_.toSet)

	def emit[A](depot: corama.Depot[A], value: A): IO[Unit] =
		// TODO use the depot's partitioning strategy
		partition(metadata.partitionKey(value)).flatMap(_.emit(depot, value))

	/** Topologies driven by this depot — used to populate [[TopologiesResults]] */
	private[corama] def subscribers(depot: Identity): List[Topology[?, ?]] =
		topologies.topologies.filter(_.builder.source == depot)

	def append[A, R](depot: corama.Depot[A], value: A, level: AckLevel[R] = AckLevel.Ack)(using log: Log[IO]): IO[R] =
		level match {
			// TODO: these branches should differ when we model replication
			case AckLevel.NoAck => emit(depot, value)
			case AckLevel.AppendAck => emit(depot, value)

			case AckLevel.Ack =>
				for {
					results <- EvalResults(subscribers(depot.id))
					// TODO use the depot's partitioning strategy
					partition <- partition(metadata.partitionKey(value))
					_ <- partition.spawn(depot, value, results)
					_ <- log.debug(s"append: awaiting results ${results}")
					raw <- results.await
					_ <- log.debug(s"append: Got raw results: ${raw}")
				} yield new AckReturns(raw, topologies)
		}

	def readDepot[A](depot: corama.Depot[A]): IO[Vector[A]] =
		metadata.allPartitions.toVector.flatTraverse { partitionId =>
			partition(partitionId).flatMap(_.readDepot(depot))
		}

	/** Invoke a query topology, as a client would.
		*/
	def invoke[S, A, R](q: QueryTopology[S, R], value: S)(using Log[IO]): IO[R] = {
		for {
			partitionId <- metadata.randomPartition
			partition <- partition(partitionId)
			result <- partition.invoke(q, value)
		} yield result
	}

	// Router implementation
	override def continue(
		task: TaskId,
		topologyId: TopologyId,
		afterNode: NodeId,
		edge: EdgeId,
		envs: PartitionBatch[Env],
		origin: Origin,
	): IO[Unit] = {
		leader(task).flatMap(_.continue(task, topologyId, afterNode, edge, envs, origin))
	}

	override def remoteResults[R](topology: Topology[?, ?], continuation: Continuation, origin: Origin)(using log: Log[IO]): EvalSink[R] = {
		val (outVarId, agg) = continuation match {
			case _: Continuation.Basic => (topology.output.vid, topology.agg)
			case c: Continuation.WithinAggregation => (c.out, c.agg)
		}

		new EvalSink[R] {
			override val varId: VarId = outVarId

			override def notify(edge: EdgeId, value: Option[R]): IO[Unit] = {
				log.debug(s"Notifying $edge|${value.fold("None")(_ => "Some")}") *>
					instance(origin.instanceId).flatMap(_.receivers.invoke(origin, edge, value))
			}

			override def completeList(edge: EdgeId, values: List[R]): IO[Unit] =
				notify(edge, Option.when(values.nonEmpty) {
					(agg.asInstanceOf[Agg[R]]).merge(values)
				})
		}
	}
}

object Interpreter {
	def createCluster(
		topologies: Topologies,
		instances: Vector[InstanceId],
		numPartitions: Int,
	)(using Log[IO]): ActiveResource[IO, Interpreter] =
		ActiveResource(
			Resource.eval(ClusterMetadata(instances, numPartitions))
				.flatMap(metadata => apply(topologies, metadata).resource)
		)

	def apply(topologies: Topologies, metadata: ClusterMetadata)(using baseLog: Log[IO]): ActiveResource[IO, Interpreter] = {
		ActiveResource(for {
			instancesRef <- Resource.eval(Deferred[IO, Vector[ClusterInstance]])
			interpreter = new Interpreter(topologies, metadata, instancesRef.get)
			clusterInstances <- metadata.instances.traverse { instance =>
				EvalReceivers(instance).flatMap { receivers =>
					val log = baseLog.addContext("instance" -> instance.id)
					Resource.eval(ClusterInstance(instance, topologies, metadata, interpreter, receivers, log))
				}
			}
			_ <- Resource.eval(instancesRef.complete(clusterInstances))
		} yield (interpreter, interpreter.run))
	}
}
