package corama.interpreter

import cats.effect.std.Random
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import corama.*
import corama.internal.*
import corama.interpreter.Env
import fs2.Stream

case class PartitionBatch[A](value: List[(PartitionId, List[A])])

case class Origin(
	instanceId: InstanceId,
	evalId: EvalId, // unique key for this specific append / query root
)

/*
The executor receives partitioned continuations and routes them to the current leader.
This is implemented by the GRPC service to a _specific_ peer, where it fails if the peer is
not leader for the task.

It's also implemented by the task router, in which case it finds the appropriate peer to invoke.

Execution continues *after* the provided nodeId, since that's trivial to provide & serialize.
*/
trait TaskExecutor {
	def continue(
		task: TaskId,
		topologyId: TopologyId,
		afterNode: NodeId,
		edge: EdgeId,
		envs: PartitionBatch[Env],
		origin: Origin,
	): IO[Unit]
}

// EvalId is dynamically minted each time we perform a fanout or repartitioning
case class EvalId(id: Long)
object EvalId {
	def generate(using rand: Random[IO]): IO[EvalId] = rand.nextLong.map(EvalId.apply)
}

case class Epoch(value: Long)
case class Leadership(instance: InstanceId, epoch: Epoch)
class ClusterMetadata(
	val leaders: Ref[IO, Map[TaskId, Leadership]],
	val instances: Vector[InstanceId], // for now numTasks is pinned to numInstances for simplicity
	numPartitions: Int, // should be a multiple of numTasks for balanced workloads
) {
	private val numTasks = instances.size
	private val taskDiffs = List(0, 1, 2)
	private def wrapTaskIndex(i: Int) = i % numTasks
	assert(numTasks >= 3)

	def taskFor(partition: PartitionId): TaskId = TaskId(partition.id % numTasks)
	def instanceFor(taskId: TaskId): InstanceId = instances(taskId.id)
	def add(taskId: TaskId, diff: Int): TaskId = TaskId(wrapTaskIndex(taskId.id + diff))
	
	private def allPartitions = (0 until numPartitions).map(PartitionId.apply)

	def partitionsIn(task: TaskId): List[PartitionId] = {
		allPartitions.filter(p => taskFor(p) === task).toList
	}
	
	// replicas are statically assigned so that e.g. taskId(2) is replicated on instance id 2, 3, and 4 (wrapping around)
	def replicasFor(task: TaskId) = taskDiffs.map { diff =>
		instanceFor(add(task, diff))
	}

	// TODO replicasFor, tasksFor and partitionsFor could probably all be one calculation / structure
	// def tasksFor(instanceId: InstanceId) = {
	// 	Range.exclusive(0, numTasks).map(TaskId.apply).filter { taskId => replicasFor(taskId).contains(instanceId) }
	// }

	// def partitionsFor(task: TaskId) = {
	// 	Range.exclusive(0, numPartitions).map(PartitionId.apply).filter { partitionId => taskFor(partition) === task }
	// }

	def partitionBatches[A](values: List[A], keyFn: A => Any): List[(TaskId, PartitionBatch[A])] = {
		values.groupBy(v => partitionKey(keyFn(v))).toList.map { case (p, items) =>
			(p, items)
		}.groupBy(pair => taskFor(pair._1)).toList.map { case (taskId, partitionedList) =>
			(taskId, PartitionBatch(partitionedList))
		}
	}

	def partitionKey(value: Any) = PartitionId(rawPartitionKey(value))

	def randomPartition(using rnd: Random[IO]): IO[PartitionId] = {
		rnd.nextIntBounded(numPartitions).map(PartitionId.apply)
	}

	// TODO make much better
	private def rawPartitionKey(value: Any) = {
		val mod = value.hashCode() % numPartitions
		if (mod < 0) {
			mod + numPartitions
		} else {
			mod
		}
	}
}

object ClusterMetadata {
	def apply(
		instances: Vector[InstanceId], // for now numTasks is pinned to numInstances for simplicity
		numPartitions: Int, // should be a multiple of numTasks for balanced workloads
	): IO[ClusterMetadata] = {
		val defaultLeaders = instances.zipWithIndex.map { case (instance, idx) =>
			// each instance owns its corresponding task, until we have an actual leadership algo
			TaskId(idx) -> Leadership(instance, Epoch(0L))
		}.toMap
		Ref[IO].of(defaultLeaders).map { leaders =>
			new ClusterMetadata(leaders, instances, numPartitions)
		}
	}
}

trait Follower {
	def follow(updates: Stream[IO, ReplicationUpdate]): Stream[IO, ReplicationAck]
}
