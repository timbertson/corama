package corama.server

import cats.syntax.all._
import java.util.UUID
import corama.InstanceId
import scala.util.Try
import java.net.ServerSocket
import cats.effect.IO
import cats.effect.kernel.Resource
import java.net.InetSocketAddress
import org.apache.ratis.protocol.RaftGroup
import org.apache.ratis.protocol.RaftPeer
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.RaftPeerId
import java.nio.charset.StandardCharsets
import corama.interpreter.TaskId
import cats.data.NonEmptyList
import corama.PartitionId
import corama.interpreter.Partitioned
import cats.effect.std.Random

case class PerInstance[T](default: Option[T], overrides: Map[InstanceId, T]) {
	def get(id: InstanceId): Try[T] = {
		overrides.get(id).orElse(default)
			.toRight(new RuntimeException(s"No default or override for instance ${id}"))
			.toTry
	}
	
	def getIO(id: InstanceId): IO[T] = IO.fromTry(get(id))
}

object PerInstance {
	def apply[T](overrides: Map[InstanceId, T]): PerInstance[T] = apply(None, overrides)
	def global[T](value: T): PerInstance[T] = apply(Some(value), Map.empty)
}

case class NetworkConfig(
	bindAddress: String,
	advertisedAddress: String,
	port: Int,
	adminPort: Int,
	grpcPort: Int,
) {
	def advertisedSocket = InetSocketAddress.createUnresolved(advertisedAddress, port)
	def advertisedAdminSocket = InetSocketAddress.createUnresolved(advertisedAddress, adminPort)
	def bindGrpcSocket = InetSocketAddress(bindAddress, grpcPort)
}

case class ClusterId(id: String)

case class ClusterInstance(id: InstanceId, tasks: NonEmptyList[TaskId]) {
	val peerId: RaftPeerId = RaftPeerId.valueOf(id.id)
}

case class ClusterConfig(
	id: ClusterId,
	tasks: NonEmptyList[TaskId],
	instances: NonEmptyList[ClusterInstance],
	networkConfig: PerInstance[NetworkConfig],
	numPartitions: Int,
	dataRoot: String,
) {
	private val numTasks = instances.size
	def raftGroup(taskId: TaskId): Try[RaftGroup] = {
		for {
			groupInstances <- NonEmptyList.fromList(instances.filter(_.tasks.contains_(taskId))).toRight(
				new RuntimeException(s"No instances for task: ${taskId}")
			).toTry
			peers <- groupInstances.traverse { instance =>
				networkConfig.get(instance.id).map { nc =>
					RaftPeer.newBuilder()
						.setId(RaftPeerId.valueOf(instance.id.id))
						.setAddress(nc.advertisedSocket)
						.setAdminAddress(nc.advertisedAdminSocket)
						.build()
				}
			}
		} yield RaftGroup.valueOf(groupId(taskId), peers.toList*)
	}

	def groupId(taskId: TaskId): RaftGroupId = {
		// deterministic a hash of the cluster and task IDs
		val bytes = s"${id.id}-${taskId.id}".getBytes(StandardCharsets.UTF_8)
		RaftGroupId.valueOf(UUID.nameUUIDFromBytes(bytes))
	}

	def taskFor(partition: PartitionId): TaskId = TaskId(partition.id % numTasks)
	
	def allPartitions = (0 until numPartitions).map(PartitionId.apply)

	def partitionsIn(task: TaskId): List[PartitionId] = {
		allPartitions.filter(p => taskFor(p) === task).toList
	}

	def partitionBatches[A](values: NonEmptyList[A], keyFn: A => Any): NonEmptyList[(TaskId, NonEmptyList[Partitioned[NonEmptyList[A]]])] = {
		val partitions = values.groupByNem(v => partitionKey(keyFn(v))).toNel
		val tasks = partitions.groupByNem(pair => taskFor(pair._1)).toNel
		tasks.map { case (taskId, partitionedList) =>
			val batch = partitionedList.map { case (partitionId, value) => Partitioned(partitionId, value) }
			(taskId, batch)
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

object ClusterConfig {
	private def genIds(numInstances: Int) =
		Range(0, numInstances).map(i => InstanceId(s"instance-$i")).toList

	// generate a random ephemeral cluster config on localhost. data files are removed on release
	def ephemeral(numTasks: Int = 2, numPartitions: Int = 2, numInstances: Int = 2): Resource[IO, ClusterConfig] = {
		tempDir().evalMap { dataRoot =>
			local(
				dataRoot = dataRoot.toString,
				clusterId = None,
				numInstances = numInstances,
				numTasks = numTasks,
				numPartitions = numPartitions
			)
		}
	}

	def persistentLocal(dataRoot: String, clusterId: Option[String] = None, numTasks: Int = 2, numPartitions: Int = 2, numInstances: Int = 2): IO[ClusterConfig] = {
		local(
			dataRoot = dataRoot,
			clusterId = clusterId,
			numInstances = numInstances,
			numTasks = numTasks,
			numPartitions = numPartitions
		)
	}

	private def local(
		dataRoot: String,
		clusterId: Option[String],
		numInstances: Int,
		numTasks: Int,
		numPartitions: Int,
	): IO[ClusterConfig] = {
		for {
			(tasks, instances) <- IO.fromTry(assignTasks(numTasks, genIds(numInstances)))
			networkConfig <- instances.traverse { instance =>
				// on localhost, each instance needs its own unique set of ports
				for {
					port <- freeLocalPort
					adminPort <- freeLocalPort
					grpcPort <- freeLocalPort
				} yield (instance.id -> NetworkConfig(
					"127.0.0.1",
					"127.0.0.1",
					port,
					adminPort,
					grpcPort,
				))
			}
		} yield ClusterConfig(
			id = ClusterId(clusterId.getOrElse("cluster")),
			tasks = tasks,
			numPartitions = numPartitions,
			instances = instances,
			networkConfig = PerInstance(networkConfig.toList.toMap),
			dataRoot = dataRoot,
		)
	}
	
	private def assignTasks(numTasks: Int, ids: List[InstanceId]): Try[(NonEmptyList[TaskId], NonEmptyList[ClusterInstance])] = {
		// each task is assigned to three consecutive instances, with wraparound
		// i.e. task 0 is assigned to instance 0, -1, & -2
		for {
			tasks <- NonEmptyList.fromList(Range(0, numTasks).map(TaskId.apply).toList)
				.toRight(new RuntimeException("numTasks cannot be 0")).toTry
			idNel <- NonEmptyList.fromList(ids)
				.toRight(new RuntimeException("instances cannot be empty")).toTry
			instances = idNel.zipWithIndex.map { (id, index) =>
				val tasks = NonEmptyList.of(index, index + 1, index + 2).map(i => TaskId(i % numTasks))
				ClusterInstance(id, tasks)
			}
		} yield (tasks, instances)
	}
	
	private def freeLocalPort: IO[Int] = {
		IO {
			val socket = ServerSocket(0)
			val port = socket.getLocalPort()
			socket.close()
			port
		}
	}

	def tempDir(desc: String = ""): Resource[IO, os.Path] =
		Resource.make(IO.blocking(os.temp.dir(prefix = s"corama-$desc")))(deleteRecursive)

	private def deleteRecursive(root: os.Path): IO[Unit] = IO.blocking(os.remove.all(root))
}
