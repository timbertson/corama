package corama.interpreter

import cats.effect.std.Random
import cats.syntax.all.*
import cats.effect.IO
import cats.effect.kernel.Ref
import corama.*
import corama.internal.*
import corama.interpreter.Env

import flogger.*
import cats.effect.Resource
import corama.error.Util.err

// The partitions this instance is leader for
class ClusterInstance(
	val instance: InstanceId,
	topologies: Topologies,
	metadata: ClusterMetadata,
	val receivers: EvalReceivers,
	router: Router,
	partitions: Ref[IO, Map[PartitionId, Partition]],
	log: Log[IO],
) {
	def continue(
		task: TaskId,
		topologyId: TopologyId,
		afterNode: NodeId,
		edge: EdgeId,
		envs: PartitionBatch[Env],
		origin: Origin,
	): IO[Unit] = {
		for {
			topology <- IO.fromTry(topologies.get(topologyId))
			continuation <- IO.fromTry(topology.continuationAfter(afterNode))
			results = router.remoteResults(topology, continuation, origin)(using log)
			newEdges <- envs.value.parTraverse { case (partitionId, envs) =>
				for {
					newEdge <- EdgeId.generate
					partition <- get(task, partitionId)
					// TODO: continuing can throw, where does that go?
					_ <- partition.continue(origin, topologyId, results, newEdge, continuation.nodes, envs)
				} yield newEdge
			}
			_ <- results.notify(EdgeId.union(edge :: newEdges), None)
		} yield ()
	}

	def get(task: TaskId, partition: PartitionId): IO[Partition] = {
		if (metadata.taskFor(partition) === task) {
			metadata.leaderFor(task).flatMap { leader =>
				if (leader.instance === instance) {
					partitions.modify { map =>
						map.get(partition) match {
							case None => {
								val newPartition = new Partition(receivers, metadata, topologies, router, log.addContext("partition" -> partition.id.toString))
								(map.updated(partition, newPartition), newPartition)
							}
							case Some(p) => (map, p)
						}
					}
				} else {
					IO.raiseError(new MetadataError(instance, s"leadership for task $task is currently $leader"))
				}
			}
		} else {
			IO.raiseError(new RuntimeException(s"$partition is not owned by $task"))
		}
	}
	
	// TODO: respond /guard against changes in leadership.
	// shutdown unused partitions, and start newly assigned ones
	def run = {
		metadata.leaders.get.flatMap { leaders =>
			leaders.toList.mapFilter { case (task, leadership) =>
				Option.when(leadership.instance === instance)(task)
			}.parTraverse_ { taskId =>
				metadata.partitionsIn(taskId).parTraverse_ { partitionId =>
					this.get(taskId, partitionId).flatMap(_.run)
				}
			}
		}
	}
}

object ClusterInstance {
	def apply(
		instance: InstanceId,
		topologies: Topologies,
		metadata: ClusterMetadata,
		router: Router,
		receivers: EvalReceivers,
		log: Log[IO],
	) = Ref[IO].of(Map.empty[PartitionId, Partition]).map { partitionMap =>
		new ClusterInstance(instance, topologies, metadata, receivers, router, partitionMap, log)
	}
}

// manages incoming eval messages for a single instance
class EvalReceivers(instance: InstanceId, slots: Ref[IO, Map[EvalId, LocalEvalSink[Any]]])(using Random[IO]) {
	// not exposing a bare resource so we can eagerly terminate the block if the server has too many outstanding evals
	def addRunning[A, B](destination: LocalEvalSink[A])(f: Origin => IO[B]): IO[B] = {
		val acquire = for {
			evalId <- EvalId.generate
			origin = Origin(instance, evalId)
			_ <- slots.update(_.updated(evalId, destination.asInstanceOf[LocalEvalSink[Any]]))
		} yield origin
		Resource.make(acquire) { origin =>
			slots.update(_.removed(origin.evalId))
		}.use(f)
	}
	
	def invoke(origin: Origin, edge: EdgeId, value: Option[Any]): IO[Unit] = {
		for {
			map <- slots.get
			slot <- map.get(origin.evalId).fold(IO.raiseError(err(s"No slot for ${origin}")))(IO.pure)
			_ <- slot.notify(edge, value)
		} yield ()
	}
}

object EvalReceivers {
	def apply(instance: InstanceId): Resource[IO, EvalReceivers] = {
		val acquire = Ref[IO].of(Map.empty[EvalId, LocalEvalSink[Any]]).map(new EvalReceivers(instance, _))
		Resource.make(acquire)(_ => IO.unit)
	}
}
