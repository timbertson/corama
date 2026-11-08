package corama.interpreter

import cats.effect.std.Random
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import corama.*
import corama.internal.*
import fs2.Stream
import flogger.Log
import corama.error.Util.err
import scala.util.control.NoStackTrace

// continue evaluation on another task
trait TaskRouter {
	def continue(
		task: TaskId,
		topologyId: TopologyId,
		afterNode: NodeId,
		edge: EdgeId,
		// envs: PartitionBatch[Env],
		origin: Origin,
	): IO[Unit]
}

// send eval results to origin
trait EvalRouter {
	def remoteResults[R](
		topology: Topology[?, ?],
		continuation: Continuation,
		origin: Origin
	)(using Log[IO]): EvalSink[R]
}

trait Router extends TaskRouter with EvalRouter

case class Epoch(value: Long)
case class Leadership(instance: InstanceId, epoch: Epoch)
class MetadataError(instance: InstanceId, message: String) extends RuntimeException(s"$instance: $message") with NoStackTrace

trait Follower {
	def follow(updates: Stream[IO, ReplicationUpdate]): Stream[IO, ReplicationAck]
}
