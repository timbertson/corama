package corama.server

import cats.effect.IO
import corama.InstanceId
import corama.interpreter.TaskId

trait InstanceTaskLeadership {
	val taskId: TaskId

	def isLeader: IO[Boolean]

	def currentLeader: IO[Option[InstanceId]]

	def requireCurrentLeader: IO[InstanceId] =
		InstanceTaskLeadership.requireCurrentLeader(taskId, currentLeader)
}

object InstanceTaskLeadership {
	def requireCurrentLeader(taskId: TaskId, currentLeader: IO[Option[InstanceId]]) = {
		currentLeader.flatMap { leader =>
			IO.fromEither(leader.toRight(new RuntimeException(s"No current leader for task ${taskId.id}")))
		}
	}
}
