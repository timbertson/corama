package corama.server

import cats.MonadError
import cats.syntax.all.*
import io.grpc.StatusRuntimeException
import io.grpc.StatusException
import io.grpc.Status

object GrpcError {
	def wrap[F[_], A](block: F[A])(using err: MonadError[F, Throwable]): F[A] = {
		block.handleErrorWith { t =>
			err.raiseError(t match {
				case _: StatusRuntimeException | _: StatusException => t
				case other: Throwable =>
					// TODO: awry
					StatusRuntimeException.apply(Status.Code.UNKNOWN.toStatus().withDescription(format(t)))
			})
		}
	}
	
	def format(t: Throwable): String = {
		val base = s"${t.getClass.getSimpleName()}: ${t.getMessage}\n at ${t.getStackTrace().mkString("\n")}"
		Option(t.getCause()).fold(base) { cause =>
			s"${base}\ncaused by:\n${format(cause)}"
		}
	}
}
