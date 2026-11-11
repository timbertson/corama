package corama.error

import cats.ApplicativeError
import cats.effect.IO
import cats.data.NonEmptyList
import scala.util.Try

object Util {
	// private [corama] def formatError(e: Throwable) = {
	// 	formatErrorLines(e).mkString("\n")
	// }

	// private [ramato] def formatErrorLines(e: Throwable): List[String] = {
	// 	s"${e.getClass().getName()}: ${Option(e.getMessage).getOrElse("")}" ::
	// 		e.getStackTrace().map(_.toString()).toList ++
	// 		Option(e.getCause()).toList.flatMap { cause =>
	// 			"Caused by:" :: formatErrorLines(cause)
	// 		}
	// }

	def err(s: String) = new RuntimeException(s)

	def neverReached: IO[Nothing] =
		IO.raiseError(new IllegalStateException("neverReached: this should be impossible"))

	def joinNothings(tasks: List[IO[Nothing]]) = tasks.parSequence_ *> neverReached

	def withContext[F[_], T](desc: => String, value: F[T])(implicit E: ApplicativeError[F, Throwable]): F[T] = {
		E.handleErrorWith(value) { e =>
			E.raiseError(new RuntimeException(desc, e))
		}
	}
	
	def requireNel[A](desc: String, list: List[A]): Try[NonEmptyList[A]] = {
		NonEmptyList.fromList(list).toRight(err(s"${desc} is empty")).toTry
	}

	def require[A](name: String, v: Option[A]): IO[A] =
		IO.fromEither(v.toRight(new RuntimeException(s"Missing property: $name")))

}
