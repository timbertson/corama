package corama.error

import cats.ApplicativeError

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

	def withContext[F[_], T](desc: => String, value: F[T])(implicit E: ApplicativeError[F, Throwable]): F[T] = {
		E.handleErrorWith(value) { e =>
			E.raiseError(new RuntimeException(desc, e))
		}
	}
}
