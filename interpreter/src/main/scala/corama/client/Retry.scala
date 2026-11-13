package corama.client

import scala.concurrent.duration.*
import cats.effect.IO
import retry.RetryPolicy
import retry.RetryPolicies
import retry.ResultHandler
import flogger.Log
import flogger.IOLogging

case class RetryConfig(
	limit: Int,
	interval: FiniteDuration,
	backoffFactor: Float,
)

object Retry extends IOLogging {
	val default: RetryPolicy[IO, Any] =
		RetryPolicies.fibonacciBackoff[IO](1.second).join(RetryPolicies.limitRetries[IO](10))

	def apply[A](config: RetryPolicy[IO, Any])(block: IO[A])(using log: Log[IO]): IO[A] = {
		retry.retryingOnErrors(block)(
			config,
			ResultHandler.retryOnAllErrors[IO, A](log = (e, details) =>
				log.info(s"Retrying request (retries:${details.retriesSoFar}, delay:${details.cumulativeDelay.toMillis}ms): $e")
			)
		)
	}

	def apply[A](block: IO[A])(using log: Log[IO]): IO[A] = apply(default)(block)
}
