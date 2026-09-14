package corama.logging

import cats.effect.IO
import flogger.*
import flogger.api.Level
import java.util as ju

val noopLogger: LogOutput[IO] = new LogOutput[IO] {

  override val name: String = "noop"

  override def log(level: Level, ctx: ju.Map[String, String], msg: => String): IO[Unit] = IO.unit

  override def log(level: Level, ctx: ju.Map[String, String], msg: => String, error: Throwable): IO[Unit] = IO.unit

  override def isEnabledFor(level: Level): IO[Boolean] = IO.pure(false)
}
