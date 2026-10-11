package corama.interpreter

import cats.syntax.all._
import weaver.Expectations
import cats.Show
import flogger.Log
import cats.effect.IO
import cats.effect.testkit.TestControl
import flogger.IOLogging
import weaver.scalacheck.Checkers
import weaver.SimpleIOSuite
import cats.Monoid
import flogger.LogOutput
import java.{util => ju}
import flogger.api.Level
import cats.effect.Ref
import scala.collection.immutable.Queue
import cats.effect.unsafe.IORuntimeConfig
import org.scalacheck.Gen
import weaver.scalacheck.CheckConfig

abstract class CoramaSuite extends SimpleIOSuite with Checkers {
	override def checkConfig: CheckConfig = super.checkConfig.withMinimumSuccessful(500)
}

object TestUtil extends IOLogging {
	def expectSeq(expectations: Seq[Expectations]): Expectations = {
		if (expectations.isEmpty) {
			Monoid[Expectations].empty // "success", but accessible outside the helpers trait
		} else {
			expectations.reduce(_ and _)
		}
	}
	def expectAll(expectations: Expectations*): Expectations = expectSeq(expectations)

	class Hint[A](value: A) {
		override def toString(): String = value.toString()
		def as[B](b: B) = b
	}

	// usage: expect(clue(hint("unrelated")).as(items).length == 1)
	def hint[A](value: A) = new Hint(value)

	implicit def showAny[A]: Show[A] = Show.fromToString
	
	class LogBuffer(underlying: Log[IO]) {
		val ref = Ref.unsafe[IO, Queue[(Level, ju.Map[String, String], String, Option[Throwable])]](Queue.empty)
		
		def flush: IO[Unit] = {
			val output = underlying.output
			ref.get.flatMap { entries =>
				entries.traverse_ {
					case (level, ctx, msg, err) => err match {
						case Some(err) => output.log(level, ctx, msg, err)
						case None => output.log(level, ctx, msg)
					}
				}
			}
		}
	}

	object LogBuffer {
		class LogOutputWrapper(ref: LogBuffer, underlying: LogOutput[IO]) extends LogOutput[IO] {
			override val name: String = underlying.name

			override def log(level: Level, ctx: ju.Map[String, String], msg: => String): IO[Unit] = {
				ref.ref.update(_.appended((level, ctx, msg, None)))
			}

			override def log(level: Level, ctx: ju.Map[String, String], msg: => String, error: Throwable): IO[Unit] = {
				ref.ref.update(_.appended((level, ctx, msg, Some(error))))
			}

			override def isEnabledFor(level: Level): IO[Boolean] = underlying.isEnabledFor(level)
		}

		class LogWrapper(ref: LogBuffer, underlying: Log[IO]) extends Log[IO] {
			override def output(implicit implicitOutput: LogOutput[IO]): LogOutput[IO] =
				new LogOutputWrapper(ref, underlying.output(using implicitOutput))

			override def unsafeContext: ju.Map[String, String] = underlying.unsafeContext

			override def addContext(ctx: (String, String)*): Log[IO] = {
				new LogWrapper(ref, underlying.addContext(ctx*))
			}
		}
	}

	class RunSyncFn(seed: String) extends Expectations.Helpers {
		def apply(io: Log[IO] ?=> IO[Expectations])(using baseLog: Log[IO]): Expectations = {
			import cats.effect.unsafe.implicits.global

			val ref = new LogBuffer(baseLog)
			given log: Log[IO] = new LogBuffer.LogWrapper(ref, baseLog)
			val fullTask: IO[Expectations] = {
				log.info(s"-- starting test case") *> io.attempt.flatMap {
					case Left(err) => ref.flush *> IO.raiseError(err)
					case Right(expectations) => {
						IO.whenA(expectations.run.isInvalid) {
							ref.flush
						}.as(expectations)
					}
				}
			}

			val runtimeConfig = IORuntimeConfig(
				cancelationCheckThreshold = 1,
				autoYieldThreshold = 2, // must be >1
			)
			try {
				TestControl.executeEmbed(fullTask, seed = Some(seed), config = runtimeConfig).unsafeRunSync()
			} catch {
				case e => {
					ref.flush.unsafeRunSync()
					// throw(e)
					failure(e.toString())
				}
			}
		}
	}
	
	object RunSync {
		val gen: Gen[RunSyncFn] = Gen.long.map { l =>
			val seed = java.util.Base64.getEncoder().encodeToString(l.toString().getBytes())
			new RunSyncFn(seed)
		}
	}
}
