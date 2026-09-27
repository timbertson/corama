package corama.interpreter

import cats.syntax.all._
import weaver.Expectations
import cats.Show
import flogger.Log
import cats.effect.IO
import cats.effect.testkit.TestControl
import java.util.concurrent.atomic.AtomicInteger
import flogger.IOLogging
import weaver.scalacheck.Checkers
import weaver.SimpleIOSuite
import cats.Monoid

abstract class CoramaSuite extends SimpleIOSuite with Checkers with RunSync

trait RunSync {
	import TestUtil.*
	val runSync = new RunSyncFn
	// extension (f: PartiallyAppliedForall) {
	// 	def io[A](gen: Gen[A], config: CheckConfig = checkConfig)
	// 		(body: A => IO[Expectations])(using Show[A])(using Log[IO]): IO[Expectations] =
	// 	{
	// 		val runner = new IORunner
	// 		f.withConfig(config)(gen) { v =>
	// 			runner.runSync(body(v))
	// 		}
	// 	}
	// }

	// def forallIO[A]
	// 	(gen: Gen[A], config: CheckConfig = checkConfig)
	// 	(body: A => IO[Expectations])(using Show[A])(using Log[IO]): IO[Expectations] = {
	// 	val runner = new IORunner
	// 	forall.withConfig(config)(gen) { v =>
	// 		runner.runSync(body(v))
	// 	}
	// }
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

	class RunSyncFn {
		private val runNoCounter = new AtomicInteger(0)

		def apply[A](io: Log[IO] ?=> IO[A])(using baseLog: Log[IO]): A = {
			import cats.effect.unsafe.implicits.global
			val runNo = runNoCounter.incrementAndGet()
			given log: Log[IO] = baseLog.addContext("run" -> runNo.toString())

			val fullTask =
				log.info(s"-- starting test case $runNo") *> io

			TestControl.executeEmbed(fullTask).unsafeRunSync()
		}
	}
	
	// object IORunner {
	// 	def apply(using Log[IO]): Resource[IO, IORunner] = Resource.eval(IO.delay(new IORunner))
	// 	def use[A](f: IORunner => IO[A])(using Log[IO]): IO[A] = apply.use(f)
	// }
}
