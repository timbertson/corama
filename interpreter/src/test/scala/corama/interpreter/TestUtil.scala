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

	class RunSyncFn extends Expectations.Helpers {
		private val runNoCounter = new AtomicInteger(0)

		def apply(io: Log[IO] ?=> IO[Expectations])(using log: Log[IO]): Expectations = {
			import cats.effect.unsafe.implicits.global
			val runNo = runNoCounter.incrementAndGet()

			val fullTask =
				log.info(s"-- starting test case $runNo") *> io

			try {
				TestControl.executeEmbed(fullTask).unsafeRunSync()
			} catch {
				case e => failure(e.toString())
			}
		}
	}
}
