package corama.interpreter

import weaver.scalacheck.Checkers
import flogger.*
import cats.effect.IO

object EvaluationSuite extends CoramaSuite {
	import Evaluation.*
	import TestUtil.*

	loggedTest("executing a value on a topology") { implicit log =>
		forall(genTopology) { ctx =>
			runSync(IO.pure(success))
			// success
		}
	}
}
