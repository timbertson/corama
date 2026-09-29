package corama.interpreter

import cats.effect.IO
import flogger.*
import weaver.scalacheck.Checkers

object EvaluationSuite extends CoramaSuite {
	import Fixtures.*
	import Evaluation.*
	import TestUtil.*

	loggedTest("executing a value on a topology") { implicit log =>
		forall.withConfig(checkConfig
			// .withPerPropertyParallelism(1).withMaximumGeneratorSize(1).withInitialSeed(Some(org.scalacheck.rng.Seed(1L)))
		)(genTopology) { ctx =>
			val body = Interpreter.createCluster(ctx.topologies, clusterInstances, numPartitions = 4).use { interpreter =>
				for {
					ack <- interpreter.append(ctx.depot, List("Hello"))
					_ <- log.info(s"Got ackResults: $ack")
					result = ack.result(ctx.topology)
				} yield success
			}
			runSync(
				log.warn(ctx.topology.topology.prettyPrint) *>
				body
			)
		}
	}
}
