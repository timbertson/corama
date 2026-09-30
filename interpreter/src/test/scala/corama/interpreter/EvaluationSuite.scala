package corama.interpreter

import cats.syntax.all.*
import cats.effect.IO
import flogger.*
import weaver.scalacheck.Checkers

object EvaluationSuite extends CoramaSuite {
	import Fixtures.*
	import Evaluation.*
	import TestUtil.*

	loggedTest("executing a value on a topology") { implicit log =>
		forall.withConfig(checkConfig
			.withPerPropertyParallelism(1)
			.withInitialSeed(org.scalacheck.rng.Seed.fromBase64("InbV10NadZ2Ad1lHST7nRvDRMRXiA9q7ZMM9KKS4VaL=").toOption)
		)(genTopology) { ctx =>
			val body = Interpreter.createCluster(ctx.root, clusterInstances, numPartitions = 4).use { interpreter =>
				for {
					ack <- interpreter.append(ctx.depot, List("Hello"))
					_ <- log.info(s"Got ackResults: $ack")
					results = ctx.topologies.map(ack.result)
				} yield success
			}
			runSync(
				ctx.topologies.toList.traverse_ { t =>
					IO(println(t.topology.prettyPrint))
				} *>
				body
			)
		}
	}
}
