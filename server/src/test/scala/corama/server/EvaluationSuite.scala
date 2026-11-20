package corama.server

import cats.syntax.all.*
import cats.effect.IO
import flogger.*
import weaver.scalacheck.Checkers

object EvaluationSuite extends CoramaSuite {
	import Fixtures.*
	import Evaluation.*
	import TestUtil.*

	loggedTest("executing a value on a topology") { implicit log =>
		val gen = for {
			topology <- genTopology
			runSync <- RunSync.gen
		} yield (topology, runSync)
		forall.withConfig(checkConfig
			// .withPerPropertyParallelism(1)
			// .withInitialSeed(org.scalacheck.rng.Seed.fromBase64("InbV10NadZ2Ad1lHST7nRvDRMRXiA9q7ZMM9KKS4VaL=").toOption)
		)(gen) { (ctx, runSync) =>
			val body = Cluster.inMemoryCluster(clusterConfig, ctx.topologies).use { client =>
				for {
					ack <- client.append(ctx.depot, List("Hello"))
					_ <- log.info(s"Got ackResults: $ack")
					results = ctx.streamTopologies.map(ack.result)
				} yield success
			}
			runSync(
				ctx.streamTopologies.toList.traverse_ { t =>
					IO(println(t.topology.prettyPrint))
				} *>
				body
			)
		}
	}
}
