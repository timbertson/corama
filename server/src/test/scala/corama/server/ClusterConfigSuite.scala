package corama.server

import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers
import org.scalacheck.Gen
import cats.data.NonEmptyList

object ClusterConfigSuite extends SimpleIOSuite with Checkers {
	import Fixtures.*

	test("for >3 instances, every group has 3 instances") {
		forall.withConfig(checkConfig
			.withInitialSeed(org.scalacheck.rng.Seed.fromBase64("zvz1yNgpxz77IvC3Wdt2UMNbIACypqCszOsYX0nvf7C=").toOption)
		)(genClusterConfig(numInstances = Gen.choose(3, maxClusterSize), tasksFromInstances = n => Gen.choose(n, 2 * maxClusterSize))) { config =>
			forEach(config.tasks) { task =>
				val instanceIds = config.groupInstances(task).get.map(_.id)
				expect(clue(instanceIds).toList.toSet.size == 3)
			}
		}
	}

	test("task n is assigned to n, n+1, n+2") {
		forall.withConfig(checkConfig
		)(for {
			// only bother checking the simple case where tasks < instances by two, so there's no wraparound
			numTasks <- Gen.choose(1, maxClusterSize)
			config <- genClusterConfig(numInstances = Gen.const(numTasks + 2), tasksFromInstances = n => Gen.const(n - 2))
		} yield config) { config =>
			forEach(config.tasks) { task =>
				val instanceIds = config.groupInstances(task).get.map(_.id)
				// Task(0) assigned to i1, i2 + i3
				val expected = NonEmptyList.of(task.id + 1, task.id + 2, task.id + 3).map(instanceIdForOrdinal)
				expect(clue(instanceIds) === clue(expected))
			}
		}
	}
	
	test("every group has at least one instance") {
		forall.withConfig(checkConfig
		)(genClusterConfig(numInstances = Gen.choose(1, maxClusterSize), tasksFromInstances = n => Gen.choose(n - 2, maxClusterSize))) { config =>
			forEach(config.tasks) { task =>
				val instanceIds = config.groupInstances(task).get.map(_.id)
				expect(clue(instanceIds).toList.toSet.size >= 1)
			}
		}
	}
}
