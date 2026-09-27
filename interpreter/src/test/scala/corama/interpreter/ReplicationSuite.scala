package corama.interpreter

import cats.syntax.all._
import scala.concurrent.duration._
import cats.effect.IO
import scala.concurrent.duration.FiniteDuration
import weaver.scalacheck.Checkers
import weaver.Expect
import weaver.Expectations
import cats.Show
import org.scalacheck.Gen
import flogger.*

object ReplicationSutie extends CoramaSuite {
	import Fixtures.*
	import Replication.*
	import TestUtil.*
	
	given showMillis: Show[FiniteDuration] = Show.show(d => s"${d.toMillis.toString()}ms")

	loggedTest("single event in healthy state") { implicit log =>
		val sim = for {
			warmup <- Warmup.gen()
			nextEvent <- Op.gen
		} yield (warmup, nextEvent)

		forall.withConfig(checkConfig
			// .withPerPropertyParallelism(1) // TODO remove
			// .withInitialSeed(org.scalacheck.rng.Seed.fromBase64("EfbDxjup3AnGUd5BCSLXuDpHQmEeFgHAt0ekC4uZ9zC=").toOption)
		)(sim) { case (warmup, nextEvent) =>
			runSync(ReplicationCtx.run(warmup) { ctx =>
				for {
					transactions <- ctx.run(List(nextEvent))
					allEvents <- ctx.events
					operationEvents = allEvents.ops.getOrElse(nextEvent, Nil)
					firstTransaction = transactions.headOption
					transactionEvents = allEvents.transactions.filter { case (id, _) => firstTransaction.contains(id) }.toMap
				} yield expectAll(
					expect(
						clue(transactions).size == 1,
						"single event should be placed in a single transaction"),

					expectSuccessfulBatchTransactions(allEvents),

					operationEvents match {
						case List(sent: Event.OpSent, accepted: Event.OpAccepted, completed: Event.OpCompleted) => {
							expect(
								clue(clue(accepted.timestamp) - clue(sent.timestamp)) <= clue(leaderConfig.maxBatchDelay),
								s"operation send within maxBatchDelay: $operationEvents") and
							expect(
								clue(clue(completed.timestamp) - clue(accepted.timestamp)) <= clue(leaderConfig.maxLag + leaderConfig.maxBatchDelay),
								s"operation ack within maxLag + maxBatchDelay: $operationEvents")
						}
						case other => failure(s"couldn't match op events: ${operationEvents}")
					},
				)
			})
		}
	}

	loggedTest("multiple events in healthy state") { implicit log =>
		val sim = for {
			warmup <- Warmup.gen()
			numEvents <- Gen.choose(0, 100)
			nextEvents <- Gen.listOfN(numEvents, Gen.zip(Op.gen, Op.emitDelayGen))
		} yield (warmup, nextEvents)
		forall(sim) { (warmup, nextEvents) =>
			runSync(ReplicationCtx.run(warmup) { ctx =>
				for {
					// enqueue with delay, then wait for all batches to be complete
					enqueued <- nextEvents.flatTraverse { case (ev, delay) =>
						ctx.enqueueAll(List(ev), delay)
					}
					_ <- ctx.awaitAll(enqueued)
					allEvents <- ctx.events
				} yield expectSuccessfulBatchTransactions(allEvents)
			})
		}
	}

	loggedTest("message sent when one follower is unhealthy") { implicit log =>
		val sim = for {
			warmup <- Warmup.gen(statesGen = FollowerStates.allHealthy)
			degradedState <- FollowerStates.degraded
			nextEvent <- Op.gen
		} yield (warmup, nextEvent, degradedState)
		forall(sim) { (warmup, nextEvent, degradedState) =>
			runSync(ReplicationCtx.run(warmup) { ctx =>
				for {
					_ <- ctx.setStates(degradedState)
					_ <- ctx.run(List(nextEvent))
					isfs <- ctx.leader.taskStatus.get.map(_.isf)
				} yield expect(clue(isfs).size == 1)
			})
		}
	}
	
	test("message sent to an unhealthy cluster".ignore) {
		// must not be acknowledged
		IO.pure(success)
	}

	test("message sent when ISR is already too low".ignore) {
		// should be immediately rejected?
		IO.pure(success)
	}

	test("unhealthy cluster becomes healthy".ignore) {
		// new messages should eventually be acknowledged (after replica catches up)
		IO.pure(success)
	}

	private def expectSuccessfulBatchTransactions(allEvents: Events): Expectations = {
		val acks = allEvents.allTransactions.filter(_.isInstanceOf[Event.ReplicationAcknowledged])
		val relevantTx = allEvents.transactionIdsForAcceptedOps
		
		val followerExpectations: Seq[Expectations] = allEvents.transactions.toList.sortBy(_._1.value).flatMap { case (tx, followerMap) =>
			followerMap.toList.map { case (follower, events) =>
				events match {
					case List(ack: Event.ReplicationAcknowledged) =>
						expect(
							clue(relevantTx).size > 0) and expect(!clue(relevantTx).contains(clue(ack.id)),
							"trailing acks should be for warmup ops, not test ops")

					case List(send: Event.ReplicationSent) => success // sent but replica didn't acknowledge

					case List(send: Event.ReplicationSent, ack: Event.ReplicationAcknowledged) =>
						expect(
							clue(ack.timestamp - send.timestamp) <= clue(leaderConfig.maxLag),
							s"batch ack within ${leaderConfig.maxLag}")
					case other => failure(s"couldn't match events: ${events}")
				}
			}
		}

		expect(clue(acks).size >= 1, "At least one replica acknowledged the batch") and
		expectSeq(followerExpectations)
	}

}
