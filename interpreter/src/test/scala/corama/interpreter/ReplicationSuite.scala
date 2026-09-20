package corama.interpreter

import scala.concurrent.duration._
import weaver.SimpleIOSuite
import cats.effect.IO
import scala.concurrent.duration.FiniteDuration
import weaver.scalacheck.Checkers
import weaver.Expect
import weaver.Expectations
import cats.Show
import flogger._

object ReplicationSuite extends SimpleIOSuite with Checkers with IOLogging {
	import Fixtures._
	given showMillis: Show[FiniteDuration] = Show.show(d => s"${d.toMillis.toString()}ms")
	implicit val log: Log[IO] = Log.empty
	implicit def show[A]: Show[A] = Show.fromToString

	/*
	Properties:

	If a follower dies and then returns, further writes should be propagated.
	(after some catch-up duration)

	Modeling network partitions:
	 - stream emits zero responses
	 - stream errors (transport error)
	
	TODO: How does a replica return to healthy? Perhaps we keep invoking the stream function until it works...
	*/

	val TODO = IO.pure(success)
	
	test("single event in healthy state") {
		val sim = for {
			warmup <- Warmup.gen
			nextEvent <- Op.gen
		} yield (warmup, nextEvent)
		forall.withConfig(
			checkConfig.withPerPropertyParallelism(1) // TODO remove
				// .withInitialSeed(org.scalacheck.rng.Seed.fromBase64("mtzNcXC0p3Z9CgR4sT4RA7MHcszPPJN-0Jz4cKtGiXP=").toOption)
		)(sim) { case (warmup, nextEvent) =>
			ReplicationCtx.run(warmup) { ctx =>
				for {
					transactions <- ctx.run(List(nextEvent))
					allEvents <- ctx.events
					operationEvents = allEvents.ops.getOrElse(nextEvent, Nil)
					firstTransaction = transactions.headOption
					transactionEvents = allEvents.transactions.filter { case (id, _) => firstTransaction.contains(id) }.toMap
				} yield expectAll(
						expect(clue(transactions).size == 1, "single event should be placed in a single transaction"),
						
						operationEvents match {
							case List(sent: Event.OpSent, accepted: Event.OpAccepted, completed: Event.OpCompleted) => {
								expect(clue(clue(accepted.timestamp) - clue(sent.timestamp)) <= clue(leaderConfig.maxBatchDelay), s"send within maxBatchDelay: $operationEvents") and
								expect(clue(clue(completed.timestamp) - clue(accepted.timestamp)) <= clue(leaderConfig.maxLag + leaderConfig.maxBatchDelay), s"ack within maxLag: $operationEvents")
							}
							case other => failure(s"couldn't match op events: ${operationEvents}")
						},

						transactionEvents.toList.sortBy(_._1.value) match {
							case List((txId, followerMap)) => {
								val followerExpectations = followerMap.toList.map { case (follower, events) =>
									events match {
										case List(send: Event.ReplicationSent) => success // sent but replica didn't acknowledge
										case List(send: Event.ReplicationSent, ack: Event.ReplicationAcknowledged) => expect(clue(ack.timestamp - send.timestamp) <= clue(leaderConfig.maxLag))
										case other => failure(s"couldn't match events: ${events}")
									}
								}
								val acks = allEvents.allTransactions.filter(_.isInstanceOf[Event.ReplicationAcknowledged])
								expectSeq(followerExpectations) and
								expect(clue(acks).size >= 1, "At least one replica acknowledged the batch")
							}
							case other => failure(s"unexpected transactionEvents: ${other}")
						}
					)
			}
		}
	}
	
	// test that the first item in a batch (with or without further items) must be accepted in certain time, as long as previous batches aren't pending

	test("all appends should either be acknowledged or rejected within max.lag + max.replication.lag") {
		TODO
	}

	test("No messages should be rejected if all replicas are functioning") {
		TODO
	}

	test("if a message is rejected, there should be >1 out-of-sync replica") {
		TODO
	}

	test("if a message is rejected, there should be >1 out-of-sync replica") {
		TODO
	}

	test("when there are insufficient ISRs, messages should be rejected immediately and not sent to any replicas") {
		TODO
	}
}
