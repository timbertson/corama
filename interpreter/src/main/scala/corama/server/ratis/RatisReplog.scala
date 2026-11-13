package corama.server.ratis

import corama.server.*
import cats.effect.IO
import corama.*
import corama.interpreter.*
import flogger.IOLogging
import flogger.Log
import fs2.Stream
import org.apache.ratis.protocol.exceptions.NotLeaderException

final class RatisRepLog(
	proposer: LocalProposer,
	stateMachine: StateMachine,
)(using Log[IO]) extends RepLog with IOLogging {
	def submit(update: ReplicationUpdate): IO[Proposal] = {
		IO.pure {
			Proposal(IO.fromCompletableFuture {
				IO.delay {
					proposer.propose(ServerCodecs.replicationUpdate.encode(update).array)
				}
			}.map(reply => LogIndex(reply.getLogIndex))
			// TODO: leader hint
			.adaptError { case e: NotLeaderException => NotLeader(None) }
			)
		}
	}

	def appliedOffsets: Stream[IO, LogIndex] = stateMachine.appliedOffsets.changes.discrete
}
