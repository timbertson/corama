package corama.server.ratis

import corama.server.*
import cats.effect.IO
import corama.*
import corama.interpreter.*
import flogger.IOLogging
import flogger.Log
import fs2.Stream
import org.apache.ratis.client.RaftClient
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.exceptions.NotLeaderException

import java.nio.ByteBuffer

/** The ratis backend of [[RepLog]] for one task's group. */
final class RatisRepLog(proposer: LocalProposer)(using Log[IO]) extends RepLog with IOLogging {
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

	def isCommitted(txId: TxId): IO[Boolean] = ???
	// TODO: durable tx bookkeeping — the state machine indexes tx ids as it applies

	def leader: IO[Option[InstanceId]] = ???
	// TODO: from the client's leader knowledge

	def appliedOffsets: Stream[IO, LogIndex] = ???
	// TODO: broadcast from the StateMachine.applyTransaction hook
}
