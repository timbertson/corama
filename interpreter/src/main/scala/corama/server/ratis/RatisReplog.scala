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
final class RatisRepLog(raft: RaftClient)(using Log[IO]) extends RepLog with IOLogging {
	// TODO: use OpenBatch from interpreter to keep 1 open batch at all times.
	// Submit should only block once there's 1 in-flight batch and the next batch is full
	// TODO: can we start sending a ratis batch before the previous one completes? Should we?
	def submit(update: ReplicationUpdate): IO[Proposal] = {
		IO.pure {
			Proposal(IO.fromCompletableFuture {
				IO.delay {
					raft.async().send(Message.valueOf(
						ByteBuffer.wrap(ServerCodecs.replicationUpdate.encode(update).array)
					))
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
