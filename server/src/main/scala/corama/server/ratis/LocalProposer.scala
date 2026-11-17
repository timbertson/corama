package corama.server.ratis

import cats.effect.IO
import corama.Bytes
import org.apache.ratis.protocol.*
import org.apache.ratis.server.RaftServer

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

// Only the client API makes proposals, but we don't want
// to propose to an arbitrary server instance. We only want to
// allow proposals which are made by the leader, because topology
// evaluations are only valid on the leader.
//
// This class builds a client request and passes it directly to the server instance.
// Ratis will only accept / commit this when the server is the leader.
//
// TODO: is there a race?
// - leader executes things for message t=1. before it actually makes the proposal, there's a GC pause
// - leadership goes away
// - t=2 is applied from some other leader
// - leadership comes back, and the proposal is submitted.
//   It's based on stale state, but there's nothing to indicate that - it's the leader
// - Maybe this is fine, as appends should be idempotent?
class LocalProposer(
	server: RaftServer,
	groupId: RaftGroupId,
	clientId: ClientId, // synthetic, unique internal ID
) {
	private val callIdSeq = new AtomicLong(1)

	// TODO: this waits until the state machine apply has occurred on the leader.
	// Technically due to our speculative view, we could return as soon as the
	// proposal has been accepted by a quorum of followers.
	// Maybe hook into the state machine's applyTransaction
	def propose(data: Bytes): CompletableFuture[RaftClientReply] = {
		val request = RaftClientRequest.newBuilder()
			.setClientId(clientId)
			.setServerId(server.getId())
			.setGroupId(groupId)
			.setCallId(callIdSeq.getAndIncrement()) // Unique ID per request
			.setMessage(Message.valueOf(ByteBuffer.wrap(data)))
			.setType(RaftClientRequest.writeRequestType())
			.build()
		server.submitClientRequestAsync(request)
	}
}

object LocalProposer {
	def apply(server: RaftServer, groupId: RaftGroupId): IO[LocalProposer] = {
		IO(ClientId.randomId()).map { clientId =>
			new LocalProposer(server, groupId, clientId)
		}
	}
}
