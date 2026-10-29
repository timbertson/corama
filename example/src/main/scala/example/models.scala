package example

import scala.jdk.CollectionConverters._
import com.rpl.rama._
import com.rpl.rama.ops.RamaFunction2
import com.rpl.rama.ops.RamaFunction1
import io.circe.generic.auto._
import corama.Codec

case class Ticket(id: Long, requester: String, description: String) extends RamaSerializable
object Ticket {
	implicit val codec: Codec[Ticket] = Codec.json
}

case class FullTicket(upstream: Ticket, assignee: Option[Long], assigneeName: Option[String]) extends RamaSerializable
object FullTicket {
	implicit val codec: Codec[FullTicket] = Codec.json
}

object UpdateTicketAssigneeName extends RamaFunction2[FullTicket, String, FullTicket]{
	override def invoke(t: FullTicket, n: String): FullTicket =
		t.copy(assigneeName = Option(n))
}

case class TicketAssignment(ticketId: Long, assigneeId: Long) extends RamaSerializable

case class PropagateTicketAssignment(ticketId: Long, assignee: Long, prevAssignee: Option[Long]) extends RamaSerializable
object PropagateTicketAssignment {
	implicit val codec: Codec[PropagateTicketAssignment] = Codec.json
}

class PropagateTicketAssignmentKey extends RamaFunction1[PropagateTicketAssignment, Long] {
	override def invoke(assignment: PropagateTicketAssignment): Long = assignment.ticketId
}

case class User(id: Long, email: String, name: String) extends RamaSerializable
object User {
	implicit val codec: Codec[User] = Codec.json
}

case class UserNameChange(userId: Long, name: String) extends RamaSerializable
object UserNameChange {
	implicit val codec: Codec[UserNameChange] = Codec.json
}

case class UserTicketsRequest(userId: Long, cursor: Option[Long] = None) extends RamaSerializable
object UserTicketsRequest {
	implicit val codec: Codec[UserTicketsRequest] = Codec.json
}

case class UserTicketsResponse(tickets: List[Ticket], nextCursor: Option[Long]) extends RamaSerializable
object UserTicketsResponse {
	val empty = UserTicketsResponse(Nil, None)
	implicit val codec: Codec[UserTicketsResponse] = Codec.json
}

class TicketIdExtractor extends RamaFunction1[Ticket, Long] {
	override def invoke(ticket: Ticket): Long = ticket.id
}

class UserPartitionKey extends RamaFunction1[User, Long] {
	override def invoke(user: User): Long = user.id
}

case class UserAlreadyExists(userId: Long) extends RamaSerializable

case class NoSuchUser(id: Long) extends RamaSerializable

case class NoSuchTicket(id: Long) extends RamaSerializable

class AssignmentPartitioner extends RamaFunction1[TicketAssignment, Long] {
	override def invoke(assignment: TicketAssignment): Long = assignment.assigneeId
}
object TicketAssignment {
	implicit val codec: Codec[TicketAssignment] = Codec.json
}

class UserNameChangePartitioner extends RamaFunction1[UserNameChange, Long] {
	override def invoke(value: UserNameChange): Long = value.userId
}

class UpsertUpstreamTicket extends RamaFunction2[FullTicket, Ticket, FullTicket] {
	override def invoke(current: FullTicket, upstream: Ticket): FullTicket = {
		println(s"upserting $upstream into ${Option(current)}")
		Option(current).fold(FullTicket(upstream, None, None))(_.copy(upstream = upstream))
	}
}

class SetTicketAssignee extends RamaFunction2[FullTicket, Long, FullTicket] {
	override def invoke(current: FullTicket, assigneeId: Long): FullTicket = {
		current.copy(assignee = Some(assigneeId))
	}
}

class ApplyNameChange extends RamaFunction2[User, UserNameChange, User] {
	override def invoke(current: User, change: UserNameChange): User = {
		current.copy(name = change.name)
	}
}
