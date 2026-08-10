package example

import scala.jdk.CollectionConverters._
import com.rpl.rama._
import com.rpl.rama.module._
import com.rpl.rama.ops.Ops
import com.rpl.rama.test._
import cats.data.Op
import com.rpl.rama.ops.RamaFunction2
import com.rpl.rama.ops.RamaFunction1
import com.rpl.rama.impl.NativeDepotPartitioning
import com.rpl.rama.impl.NativeRamaFunction1
import scala.annotation.unused
import com.rpl.rama.impl.NativeRamaFunction2
import java.util.Arrays
import java.lang.{Long => JLong}
import com.strobel.collections.ListBuffer
import java.time.Instant


case class Ticket(id: JLong, requester: String, description: String) extends RamaSerializable

case class FullTicket(upstream: Ticket, assignee: Option[JLong], assigneeName: Option[String]) extends RamaSerializable
object UpdateTicketAssigneeName extends RamaFunction2[FullTicket, String, FullTicket]{
	override def invoke(t: FullTicket, n: String): FullTicket =
		t.copy(assigneeName = Option(n))
}

case class TicketAssignment(ticketId: JLong, assigneeId: JLong) extends RamaSerializable

case class PropagateTicketAssignment(ticketId: JLong, assignee: JLong, prevAssignee: Option[JLong]) extends RamaSerializable
class PropagateTicketAssignmentKey extends RamaFunction1[PropagateTicketAssignment, JLong] {
	override def invoke(assignment: PropagateTicketAssignment): JLong = assignment.ticketId
}

case class User(id: JLong, email: String, name: String) extends RamaSerializable

case class UserNameChange(userId: JLong, name: String) extends RamaSerializable

case class UserTicketsRequest(userId: JLong, cursor: Option[JLong] = None) extends RamaSerializable
case class UserTicketsResponse(tickets: List[Ticket], nextCursor: Option[JLong]) extends RamaSerializable

class TicketIdExtractor extends RamaFunction1[Ticket, JLong] {
	override def invoke(ticket: Ticket): JLong = ticket.id
}

class UserPartitionKey extends RamaFunction1[User, JLong] {
	override def invoke(user: User): JLong = user.id
}

case class UserAlreadyExists(userId: JLong) extends RamaSerializable

case class NoSuchUser(id: JLong) extends RamaSerializable

case class NoSuchTicket(id: JLong) extends RamaSerializable

class AssignmentPartitioner extends RamaFunction1[TicketAssignment, JLong] {
	override def invoke(assignment: TicketAssignment): JLong = assignment.assigneeId
}

class UserNameChangePartitioner extends RamaFunction1[UserNameChange, JLong] {
	override def invoke(value: UserNameChange): JLong = value.userId
}

class UpsertUpstreamTicket extends RamaFunction2[FullTicket, Ticket, FullTicket] {
	override def invoke(current: FullTicket, upstream: Ticket): FullTicket = {
		println(s"upserting $upstream into ${Option(current)}")
		Option(current).fold(FullTicket(upstream, None, None))(_.copy(upstream = upstream))
	}
}

class SetTicketAssignee extends RamaFunction2[FullTicket, JLong, FullTicket] {
	override def invoke(current: FullTicket, assigneeId: JLong): FullTicket = {
		current.copy(assignee = Some(assigneeId))
	}
}

class ApplyNameChange extends RamaFunction2[User, UserNameChange, User] {
	override def invoke(current: User, change: UserNameChange): User = {
		current.copy(name = change.name)
	}
}

