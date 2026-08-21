package flow

import cats.syntax.all._
import cats.effect._
import java.lang.{Long => JLong}

/** A slice of `example.HelloWorldModule` rewritten in [[Flow]], as an API check.
  *
  * The chosen slice is the assignment-propagation path from Main.scala:198-230 and
  * the `userTickets` query from Main.scala:233-269. Those are the parts that fan out
  * and re-partition, so they exercise exactly what the eager builder handles badly.
  *
  * Compare `Main.scala`'s version: every var is a string literal, every type is
  * `Object`, and the fan-out at `.each(Ops.EXPLODE, "*ticketIds").out("*ticketId")`
  * is invisible in the types. Here the same shape typechecks, and `Interp` runs it.
  */
object Example extends IOApp {

	case class Ticket(id: Long, requester: String, description: String)
	case class FullTicket(ticket: Ticket, assignee: Option[Long])
	case class Assignment(ticketId: Long, assigneeId: Long)

	val topologies: Topologies = Topologies()

	val fullTickets: PState[Long, FullTicket] = topologies.pstate("fullTickets")
	val userTickets: PState[Long, Set[Long]] = topologies.pstate("userTickets")
	val users: PState[Long, String] = topologies.pstate("users")
	val ticketAssignment: Depot[Assignment] = topologies.depot("ticketAssignment")

	/** Propagate an assignment: update the ticket, add to the new owner's set,
	  * remove from the previous owner's. Ack returns whoever owned it before.
	  *
	  * The `AckReturning[Option[Long]]` in the requirement is what `Flow.ackReturn`
	  * below contributes: this fragment only goes in a topology declared to aggregate
	  * ack returns of that type.
	  */
	def propagateAssignment(assignment: Var[Assignment]) =
		for {
			ticketId <- assignment.map(_.ticketId)
			newOwner <- assignment.map(_.assigneeId)

			// Partition by ticket: read the previous assignee before overwriting it.
			// Rama's version carries `prevAssignee` on an intermediate depot record so
			// a retry can't lose it (Main.scala:196-200); the same concern applies here.
			_ <- Flow.hashPartition(ticketId)
			existing <- fullTickets.get(ticketId)
			_ <- existing.filter(_.isDefined)
			previousOwner <- existing.map(_.flatMap(_.assignee))
			_ <- fullTickets.update(ticketId).zip(newOwner) { (cur, owner) =>
				cur.map(_.copy(assignee = Some(owner)))
			}
			previousOwnerList <- previousOwner.into[List]

			// Partition by the new owner: add the ticket to their set.
			// This write is in a different transaction (in terms of pstate upates)
			_ <- Flow.hashPartition(newOwner)
			_ <- userTickets.update(newOwner).zip(ticketId) { (prev, tid) =>
				Some(prev.getOrElse(Set.empty) + tid)
			}

			// And remove from the previous owner, when there was a different one.
			hop <- previousOwner.map.zip(newOwner)((prev, now) => prev.filter(_ != now))
			_ <- hop.filter(_.isDefined)
			oldOwner <- hop.map(_.get)
			_ <- Flow.hashPartition(oldOwner)
			_ <- userTickets.update(oldOwner).zip(ticketId) { (prev, tid) =>
				prev.map(_ - tid)
			}
		} yield previousOwnerList

	/** List a user's tickets: fan out over their ticket ids, look each one up on its
	  * own partition, then collapse the results back into a single list.
	  */
	def listUserTickets(userId: Var[Long]): Flow[List[Ticket], Any] = {
		for {
			_ <- Flow.hashPartition(userId)
			ids <- userTickets.get(userId)

			// Each ticket lives on its own partition. Explode
			// fans out each ID
			ticketId <- ids.explode(_.getOrElse(Set.empty).toList.sorted)
			_ <- Flow.hashPartition(ticketId)
			full <- fullTickets.get(ticketId)
			_ <- full.filter(_.isDefined)
			ticket <- full.map(_.get.ticket)
			tickets <- ticket.into[List]
		} yield tickets
	}

	val userTicketsQuery: QueryTopology[Long, List[Ticket]] =
		topologies.query[Long, List[Ticket]]("userTickets", Agg.list[Ticket])(listUserTickets)

	val assigning: StreamTopology[Assignment, List[Option[Long]]] =
		topologies.stream(ticketAssignment, Agg.list[Option[Long]])(propagateAssignment)

	override def run(args: List[String]): IO[ExitCode] = {
		println("flow.Example")
		
		// Seed three tickets owned by user 111, one by 222.
		val tickets = List(
			Ticket(1, "Troubles McGee", "My printer is on fire!"),
			Ticket(2, "Troubles McGee", "I don't have water"),
			Ticket(3, "Passerby", "There seems to be a fire"))

		val interp = new Interpreter(topologies)
		interp.running {
			for {
				_ <- tickets.traverse_ { t =>
					interp.put(fullTickets, t.id, FullTicket(t, Some(111L)))
				}
				_ <- interp.put(userTickets, 111L, Set(1L, 2L))
				_ <- interp.put(userTickets, 222, Set(3L))

				_ = println("Getting user tickets")
				listed <- interp.invoke(userTicketsQuery, 111L)
				_ = println(s"  user 111 tickets: $listed")

				expected = List(tickets(0), tickets(1))
				_ = assert(listed == expected, s"expected $expected, got $listed")
				

				// Reassign ticket 1 from user 111 to user 222 and check both sets moved.
				acks <- interp.append(ticketAssignment, Assignment(1L, 222L))

				// And the ack *return* carries a value back: who owned the ticket before.
				previous <- IO.fromTry(acks.unique(assigning))
				_ = println(s"  reassigned ticket 1, previously owned by: $previous")
				_ = assert(previous == Some(111L), s"ack return: $previous")

				_ <- interp.get(userTickets, 222L).map { result =>
					assert(result == Some(Set(3L, 1L)), s"new owner: ${result}")
				}
				_ <- interp.get(userTickets, 111L).map { result =>
					assert(result == Some(Set(2L)), s"old owner: ${result}")
				}
				_ <- interp.get(fullTickets, 1L).map { result =>
					assert(result == Some(FullTicket(tickets(0), Some(222L))), s"ticket: ${result}")
				}

				// Reassigning to the same owner must not remove the ticket.
				_ <- interp.append(ticketAssignment, Assignment(1L, 222L))
				_ <- interp.get(userTickets, 222L).map { result =>
					assert(result == Some(Set(3L, 1L)), s"self-reassign: ${result}")
				}
			} yield ExitCode.Success
		}
	}
}
