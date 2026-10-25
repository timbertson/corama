package example

import cats.Eq
import cats.syntax.all._
import corama._
import com.rpl.rama.RamaModule
import com.rpl.rama
import scala.annotation.unused
import corama.rama.RamaBackend
import scala.jdk.CollectionConverters._
import corama.rama.RamaCodecSingleton
import corama.rama.RamaCodec
import corama.rama.CodecRegistry
import izumi.reflect.Tag

class RamaExample(backend: RamaBackend) extends RamaModule {
	val topologies: Topologies = Topologies()

	// inputs from another system (e.g. support, via kafka)
	val upstreamTicket = topologies.depot[Ticket]("upstreamTicket", rama.Depot.hashBy(classOf[TicketIdExtractor]))

	// user-initiated mutations
	val userRegistration = topologies.depot[User]("userRegistration", rama.Depot.hashBy(classOf[UserPartitionKey]))
	val ticketAssignment = topologies.depot[TicketAssignment]("ticketAssignment", rama.Depot.hashBy(classOf[AssignmentPartitioner]))
	val userNameChange = topologies.depot[UserNameChange]("userNameChange", rama.Depot.hashBy(classOf[UserNameChangePartitioner]))

	// internal
	val mergedUserChange = topologies.depot[User]("mergedUserChange", rama.Depot.hashBy(classOf[UserPartitionKey]))
	val propagateTicketAssignment = topologies.depot[PropagateTicketAssignment]("propagateTicketAssignment", rama.Depot.hashBy(classOf[PropagateTicketAssignmentKey]))

	val users = topologies.pstateKV[Long, User]("user")

	// tickets is a set of IDs, subindexed so it can be arbitrarily large
	val userTickets = topologies.pstateKVSet[Long, Long]("userTickets", Subindex.indexed)

	val fullTickets = topologies.pstateKV[Long, FullTicket]("fullTicket")

	// upstream ticket changes update the fullTicket view
	val _ = topologies.stream_(upstreamTicket) { ticket =>
		for {
			id <- ticket.map(_.id)
			_ <- fullTickets.key(id).updateWith(ticket) { (full, upstream) =>
				println(s"upserting $upstream into $full")
				Some(full.fold(FullTicket(upstream, None, None))(_.copy(upstream = upstream)))
			}
		} yield Var.unit
	}

	// creates a user (further updates have no effect)
	val registrationStream = topologies.stream(userRegistration, Result.agg[Unit]) { user =>
		for {
			userId <- user.map(_.id)
			existing <- users.key(userId).get
			result <- existing.fold[Result.Void, corama.Etl] {
				// no existing user
				for {
					_ <- mergedUserChange.localAppend(user)
					result <- Flow.pure(Result.empty[Unit])
				} yield result
			} { (existing: Var[?]) =>
				userId.map(uid => Result.failure(s"User already exists: $uid"))
			}
		} yield result
	}

	val nameChangeStream = topologies.stream(userNameChange, Result.agg[Unit]) { change =>
		for {
			userId <- change.map(_.userId)
			existing <- users.key(userId).get
			result <- existing.fold[Result.Void, corama.Etl] {
				userId.map { uid =>
					Result.failure(s"No such user: $uid")
				}
			} { existing =>
				for {
					user <- existing.mapWith(change) { (existing, change) =>
						existing.copy(name = change.name)
					}
					_ <- mergedUserChange.localAppend(user)
					// TODO it's a bit awkward to return something
					result <- Flow.pure[Result[Unit]](Result.unit)
				} yield result
			}
		} yield result
	}

	// When user changes (from either userRegistration or userNameChange),
	// update the user view in $$userTickets
	val _ = topologies.stream_(mergedUserChange) { user =>
		for {
			userId <- user.map(_.id)
			_ <- user.foreach { user => println(s"Applying merged user change: $user") }
			// store the new user
			_ <- users.key(userId).put(user)
			// _ <- userId.foreach { id => println(s"Updating user $id in userTickets") }
			// _ <- userTickets.update(userId, user) { (maybeUserTickets, user) =>
			// 	maybeUserTickets.map(_.copy(user = user))
			// }
		} yield Var.unit
	}

	val _ = topologies.stream(ticketAssignment, Result.agg[Unit]) { assignment =>
		for {
			_ <- assignment.foreach(a => println(s"Assignment: $a"))
			userId <- assignment.map(_.assigneeId)
			ticketId <- assignment.map(_.ticketId)

			// assignment is partitioned by (new) assigneeId. First validate the new assignee
			newAssignee <- users.key(userId).get
			result <- newAssignee.fold[Result.Void, StreamContext[Result.Void]] {
				userId.map(userId => Result.failure(s"No such user: $userId"))
			} { newAssignee =>
				for {
					_ <- Flow.hashPartition(ticketId)
					_ <- ticketId.foreach(t => println(s"Finding ticket $t"))
					prevTicket <- fullTickets.key(ticketId).get
					result <- prevTicket.fold {
						ticketId.map(ticketId => Result.failure[Unit](s"No such ticket: $ticketId"))
					} { ticket =>
						// At this point we know the new user and ticket are both valid.
						// Dispatch an event keyed on the ticket ID, serializing the
						// old assignee ID so it can't be lost in the case of a retried transaction
						for {
							message <- ticket.mapWith(userId) { (ticket, assignee) =>
								PropagateTicketAssignment(ticket.upstream.id, assignee = assignee, prevAssignee = ticket.assignee)
							}
							_ <- propagateTicketAssignment.localAppend(message)
							ok <- Flow.pure(Result.unit)
						} yield ok
					}
				} yield result
			}
		} yield result
	}

	val _ = topologies.stream_(propagateTicketAssignment) { assignment =>
		for {
			_ <- assignment.foreach(a => println(s"Propagating assignment $a"))
			ticketId <- assignment.map(_.ticketId)
			newUserId <- assignment.map(_.assignee)
			oldUserId <- assignment.map(_.prevAssignee)

			// Update fullTickets, it's the source of truth for who owns a ticket
			_ <- fullTickets.key(ticketId).updateWith(newUserId) { (ticket, newUserId) =>
				ticket.map(_.copy(assignee = Some(newUserId)))
			}
			
			// now update new user
			_ <- Flow.hashPartition(newUserId)

			_ <- userTickets.key(newUserId).add(ticketId)
			
			// now remove from old user, if defined (and different from new user)
			oldUserId <- oldUserId.mapFilter(identity)
			_ <- oldUserId.filterWith(newUserId)((a, b) => a =!= b)
			_ <- Flow.hashPartition(oldUserId)
			_ <- userTickets.key(oldUserId).remove(ticketId)
		} yield Var.unit
	}

	val ticketAgg = Agg(UserTicketsResponse.empty, (a, b) => {
		UserTicketsResponse(a.tickets ++ b.tickets, a.nextCursor.orElse(b.nextCursor))
	})

	val getUserTickets = topologies.query[UserTicketsRequest, UserTicketsResponse]("userTickets", ticketAgg) { query =>
		val userTicketsPageSize = 10
		for {
			userId <- query.map(_.userId)
			cursor <- query.map(_.cursor)

			_ <- Flow.hashPartition(userId)
			_ <- userId.foreach(uid => println(s"fetching ticket assignment for $uid"))
			
			ticketIds <- userTickets.key(userId).selectRange(afterItem = cursor, maxItems = userTicketsPageSize)
			cursor <- ticketIds.map { ids =>
				// potentially more results, use the last ID as the cursor
				Option.when(ids.size >= userTicketsPageSize)(ids.last)
			}
			
			// fanout each ticket ID
			// TODO: is this serial? Can I make it parallel per hashPartition?
			ticketId <- ticketIds.explode[Long]
			_ <- Flow.hashPartition(ticketId)

			// lookup ticket; filter out None (TODO: log / alert on none?)
			fullTicketOpt <- fullTickets.key(ticketId).get
			fullTicket <- fullTicketOpt.mapFilter(identity)
			ticket <- fullTicket.map(_.upstream)
			result <- ticket.mapWith(cursor) { (ticket, cursor) =>
				println(s"Found ticket $ticket for ID ${ticket.id}")
				UserTicketsResponse(List(ticket), cursor)
			}
		} yield result
	}

	override def define(setup: RamaModule.Setup, ramaTopologies: RamaModule.Topologies): Unit = {
		backend.install(setup, ramaTopologies, topologies)
	}
}

object RamaExample {
	def main(@unused argv: Array[String]): Unit = {

		val registry = new CodecRegistry()
		RamaCodec.setGlobal(registry)
		val backend = RamaBackend(registry)
		val example = new RamaExample(backend)
		val serializers: java.util.List[Class[?]] = List(classOf[RamaCodecSingleton]).asJava

		val ramaCluster = rama.test.InProcessCluster.create(serializers)
		val ctl = RamaBackend.Cluster(ramaCluster, classOf[RamaExample])
		// TODO autoclose?
		try {
			println("Launching module...")
			ramaCluster.launchModule(example, new rama.test.LaunchConfig(8, 8).numWorkers(8));
			println("Module launched...")

			ctl.append(example.upstreamTicket,
				Ticket(1, requester = "Troubles McGee", description = "My printer is on fire!"),
				Ticket(2, requester = "Troubles McGee", description = "I don't have water"),
				Ticket(3, requester = "Passerby", description = "There seems to be a fire"),
			)

			ctl.append(example.userRegistration,
				User(111, name = "Agent Smith", email="smiffy@agents.r.us"),
				User(222, name = "Sooty", email="soots@puppet.com"),
			)

			ctl.append(example.ticketAssignment,
				TicketAssignment(1, 111),
				TicketAssignment(9, 111),
				TicketAssignment(3, 999),
				TicketAssignment(2, 111),
				TicketAssignment(3, 111),
				TicketAssignment(2, 222),
				TicketAssignment(3, 111),
			)

			(4L to 30L).foreach { id =>
				ctl.append(example.upstreamTicket, Ticket(id, requester = "Troubles McGee", description = s"Spam #$id"))
				// sorry 111, you get all the goods
				ctl.append(example.ticketAssignment, TicketAssignment(id, 111))
			}

			ctl.append(example.userNameChange, UserNameChange(111, name = "Ex-Agent Smith"))
			
			println("Ticket 1: " + ctl.get(example.fullTickets, 1L))
			println("Ticket 2: " + ctl.get(example.fullTickets, 2L))
			println("Ticket 3: " + ctl.get(example.fullTickets, 3L))

			println("User 111: " + ctl.get(example.users, 111L))
			println("User 222: " + ctl.get(example.users, 222L))

			println("User tickets[222]: " + ctl.query(example.getUserTickets, UserTicketsRequest(222L)))

			def recurseListTickets(pageNum: Int, cursor: Option[Long]): Unit = {
				val result = ctl.query(example.getUserTickets, UserTicketsRequest(111L, cursor))
				println(s"User tickets[111] (page $pageNum): ${result}")
				result.nextCursor.foreach { cursor =>
					val nextPage = pageNum + 1
					println(s"Getting page $nextPage")
					recurseListTickets(nextPage, Some(cursor))
				}
			}
			recurseListTickets(1, None)
			println("DONE")
		} finally {
			ctl.close()
			println("CLOSED")
		}
		println("shutting down...")
	}
}
