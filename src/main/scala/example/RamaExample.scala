package example

import cats.Eq
import cats.syntax.all._
import cats.data.Op
import flow._
import com.rpl.rama.RamaModule
import com.rpl.rama
import dsl.JLong
import scala.annotation.unused
import flow.Example.assigning
import flow.rama.RamaBackend

class RamaExample extends RamaModule {
	// import RamaModule._
	import RamaExample._

	val topologies: Topologies = Topologies()

	// val fullTickets: PState[Long, FullTicket] = topologies.pstate("fullTickets")
	// val users: PState[Long, String] = topologies.pstate("users")

	// val ticketAssignment: Depot[TicketAssignment] = topologies.depot("ticketAssignment")

	// inputs from another system (e.g. support, via kafka)
	val upstreamTicket = topologies.depot[Ticket]("upstreamTicket", rama.Depot.hashBy(classOf[TicketIdExtractor]))

	// user-initiated mutations
	val userRegistration = topologies.depot[User]("userRegistration", rama.Depot.hashBy(classOf[UserPartitionKey]))
	val ticketAssignment = topologies.depot[TicketAssignment]("ticketAssignment", rama.Depot.hashBy(classOf[AssignmentPartitioner]))
	val userNameChange = topologies.depot[UserNameChange]("userNameChange", rama.Depot.hashBy(classOf[UserNameChangePartitioner]))

	// internal
	val mergedUserChange = topologies.depot[User]("mergedUserChange", rama.Depot.hashBy(classOf[UserPartitionKey]))
	val propagateTicketAssignment = topologies.depot[PropagateTicketAssignment]("propagateTicketAssignment", rama.Depot.hashBy(classOf[PropagateTicketAssignmentKey]))

	val users = topologies.pstateKV[JLong, User]("user")

	// tickets is a set of IDs, subindexed so it can be arbitrarily large
	val userTickets = topologies.pstateKVSet[JLong, JLong]("userTickets", Subindex.indexed)

	// val userTickets = topologies.pstate("userTickets",
	// 	Schema.map(Schema[JLong], Schema.fixed(
	// 		"user" -> Schema[User],
	// 		"ticketIds" -> Schema.set[JLong](Subindex.indexed)
	// 	)
	// ))

	val fullTickets = topologies.pstateKV[JLong, FullTicket]("fullTicket")

	// upstream ticket changes update the fullTicket view
	topologies.stream_(upstreamTicket) { ticket =>
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
			result <- existing.fold[Result.Void, flow.Etl] {
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
			result <- existing.fold[Result.Void, flow.Etl] {
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
	topologies.stream_(mergedUserChange) { user =>
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

	topologies.stream(ticketAssignment, Result.agg[Unit]) { assignment =>
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

	topologies.stream_(propagateTicketAssignment) { assignment =>
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

	val userTicketsPageSize = 10
	val ticketAgg = Agg(UserTicketsResponse.empty, (a, b) => {
		UserTicketsResponse(a.tickets ++ b.tickets, a.nextCursor.orElse(b.nextCursor))
	})

	topologies.query[UserTicketsRequest, UserTicketsResponse]("userTickets", ticketAgg) { query =>
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
			ticketId <- ticketIds.explode[JLong]
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
		RamaBackend(setup, ramaTopologies).install(topologies)
	}
}

object RamaExample {
	implicit val eqLong: Eq[JLong] = Eq.fromUniversalEquals

	def main(@unused argv: Array[String]): Unit = {

		def appendTo(depot: rama.Depot, obj: Object*) = {
			obj.foreach { obj =>
				val appendResult = depot.append(obj)
				println(s"$appendResult <- from appending $obj")
			}
		}

		// TODO autoclose?
		val cluster = rama.test.InProcessCluster.create()
		try {
			println("Launching module...")
			cluster.launchModule(new RamaExample(), new rama.test.LaunchConfig(8, 8).numWorkers(8));
			println("Module launched...")

			val moduleName = classOf[RamaExample].getName()

			val upstreamTicketDepot = cluster.clusterDepot(moduleName, "*upstreamTicket")
			appendTo(upstreamTicketDepot,
				Ticket(1, requester = "Troubles McGee", description = "My printer is on fire!"),
				Ticket(2, requester = "Troubles McGee", description = "I don't have water"),
				Ticket(3, requester = "Passerby", description = "There seems to be a fire"),
			)

			appendTo(cluster.clusterDepot(moduleName, "*userRegistration"),
				User(111, name = "Agent Smith", email="smiffy@agents.r.us"),
				User(222, name = "Sooty", email="soots@puppet.com"),
			)

			val assignmentsDepot = cluster.clusterDepot(moduleName, "*ticketAssignment")
			appendTo(assignmentsDepot,
				TicketAssignment(1, 111),
				TicketAssignment(9, 111),
				TicketAssignment(3, 999),
				TicketAssignment(2, 111),
				TicketAssignment(3, 111),
				TicketAssignment(2, 222),
				TicketAssignment(3, 111),
			)

			(4L to 30L).foreach { id =>
				upstreamTicketDepot.append(Ticket(id, requester = "Troubles McGee", description = s"Spam #$id"))
				// sorry 111, you get all the goods
				appendTo(assignmentsDepot, TicketAssignment(id, 111))
				// assignmentsDepot.append(TicketAssignment(id, 111))
			}

			appendTo(cluster.clusterDepot(moduleName, "*userNameChange"),
				UserNameChange(111, name = "Ex-Agent Smith"),
			)
			
			println("Ticket 1: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](rama.Path.key(long2Long(1L))))
			println("Ticket 2: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](rama.Path.key(long2Long(2L))))
			println("Ticket 3: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](rama.Path.key(long2Long(3L))))

			println("User 111: " + cluster.clusterPState(moduleName, "$$user").selectOne[User](rama.Path.key(long2Long(111L))))
			println("User 222: " + cluster.clusterPState(moduleName, "$$user").selectOne[User](rama.Path.key(long2Long(222L))))

			val userTicketsQuery = cluster.clusterQuery[UserTicketsResponse](moduleName, "userTickets")
			println("User tickets[222]: " + userTicketsQuery.invoke(UserTicketsRequest(222L)))

			def recurseListTickets(pageNum: Int, cursor: Option[JLong]): Unit = {
				val result = userTicketsQuery.invoke(UserTicketsRequest(111L, cursor)).asInstanceOf[UserTicketsResponse]
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
			cluster.close()
		}
	}
}
