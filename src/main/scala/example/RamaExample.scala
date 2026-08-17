package example

import cats.data.Op
import flow._
import com.rpl.rama.RamaModule
import com.rpl.rama
import dsl.JLong
import scala.annotation.unused
import flow.Example.ticketAssignment

class RamaExample extends RamaModule {
	// import RamaModule._
	import RamaExample._

	val topologies: Topologies = Topologies()

	// val fullTickets: PState[Long, FullTicket] = topologies.pstate("fullTickets")
	// val userTickets: PState[Long, Set[Long]] = topologies.pstate("userTickets")
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
	topologies.depot[PropagateTicketAssignment]("propagateTicketAssignment", rama.Depot.hashBy(classOf[PropagateTicketAssignmentKey]))

	val users = topologies.pstate[JLong, User]("user", rama.PState.mapSchema(classOf[JLong], classOf[User]))

	// tickets is a set of IDs, subindexed so it can be arbitrarily large
	val userTickets = topologies.pstate[JLong, List[JLong]]("userTickets", rama.PState.mapSchema(classOf[JLong],
		rama.PState.fixedKeysSchema(
			"user", classOf[User],
			"ticketIds", rama.PState.setSchema(classOf[JLong]).subindexed,
		))
	)

	val fullTicket = topologies.pstate[JLong, FullTicket]("fullTicket", rama.PState.mapSchema(classOf[JLong], classOf[FullTicket]))

	// upstream ticket changes update the fullTicket view
	topologies.stream[Ticket](upstreamTicket) { t =>
		val ticket = t.input
		for {
			id <- ticket.map(_.id)
			_ <- fullTicket.update(id, ticket) { (full, upstream) =>
				println(s"upserting $upstream into $full")
				Some(full.fold(FullTicket(upstream, None, None))(_.copy(upstream = upstream)))
			}
		} yield Var.unit
	}

	// creates a user (further updates have no effect)
	val registrationStream = topologies.stream(userRegistration, Agg.error[UserAlreadyExists]) { t =>
		val user = t.input
		for {
			userId <- user.map(_.id)
			existing <- users.get(userId)
			result <- existing.fold[Either[UserAlreadyExists, Unit], flow.Etl] {
				// no existing user
				for {
					_ <- mergedUserChange.localAppend(user)
					result <- Flow.pure(Right(()) : Either[UserAlreadyExists, Unit])
				} yield result
			} { (existing: Var[?]) =>
				userId.map(uid => Left(new UserAlreadyExists(uid)))
			}
			_ <- Flow.ackReturn(result)
		} yield Var.unit
	}

	val nameChangeStream = topologies.stream(userNameChange, Agg.error[NoSuchUser]) { t =>
		val change = t.input
		for {
			userId <- change.map(_.userId)
			existing <- users.get(userId)
			result <- existing.fold {
				userId.map { uid =>
					Left(NoSuchUser(uid))
				}
			} { existing =>
				for {
					user <- existing.mapWith(change) { (existing, change) =>
						existing.copy(name = change.name)
					}
					_ <- mergedUserChange.localAppend(user)
					// TODO it's a bit awkward to return something
					result <- Flow.pure[Either[NoSuchUser, Unit]](Right(()))
				} yield result
			}
			_ <- Flow.ackReturn(result)
		} yield Var.unit
	}

	// When user changes (from either userRegistration or userNameChange),
	// update the user view in $$userTickets
	topologies.stream(mergedUserChange) { t =>
		val user = t.input
		for {
			userId <- user.map(_.id)
			_ <- user.each { user => println(s"Applying merged user change: $user") }
			// store the new user
			_ <- users.put(userId, user)
			// TODO?
			// _ <- userId.each { id => println(s"Updating user $id in userTickets") }
			// _ <- userTickets.update(userId, user) { (maybeUserTickets, user) =>
			// 	maybeUserTickets.map(_.copy(user = user))
			// }
		} yield Var.unit
	}

	// s.source("*ticketAssignment").out("*assignment")
	// 	.each(Ops.CURRENT_TASK_ID).out("*taskId")
	// 	.each(Ops.PRINTLN, "Assignment:", "*assignment", "on task", "*taskId")
	// 	.each((value: TicketAssignment) => value.assigneeId, "*assignment").out("*userId")
	// 	.each((value: TicketAssignment) => value.ticketId, "*assignment").out("*ticketId")

	// 	// assignment is partitioned by (new) assigneeId. First validate the new assignee
	// 	.localSelect("$$userTickets", Path.key("*userId", "user")).out("*newAssignee")
	// 	.ifTrue(new Expr(Ops.EQUAL, null, "*newAssignee"),
	// 		Block.each(NoSuchUser.apply, "*userId").out("*error")
	// 			.ackReturn("*error")
	// 	, // else
	// 		Block.hashPartition("*ticketId")
	// 		// then grab the current view of the ticket
	// 		.each(Ops.CURRENT_TASK_ID).out("*taskId")
	// 		.each(Ops.PRINTLN, "Finding ticket", "*ticketId", "on task", "*taskId")
	// 		.localSelect("$$fullTicket", Path.key("*ticketId")).out("*prevTicket")
	// 		.ifTrue(new Expr(Ops.EQUAL, null, "*prevTicket"),
	// 			// if ticket == null
	// 				Block.each(NoSuchTicket.apply, "*ticketId").out("*error")
	// 					.ackReturn("*error")
	// 			, // else

	// 				// At this point we know the new user and ticket are both valid.
	// 				// Dispatch an event keyed on the ticket ID, serializing the
	// 				// old assignee ID so it can't be lost in the case of a retried transaction
	// 				Block.each((t: FullTicket, newAssignee: JLong) =>
	// 					PropagateTicketAssignment(t.upstream.id, assignee = newAssignee, prevAssignee = t.assignee)
	// 				, "*prevTicket", "*userId").out("*message")
	// 				.depotPartitionAppend("*propagateTicketAssignment", "*message")
	// 		)
	// 	)

	// s.source("*propagateTicketAssignment").out("*assignment")
	// 	.each(Ops.PRINTLN, "Propagating assignment", "*assignment")
	// 	.each((x: PropagateTicketAssignment) => x.ticketId, "*assignment").out("*ticketId")
	// 	.each((x: PropagateTicketAssignment) => x.assignee, "*assignment").out("*newUserId")
	// 	.each((x: PropagateTicketAssignment) => x.prevAssignee.orNull, "*assignment").out("*oldUserId")

	// 	// Update fullTicket, it's the source of truth for who owns a ticket
	// 	.localTransform("$$fullTicket", Path.key("*ticketId").term(new SetTicketAssignee(), "*newUserId"))
		
	// 	// now update new user
	// 	.hashPartition("*newUserId")
	// 	.localTransform("$$userTickets",
	// 		// add to set
	// 		Path.key("*newUserId", "ticketIds").voidSetElem().termVal("*ticketId")
	// 	)

	// 	// and remove from old user, if defined (and different from new user)
	// 	.keepTrue(new Expr(Ops.AND,
	// 		new Expr(Ops.IS_NOT_NULL, "*oldUserId"),
	// 		new Expr(Ops.NOT_EQUAL, "*newUserId", "*oldUserId")
	// 	))
	// 	.hashPartition("*oldUserId")
	// 	.localTransform("$$userTickets",
	// 		// remove from set
	// 		Path.key("*oldUserId", "ticketIds").setElem("*ticketId").termVoid()
	// 	)

	// val userTicketsPageSize = 10
	// topologies.query[UserTicketsRequest, List[Ticket]]("userTickets", Agg.list) { t =>
	// 	val req = t.input
	// 	for {
	// 		userId <- req.map(_.userId)
	// 		cursor <- req.map(_.cursor.getOrElse[JLong](Long.MinValue))

	// 		_ <- Flow.hashPartition(userId)
	// 		_ <- userId.foreach(uid => println(s"fetching ticket assignment for $uid"))
			
	// 		// TODO: paginate via cursor
	// 		ticketIds <- userTickets.getOrElse(userId, Nil)

	// 		result <- req.map(_ => Nil)
	// 	} yield result
	// }

	// {
	// 	()
	// 	.each((r: UserTicketsRequest) => r.userId, "*request").out("*userId")
	// 	.each((r: UserTicketsRequest) => r.cursor.getOrElse[JLong](Long.MinValue), "*request").out("*minId")

	// 	.hashPartition("*userId")
	// 	.each((uid: JLong) => println(s"fetching ticket assignment for $uid"), "*userId")

	// 	.localSelect("$$userTickets",
	// 		Path.key("*userId", "ticketIds").sortedSetRangeFrom("*minId", userTicketsPageSize + 1)
	// 	).out("*ticketIds")

	// 	// fanout each ID
	// 	// TODO is this serial? Can I make it parallel per hashPartition?
	// 	.each(Ops.EXPLODE, "*ticketIds").out("*ticketId")
	// 	.hashPartition("*ticketId")
	// 	.each(Ops.CURRENT_TASK_ID).out("*taskId")

	// 	// lookup ticket; filter out None
	// 	.localSelect("$$fullTicket", Path.key("*ticketId")).out("*fullTicket")
	// 	.keepTrue(new Expr(Ops.IS_NOT_NULL, "*fullTicket"))
	// 	.each((t: FullTicket) => t.upstream, "*fullTicket").out("*ticket")
	// 	// .each(Ops.PRINTLN, "Found ticket", "*fullTicket", "for ID", "*ticketId")

	// 	// send all results back to the original partition to construct the return value
	// 	.originPartition()
	// 	.agg(Agg.list("*ticket")).out("*tickets")
	// 	.each((jtickets: java.util.List[Ticket]) => {
	// 		val allTickets = jtickets.asScala
	// 		if (allTickets.size > userTicketsPageSize) {
	// 			val cursor = Some(allTickets.last.id)
	// 			// more results, trim the last one and return its ID as the cursor
	// 			UserTicketsResponse(allTickets.dropRight(1).toList, cursor)
	// 		} else {
	// 			// no more results
	// 			UserTicketsResponse(allTickets.toList, None)
	// 		}
	// 	}, "*tickets").out("*result")
	// }

	override def define(setup: RamaModule.Setup, ramaTopologies: RamaModule.Topologies): Unit = {
		RamaBackend(setup, ramaTopologies).install(topologies)
	}

}

object RamaExample {
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

			// (4L to 30L).foreach { id =>
			// 	upstreamTicketDepot.append(Ticket(id, requester = "Troubles McGee", description = s"Spam #$id"))
			// 	// sorry 111, you get all the goods
			// 	appendTo(assignmentsDepot, TicketAssignment(id, 111))
			// 	// assignmentsDepot.append(TicketAssignment(id, 111))
			// }

			// appendTo(cluster.clusterDepot(moduleName, "*userNameChange"),
			// 	UserNameChange(111, name = "Ex-Agent Smith"),
			// )
			
			println("Ticket 1: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](rama.Path.key(long2Long(1L))))
			println("Ticket 2: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](rama.Path.key(long2Long(2L))))
			println("Ticket 3: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](rama.Path.key(long2Long(3L))))

			println("User 111: " + cluster.clusterPState(moduleName, "$$user").selectOne[User](rama.Path.key(long2Long(111L))))
			println("User 222: " + cluster.clusterPState(moduleName, "$$user").selectOne[User](rama.Path.key(long2Long(222L))))

			// val userTicketsQuery = cluster.clusterQuery[UserTicketsResponse](moduleName, "userTickets")
			// println("User tickets[222]: " + userTicketsQuery.invoke(UserTicketsRequest(222L)))

			// def recurseListTickets(pageNum: Int, cursor: Option[JLong]): Unit = {
			// 	val result = userTicketsQuery.invoke(UserTicketsRequest(111L, cursor)).asInstanceOf[UserTicketsResponse]
			// 	println(s"User tickets[111] (page $pageNum): ${result}")
			// 	result.nextCursor.foreach { cursor =>
			// 		val nextPage = pageNum + 1
			// 		println(s"Getting page $nextPage")
			// 		recurseListTickets(nextPage, Some(cursor))
			// 	}
			// }
			// recurseListTickets(1, None)
			println("DONE")
		} finally {
			cluster.close()
		}
	}
}
