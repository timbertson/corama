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
import flow.RamaBackend

class RamaExample extends RamaModule {
	import RamaModule._
	import RamaExample._

	override def define(setup: Setup, topologies: Topologies): Unit = {
		defineRama(setup, topologies)
	}

	def defineFlow(setup: Setup, topologies: Topologies): Unit = {
		RamaBackend(setup, topologies).install(flow.Example.topologies)
	}

	def defineRama(setup: Setup, topologies: Topologies): Unit = {
		// inputs from another system (e.g. support, via kafka)
		setup.declareDepot("*upstreamTicket", Depot.hashBy(classOf[TicketIdExtractor]))

		// user-initiated mutations
		setup.declareDepot("*userRegistration", Depot.hashBy(classOf[UserPartitionKey]))
		setup.declareDepot("*ticketAssignment", Depot.hashBy(classOf[AssignmentPartitioner]))
		setup.declareDepot("*userNameChange", Depot.hashBy(classOf[UserNameChangePartitioner]))

		// internal
		setup.declareDepot("*mergedUserChange", Depot.hashBy(classOf[UserPartitionKey]))
		setup.declareDepot("*propagateTicketAssignment", Depot.hashBy(classOf[PropagateTicketAssignmentKey]))

		val s = topologies.stream("core")
		
		s.pstate("$$user", PState.mapSchema(classOf[JLong], classOf[User]))

		// tickets is a set of IDs, subindexed so it can be arbitrarily large
		s.pstate("$$userTickets", PState.mapSchema(classOf[JLong],
			PState.fixedKeysSchema(
				"user", classOf[User],
				"ticketIds", PState.setSchema(classOf[JLong]).subindexed,
			)))

		s.pstate("$$fullTicket", PState.mapSchema(classOf[JLong], classOf[FullTicket]))

		// upstream ticket changes update the fullTicket view
		s.source("*upstreamTicket").out("*ticket")
			.each(Ops.PRINTLN, "*ticket")
			.each((t: Ticket) => t.id, "*ticket").out("*id")
			.localTransform("$$fullTicket",
				Path.key("*id").term(new UpsertUpstreamTicket(), "*ticket")
			)

		// creates a user (further updates have no effect)
		s.source("*userRegistration").out("*user")
			.each(Ops.CURRENT_TASK_ID).out("*taskId")
			.each(Ops.PRINTLN, "*user", "on task", "*taskId")
			.each((t: User) => t.id, "*user").out("*id")

			.localSelect("$$user", Path.key("*id")).out("*existing")
			.ifTrue(new Expr(Ops.EQUAL, null, "*existing"),
				Block.localTransform("$$user",
						Path.key("*id").termVal("*user")
				)
				.depotPartitionAppend("*mergedUserChange", "*user")
				.ackReturn(null)
				, // else
				Block
					.each(UserAlreadyExists.apply, "*id").out("*error")
					.ackReturn("*error")
			)

		s.source("*userNameChange").out("*change")
			.each(Ops.CURRENT_TASK_ID).out("*taskId")
			.each(Ops.PRINTLN, "*change", "on task", "*taskId")
			.each((t: UserNameChange) => t.userId, "*change").out("*userId")

			.localSelect("$$user", Path.key("*userId")).out("*prev")
			.ifTrue(new Expr(Ops.IS_NOT_NULL, "*prev"),
				Block.each(
					(prev: User, change: UserNameChange) => {
						prev.copy(name = change.name)
					}, "*prev", "*change").out("*user")
					.localTransform("$$user",
						Path.key("*userId").termVal("*user")
					)
					.depotPartitionAppend("*mergedUserChange", "*user").out("*appendResult")
					.ackReturn("*appendResult")
				, // else
				Block
					.each(NoSuchUser.apply, "*userId").out("*error")
					.ackReturn("*error")
			)

		// When user changes (from either userRegistration or userNameChange),
		// update the user view in $$userTickets
		s.source("*mergedUserChange").out("*user")
			.each(Ops.PRINTLN, "Applying merged user change", "*user")
			.each((t: User) => t.id, "*user").out("*id")
			.each((id: JLong) => println(s"updating user $id in userTickets"), "*id")
			.localTransform("$$userTickets",
				Path.key("*id", "user").termVal("*user")
			)

		s.source("*ticketAssignment").out("*assignment")
			.each(Ops.CURRENT_TASK_ID).out("*taskId")
			.each(Ops.PRINTLN, "Assignment:", "*assignment", "on task", "*taskId")
			.each((value: TicketAssignment) => value.assigneeId, "*assignment").out("*userId")
			.each((value: TicketAssignment) => value.ticketId, "*assignment").out("*ticketId")

			// assignment is partitioned by (new) assigneeId. First validate the new assignee
			.localSelect("$$userTickets", Path.key("*userId", "user")).out("*newAssignee")
			.ifTrue(new Expr(Ops.EQUAL, null, "*newAssignee"),
				Block.each(NoSuchUser.apply, "*userId").out("*error")
					.ackReturn("*error")
			, // else
				Block.hashPartition("*ticketId")
				// then grab the current view of the ticket
				.each(Ops.CURRENT_TASK_ID).out("*taskId")
				.each(Ops.PRINTLN, "Finding ticket", "*ticketId", "on task", "*taskId")
				.localSelect("$$fullTicket", Path.key("*ticketId")).out("*prevTicket")
				.ifTrue(new Expr(Ops.EQUAL, null, "*prevTicket"),
					// if ticket == null
						Block.each(NoSuchTicket.apply, "*ticketId").out("*error")
							.ackReturn("*error")
					, // else

						// At this point we know the new user and ticket are both valid.
						// Dispatch an event keyed on the ticket ID, serializing the
						// old assignee ID so it can't be lost in the case of a retried transaction
						Block.each((t: FullTicket, newAssignee: JLong) =>
							PropagateTicketAssignment(t.upstream.id, assignee = newAssignee, prevAssignee = t.assignee)
						, "*prevTicket", "*userId").out("*message")
						.depotPartitionAppend("*propagateTicketAssignment", "*message")
				)
			)

		s.source("*propagateTicketAssignment").out("*assignment")
			.each(Ops.PRINTLN, "Propagating assignment", "*assignment")
			.each((x: PropagateTicketAssignment) => x.ticketId, "*assignment").out("*ticketId")
			.each((x: PropagateTicketAssignment) => x.assignee, "*assignment").out("*newUserId")
			.each((x: PropagateTicketAssignment) => x.prevAssignee.orNull, "*assignment").out("*oldUserId")
	
			// Update fullTicket, it's the source of truth for who owns a ticket
			.localTransform("$$fullTicket", Path.key("*ticketId").term(new SetTicketAssignee(), "*newUserId"))
			
			// now update new user
			.hashPartition("*newUserId")
			.localTransform("$$userTickets",
				// add to set
				Path.key("*newUserId", "ticketIds").voidSetElem().termVal("*ticketId")
			)

			// and remove from old user, if defined (and different from new user)
			.keepTrue(new Expr(Ops.AND,
				new Expr(Ops.IS_NOT_NULL, "*oldUserId"),
				new Expr(Ops.NOT_EQUAL, "*newUserId", "*oldUserId")
			))
			.hashPartition("*oldUserId")
			.localTransform("$$userTickets",
				// remove from set
				Path.key("*oldUserId", "ticketIds").setElem("*ticketId").termVoid()
			)

		val userTicketsPageSize = 10
		topologies.query("userTickets", "*request").out("*result")
			.each((r: UserTicketsRequest) => r.userId, "*request").out("*userId")
			.each((r: UserTicketsRequest) => r.cursor.getOrElse[JLong](Long.MinValue), "*request").out("*minId")

			.hashPartition("*userId")
			.each((uid: JLong) => println(s"fetching ticket assignment for $uid"), "*userId")

			.localSelect("$$userTickets",
				Path.key("*userId", "ticketIds").sortedSetRangeFrom("*minId", userTicketsPageSize + 1)
			).out("*ticketIds")

			// fanout each ID
			// TODO is this serial? Can I make it parallel per hashPartition?
			.each(Ops.EXPLODE, "*ticketIds").out("*ticketId")
			.hashPartition("*ticketId")
			.each(Ops.CURRENT_TASK_ID).out("*taskId")

			// lookup ticket; filter out None
			.localSelect("$$fullTicket", Path.key("*ticketId")).out("*fullTicket")
			.keepTrue(new Expr(Ops.IS_NOT_NULL, "*fullTicket"))
			.each((t: FullTicket) => t.upstream, "*fullTicket").out("*ticket")
			// .each(Ops.PRINTLN, "Found ticket", "*fullTicket", "for ID", "*ticketId")

			// send all results back to the original partition to construct the return value
			.originPartition()
			.agg(Agg.list("*ticket")).out("*tickets")
			.each((jtickets: java.util.List[Ticket]) => {
				val allTickets = jtickets.asScala
				if (allTickets.size > userTicketsPageSize) {
					val cursor = Some(allTickets.last.id)
					// more results, trim the last one and return its ID as the cursor
					UserTicketsResponse(allTickets.dropRight(1).toList, cursor)
				} else {
					// no more results
					UserTicketsResponse(allTickets.toList, None)
				}
			}, "*tickets").out("*result")

		()
	}
}

object RamaExample {
	def main(@unused argv: Array[String]): Unit = {

		def appendTo(depot: Depot, obj: Object*) = {
			obj.foreach { obj =>
				val appendResult = depot.append(obj)
				println(s"$appendResult <- from appending $obj")
			}
		}

		// TODO autoclose?
		val cluster = InProcessCluster.create()
		try {
			println("Launching module...")
			cluster.launchModule(new RamaExample(), new LaunchConfig(8, 8).numWorkers(8));
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
			
			println("Ticket 1: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](Path.key(long2Long(1L))))
			println("Ticket 2: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](Path.key(long2Long(2L))))
			println("Ticket 3: " + cluster.clusterPState(moduleName, "$$fullTicket").selectOne[Ticket](Path.key(long2Long(3L))))

			println("User 111: " + cluster.clusterPState(moduleName, "$$user").selectOne[User](Path.key(long2Long(111L))))
			println("User 222: " + cluster.clusterPState(moduleName, "$$user").selectOne[User](Path.key(long2Long(222L))))

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
