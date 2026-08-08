package flow

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
object Example {

	case class Ticket(id: Long, requester: String, description: String)
	case class FullTicket(ticket: Ticket, assignee: Option[Long])
	case class Assignment(ticketId: Long, assigneeId: Long)

	val topologies: Topologies = Topologies()

	val fullTickets: PState[Long, FullTicket] = topologies.pstate("fullTickets")
	val userTickets: PState[Long, Set[Long]] = topologies.pstate("userTickets")
	val users: PState[Long, String] = topologies.pstate("users")
	val ticketAssignment: Depot[Assignment] = topologies.depot("ticketAssignment")

	/** Propagate an assignment: update the ticket, add to the new owner's set,
	  * remove from the previous owner's.
	  *
	  * In Rama this spans three partitions and therefore three separate
	  * transactions — one per `hashPartition`. The `Flow.hashPartition` calls mark
	  * where, which is more than Rama's own API tells you, though the types still
	  * don't *enforce* anything about what may cross a boundary.
	  */
	def propagateAssignment(assignment: Var[Assignment]): Flow[Unit] =
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
			_ <- fullTickets.updateWith(ticketId, newOwner) { (cur, owner) =>
				cur.map(_.copy(assignee = Some(owner)))
			}

			// Partition by the new owner: add the ticket to their set. Note this write
			// is in a *different* transaction from the one above — the boundary is the
			// hashPartition, which nothing in the types currently enforces.
			_ <- Flow.hashPartition(newOwner)
			_ <- userTickets.updateWith(newOwner, ticketId) { (prev, tid) =>
				Some(prev.getOrElse(Set.empty) + tid)
			}

			// And remove from the previous owner, when there was a different one.
			hop <- Flow.map2(previousOwner, newOwner)((prev, now) => prev.filter(_ != now))
			_ <- hop.filter(_.isDefined)
			oldOwner <- hop.map(_.get)
			_ <- Flow.hashPartition(oldOwner)
			_ <- userTickets.updateWith(oldOwner, ticketId) { (prev, tid) =>
				prev.map(_ - tid)
			}
			done <- Flow.unit
		} yield done

	/** List a user's tickets: fan out over their ticket ids, look each one up on its
	  * own partition, then collapse the results back into a single list.
	  *
	  * This is the shape `aggList` exists for. Rama writes it as `.each(EXPLODE,
	  * "*ticketIds")` ... `.originPartition().agg(Agg.list("*ticket")).out("*tickets")`,
	  * where nothing indicates that the region between the two is fanned out.
	  */
	def listUserTickets(userId: Var[Long]): Flow[List[Ticket]] = {
		val perTicket: Flow[Ticket] = for {
			_ <- Flow.hashPartition(userId)
			ids <- userTickets.get(userId)
			ticketId <- ids.explode(_.getOrElse(Set.empty).toList.sorted)

			// Each ticket lives on its own partition.
			_ <- Flow.hashPartition(ticketId)
			full <- fullTickets.get(ticketId)
			_ <- full.filter(_.isDefined)
			ticket <- full.map(_.get.ticket)
		} yield ticket

		perTicket.aggList
	}

	/** The two topologies of this module — one of each kind, as in `Main.scala`.
	  *
	  * `listUserTickets` is a **query**: it reads two PStates, aggs back to one value
	  * and returns it to whoever asked (Main.scala:233, `topologies.query("userTickets",
	  * "*request").out("*result")`). `propagateAssignment` is a **stream**: driven by a
	  * depot append, writes PStates, returns nothing (Main.scala:205).
	  */
	val userTicketsQuery: Topology[Long, List[Ticket]] =
		topologies.query[Long, List[Ticket]]("userTickets")(t => listUserTickets(t.input))

	topologies.stream[Assignment](ticketAssignment)(t => propagateAssignment(t.input))

	def main(args: Array[String]): Unit = {
		println("flow.Example")
		
		val interp = new Interpreter(topologies)

		// Seed three tickets owned by user 111, one by 222.
		val tickets = List(
			Ticket(1, "Troubles McGee", "My printer is on fire!"),
			Ticket(2, "Troubles McGee", "I don't have water"),
			Ticket(3, "Passerby", "There seems to be a fire"))
		tickets.foreach(t => interp.put(fullTickets, t.id, Some(FullTicket(t, Some(111L)))))
		interp.put(userTickets, 111L, Some(Set(1L, 2L)))
		interp.put(userTickets, 222L, Some(Set(3L)))

		// A query is *invoked* and returns one value, so there's no trace to unwrap and
		// no chance of silently reading the first of several emissions.
		val listed: List[Ticket] = Interp.invoke(userTicketsQuery, 111L, store)
		println(s"  user 111 tickets: $listed")

		val expected = List(tickets(0), tickets(1))
		assert(listed == expected, s"expected $expected, got $listed")
		// One hop for the user, then one per fanned-out ticket.
		val listHops = Interp.run(userTicketsQuery, List(111L), store).head.partitionKeys
		assert(listHops == List(111L, 1L, 2L), s"got $listHops")

		// Reassign ticket 1 from user 111 to user 222 and check both sets moved.
		val moved = Interp.run(ticketAssignment, List(Assignment(1L, 222L)), store).head
		println(s"  reassign hops:    ${moved.partitionKeys}")

		assert(store.get("userTickets", 222L) == Some(Set(3L, 1L)), s"new owner: ${store.get("userTickets", 222L)}")
		assert(store.get("userTickets", 111L) == Some(Set(2L)), s"old owner: ${store.get("userTickets", 111L)}")
		assert(
			store.get("fullTicket", 1L) == Some(FullTicket(tickets(0), Some(222L))),
			s"ticket: ${store.get("fullTicket", 1L)}")
		// Three transactions: by ticket, by new owner, by old owner.
		assert(moved.partitionKeys == List(1L, 222L, 111L), s"got ${moved.partitionKeys}")

		// Reassigning to the same owner must not remove the ticket.
		val noop = Interp.run(ticketAssignment, List(Assignment(1L, 222L)), store).head
		assert(store.get("userTickets", 222L) == Some(Set(3L, 1L)), s"self-reassign: ${store.get("userTickets", 222L)}")
		assert(noop.partitionKeys == List(1L, 222L), s"expected no third hop, got ${noop.partitionKeys}")

		// And both compile to real Rama blocks, each reporting the names Rama needs
		// positionally: the query's `.out("*result")`, the stream's `.source(...).out(...)`.
		val q = RamaBackend.compileTopology(userTicketsQuery)
		val s = RamaBackend.compileTopology(ticketAssignment)
		assert(q.block != null && s.block != null)
		println(s"  query ${q.name}:   ${q.in} -> ${q.out.get}")
		println(s"  stream ${s.name}: ${s.in} (out: ${s.out})")

		println("  ok — interpreted and compiled to Rama")
	}
}
