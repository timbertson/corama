package example

import scala.jdk.CollectionConverters._
import com.rpl.rama._
import com.rpl.rama.module._
import com.rpl.rama.RamaModule._
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
import java.util.{List => JList}
import com.strobel.collections.ListBuffer
import java.time.Instant
import rpl.rama.distributed.topology.microbatch.microbatch_state.establish_diff_processor_work_BANG_

case class JobState(
	jobType: String,
	jobId: JLong,
	state: RamaSerializable & UpdateCursor[?],
	lastRun: Instant,
) extends RamaSerializable {
	// TODO can we do dynamic things like storing the name of the pstate to query?
}

class JobStatePartitionKey extends RamaFunction1[JobState, JLong] {
	override def invoke(j: JobState): JLong = j.jobId
}

trait UpdateCursor[T <: RamaSerializable & UpdateCursor[?]] {
	def updateCursor(newCursor: Object): T
}

case class UserNameState(uid: JLong, ticketsCursor: Option[JLong]) extends RamaSerializable with UpdateCursor[UserNameState] {
	def updateCursor(newCursor: Object): UserNameState =
		copy(ticketsCursor = Some(newCursor.asInstanceOf[JLong]))
}

object UpdateJobState extends RamaFunction2[JobState, Object, JobState] {
	def invoke(current: JobState, newCursor: Object): JobState = {
		current.copy(state = current.state.updateCursor(newCursor))
	}
}

object JobSubmission extends RamaFunction2[JobState, JobState, JobState] {
	def invoke(current: JobState, submitted: JobState): JobState = {
		Option(current).getOrElse(submitted)
	}
}

// propagates
object UserNameJob {
	def apply(jobVar: String): Block.Impl = {
		// todo: GenVar
		val userNameState = Helpers.genVar("userNameState")
		val minRange = Helpers.genVar("minRange")
		val user = Helpers.genVar("user")
		val userName = Helpers.genVar("userName")
		val uid = Helpers.genVar("uid")
		val jobId = Helpers.genVar("jobId")
		val ticketIds = Helpers.genVar("ticketIds")
		val ticketId = Helpers.genVar("ticketId")
		val nextCursor = Helpers.genVar("nextCursor")
		Block.each((x: JobState) => x.state.asInstanceOf[UserNameState], jobVar).out(userNameState)
		.each((x: JobState) => x.jobId, jobVar).out(jobId)
		.each((s: UserNameState) => s.uid, userNameState).out(uid)
		.each((s: UserNameState) => s.ticketsCursor.getOrElse(Long.MinValue), userNameState).out(minRange)
		.localSelect("$$user", Path.key(uid)).out(user)
		.each((x: User) => x.name, user).out(userName)
		.localSelect("$$userTickets", Path.key(uid, "tickets").sortedMapRangeFrom(minRange, 100)).out(ticketIds)

		// figure out the next cursor based on the returned list
		.each((ids: JList[JLong]) => {
			if (ids.isEmpty()) {
				null
			} else {
				ids.get(ids.size() - 1)
			}
		}, ticketIds).out(nextCursor)

		.each(Ops.EXPLODE, ticketIds).out(ticketId)
		.hashPartition(ticketId)
		.localTransform("$$fullTicket", Path.key(ticketId).term(UpdateTicketAssigneeName, userName))
		.hashPartition(jobId)
		.ifTrue(new Expr(Ops.IS_NULL, nextCursor),
			// if null
			Block.`macro`(Job.removeJob(jobVar, "complete")),
			
			// else
			Block.localTransform("$$jobs", Path.key(jobId).term(UpdateJobState, nextCursor))
		)
	}

	def applyDsl(
		jobPstate: dsl.Var[PState],
		userPstate: dsl.Var[PState],
		userTicketsPstate: dsl.Var[PState],
		ticketPstate: dsl.Var[PState],
		fullTicketPstate: dsl.Var[PState],
		job: dsl.Var[JobState]
	): dsl.BlockM[?] = {
		import dsl.*
		for {
			userNameState <- job.map(_.state.asInstanceOf[UserNameState])
			jobId <- job.map(_.jobId)
			uid <- userNameState.map(_.uid)
			minRange <- userNameState.map(_.ticketsCursor.getOrElse(Long.MinValue))

			user: Var[User] <- localSelect[User](userPstate, Path.key(uid))
			userName <- user.map(_.name)
			ticketIds <- localSelect[JList[Long]](userTicketsPstate, Path.key(uid).key("tickets").sortedMapRangeFrom(minRange, 100))

			// figure out the next cursor based on the returned list
			nextCursor <- ticketIds.map { ids =>
				ids.asScala.lastOption.map(_ + 1L).orNull
			}

			// update assignee on each ticket
			ticketId <- explodeList(ticketIds)
			_ <- hashPartition(ticketId)
			_ <- localTransform(fullTicketPstate, Path.key(ticketId).key("assigneeName").termVal(userName))

			// update job state
			_ <- hashPartition(jobId)
			_ <- ifTrue(Expr.isNull(nextCursor),
				thenDo = lift(Job.removeJob(job.raw, "complete")),
				elseDo = localTransform(jobPstate, Path.key(jobId).term(UpdateJobState, nextCursor))
			)
		} yield Var.nothing
	}
}

object Job {
	def removeJob(jobvar: String, reason: String) = {
		val jobid = Helpers.genVar("jobId")
		Block.each((j: JobState) => j.jobId, jobvar).out(jobid)
		.hashPartition(jobid)
		.each(Ops.PRINTLN, "Removing job", jobvar, reason)
		.localTransform("$$jobs", Path.key(jobid).termVoid())
	}

	def define(setup: Setup, topologies: Topologies): Unit = {
		import dsl.*
		setup.declareTickDepot("*tick", 1000)
		setup.declareDepot("*jobs", Depot.hashBy(classOf[JobStatePartitionKey]))
		
		val s = topologies.stream("fanout")
		val jobState = dsl.Var.pstate("jobs")
		
		s.pstate(jobState.raw, PState.mapSchema(classOf[JLong], classOf[JobState]))

		s.sourceBlock[JobState](Var.depot("jobs")) { (job) =>
			for {
				jobId <- job.map(_.jobId)
				_ <- localTransform(jobState, Path.key(jobId).nullToVal(job).term(Ops.IDENTITY))
			} yield Var.nothing
		}
		s.sourceBlock(Var.depot("tick")) { _ =>
			for {
				_ <- allPartition
				_ <- lift(Block.each(Ops.PRINTLN, "tick!"))
				jobs: Var[JList[JobState]] <- localSelect(Var.pstate("jobs"), Path.sortedSetRangeFrom("", 200).mapVals)
				job <- jobs.map(jobs => jobs.asScala.sortBy(_.lastRun).headOption.orNull)
				_ <- keepTrue(Expr.notNull(job))
				_ <- job.foreach(job => println("Executing job" + job))
				jobType <- job.map(_.jobType)
				jobId <- job.map(_.jobId)
				_ <- cond(
					caseExpr(Expr.eq(jobType, "userName")) { lift(UserNameJob(job.raw)) },
					defaultCase {
						for {
							reason <- each(jobType, t => s"Unknown jobType: $t")
							_ <- lift(removeJob(job.raw, reason.raw))
						} yield Var.nothing
					}
				)
			} yield Var.nothing
		}
		
		// s.source("*tick")
		// .allPartition()
		// .localSelect("$$jobs", Path.sortedSetRangeFrom("", 200).mapVals()).out("*jobs")
		// // TODO: rate limiting; skip if lastRun is within certain duration?
		// .each((jobs: JList[JobState]) => jobs.asScala.sortBy(_.lastRun).headOption.orNull, "*jobs").out("*job")
		// .keepTrue(new Expr(Ops.IS_NOT_NULL, "*job"))
		// .each(Ops.PRINTLN, "Executing job", "*job")
		// .each((x: JobState) => x.jobType, "*job").out("*jobType")
		// .each((x: JobState) => x.jobId, "*job").out("*jobId")
		
		// // now execute the job:
		// // todo subbatch?
		// .cond(
		// 	Case.create(new Expr(Ops.EQUAL, "userName", "*jobType")).`macro`(UserNameJob("*job")),
		// 	Case.create(true)
		// 		.each((jobType: String) => s"UnknownType: " + jobType, "*jobType").out("*reason")
		// 		.`macro`(removeJob("*job", "*reason"))
		// )

		// ()
	}
}


/*
 * There's a single pstate for fanouts
 *
 * partitioned by (fanoutName, rootEntity)
 *  - state is per job type
 *
 *
 * I wonder if the state can be stored by lastProcessTime,
 * in order to pick up the oldest work first
 *
 *
 * Account moves:
 * each pstate, run through and get account_id from toplevel items
 * ...or index by account_id
 * send elements, subindexed ones by batch?
 * buffer up deltas somehow (how to catch them all?)
 *  - can we just replay all events? Maybe, at least for everything that's deterministic
 *  - what sort of things aren't? ID generation maybe
 *  - it's probably quite hard to verify correctness though :/
 *
 * accountmove outbox does
 *  - global pstate to maintain lock status
 *  - on locked, push event to account move's partition
 *  - stored in a pstate, sorted by arrival order
 *  - once bulk is complete, we push directly instead of updating pstate
 *   - we know it's non-racey because we're the only writer
 *
 * bulk phase must complete before we start draining the outbox
 *  - needs to iterate over all pstates? eek
 *  - can we maintain an account_id index of every pstate?
 *  - each pstate schema should be amenable to account_id indexing somehow
 *  - does it need a full scan tho?
 *
 *
 * if each pstate is a map of (key, _) then we can store ((account_id, pstate), [ ids ])
 * a generic GSI index would be pretty nice
 * how to manage lag? storing the lastmod should work
 *
 * for account_id GSI, only operations which create/delete should trigger a GSI update.
 * such operations could literally be (add,remove)
 */
