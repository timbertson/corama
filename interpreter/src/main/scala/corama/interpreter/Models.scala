package corama.interpreter

import cats.Eq
import cats.syntax.all.*
import corama.*
import corama.internal.HexId
import cats.effect.std.Random
import cats.effect.SyncIO
import cats.Order

/**
	* An environment is one *instance* of a topology execution, mapping vars to values.
	* A node maps one environment to 0..n environments — `Each` gives
	* one, `Filter` gives zero or one, `Explode` gives n.
	*
	* Continuing execution on a different node does not require any waiting on the source.
	* The evaluation is handed off, and any acknowledgement is routed to the original
	* requestor, not any of the intermediate nodes which played a part.
	*/
type Env = Map[VarId, Any]

case class TxId(value: Long)
object TxId {
	implicit val ord: Ordering[TxId] = Ordering.by(_.value)
	implicit val eq: Eq[TxId] = Eq.by(_.value)
}

// There are `n` tasks in an `n` node setup.
// Each task is also replicated to 3 nodes (leader + 2)
case class TaskId(id: Int)
object TaskId {
	implicit val eq: Eq[TaskId] = Eq.fromUniversalEquals
	implicit val ord: Order[TaskId] = Order.by(_.id)
}

case class PStateUpdate(key: PStateKey, value: Option[Bytes])

case class DepotAppend(value: Bytes)
case class DepotAppends(id: DepotId, updates: Array[Partitioned[DepotAppend]])

case class ReplicationUpdate(
	id: TxId,
	depotAppends: List[DepotAppends],
	pstateUpdates: Array[PStateUpdate],
)

// EvalId is dynamically minted each time we perform a fanout
case class EvalId(id: Long) extends HexId {
	override def toString(): String = s"EvalId($hex)"
}

object EvalId {
	def generate(using rand: Random[SyncIO]): SyncIO[EvalId] = rand.nextLong.map(EvalId.apply)
}

case class Partitioned[A](partition: PartitionId, value: A)
