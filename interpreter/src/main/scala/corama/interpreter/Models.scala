package corama.interpreter

import cats.Eq
import cats.syntax.all.*
import corama.*

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
}

case class PStateUpdate(key: PStateKey, value: Option[Bytes])

case class DepotAppend(value: Bytes)
case class DepotAppends(id: DepotId, updates: Array[Partitioned[DepotAppend]])

case class ReplicationUpdate(
	id: TxId,
	depotAppends: List[DepotAppends],
	pstateUpdates: Array[PStateUpdate],
)
