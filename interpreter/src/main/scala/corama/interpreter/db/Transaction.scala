package corama.interpreter.db

import corama._

case class TxId(value: Long)

case class KvUpdate(
	key: Bytes,
	value: Option[Bytes]
)

case class DbUpdates(
	pstateId: PStateID,
	updates: Vector[KvUpdate]
)

case class Transaction(
	id: TxId,
	updates: List[DbUpdates]
)
