package corama.server

import corama.Codec
import corama.interpreter.*
import corama.*

/** Wire codecs for the replog payload (`ReplicationUpdate`), built on the existing
	* `Codec` combinators (tuples / lists / length-prefixed primitives).
	*/
object ServerCodecs {
	import Codec.via
	implicit val txId: Codec[TxId] = via[TxId, Long](_.value)(TxId.apply)
	implicit val depotId: Codec[DepotId] = via[DepotId, String](_.value)(DepotId.apply)
	implicit val pstateId: Codec[PStateId] = via[PStateId, String](_.value)(PStateId.apply)
	implicit val depotAppend: Codec[DepotAppend] = via[DepotAppend, Bytes](_.value)(DepotAppend.apply)
	implicit val pstateKey: Codec[PStateKey] =
		via[PStateKey, Bytes](k => k.raw.raw) { bytes => PStateKey(Bytes.Value(bytes)) }

	implicit val pstateUpdate: Codec[PStateUpdate] =
		via[PStateUpdate, (PStateKey, Option[Bytes])](p => (p.key, p.value)) { case (key, value) => PStateUpdate(key, value) }

	implicit val depotAppends: Codec[DepotAppends] =
		via[DepotAppends, (DepotId, Array[(Int, Bytes)])]
			(d => (d.id, d.updates.map(append => (append.partition.id, append.value.value))))
			{ case (id, values) => DepotAppends(id, values.map { (partition, value) => Partitioned(PartitionId(partition), DepotAppend(value)) }.toArray) }

	implicit val replicationUpdate: Codec[ReplicationUpdate] =
		via[ReplicationUpdate, ((TxId, List[DepotAppends]), Array[PStateUpdate])](r => ((r.id, r.depotAppends), r.pstateUpdates)) {
			case ((id, depotAppends), pstateUpdates) => ReplicationUpdate(id, depotAppends, pstateUpdates)
		}
}
