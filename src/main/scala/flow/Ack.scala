package flow

sealed trait AckLevel[R]

object AckLevel {

	// Fire and forget, data has been accepted but not durably (not yet replicated)
	case object NoAck extends AckLevel[Unit]

	// Return once the data has been replicated
	case object AppendAck extends AckLevel[Unit]

	// Wait for all associated topologies to return
	case object Ack extends AckLevel[AckReturns]
}

/** The ack returns from one depot append, keyed by the topology that produced each.
  * Absence and "returned nothing" are indistinguishable. TODO should they be?
  */
final class AckReturns private[flow] (private val values: Map[TopologyId, Any]) {

	// TODO: assert topology is a Stream, not a Microbatch
	def apply[S, R](t: Topology[S, R]): Option[R] = {
		require(
			t.builder.source.namespace == IdentityNamespace.Depot,
			s"${t.builder.source} is not a stream topology, so it cannot produce an ack return")
		values.get(t.builder.id).map(_.asInstanceOf[R])
	}

	override def toString: String = s"AckReturns(${values.size})"
}

object AckReturns {
	val empty: AckReturns = new AckReturns(Map.empty)
}
