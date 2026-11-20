package corama.server

import streamdsl.rpc.AppendRequest
import streamdsl.rpc.QueryRequest
import scala.util.Try
import corama.Topologies
import corama.error.Util
import corama.DepotId
import corama.Encoded
import corama.PartitionId
import corama.AckLevel
import corama.QueryId

// resolves topologies, partitions & inputs.
// Used by both InstanceController & APIController
class RequestResolver(config: ClusterConfig, topologies: Topologies) {
	def resolveAppend(request: AppendRequest): Try[Partitioned[TaskCommand.Append[?]]] = {
		val depotId = DepotId(request.depot)
		for {
				depot <- topologies.depots.get(depotId).toRight(Util.err("No such depot")).toTry
				bytes = request.value.toByteArray
				decoded <- Encoded.fromBytes(bytes)(using depot.codec)
				partition = getPartition(request.partition, decoded)
		} yield Partitioned(partition, TaskCommand.Append(depot, partition, decoded, AckLevel.Ack)) // TODO parse ack level
	}

	def resolveQuery(request: QueryRequest): Try[Partitioned[TaskCommand.Query[?, ?]]] = {
		// TODO: we assume we're partitionin on input, use the first partition step in the topology
		// or default to a random partition in the current task.

		val queryId = QueryId(request.query)
		for {
			query <- topologies.queries.get(queryId).toRight(Util.err("No such query")).toTry
			bytes = request.value.toByteArray
			decoded <- Encoded.fromBytes(bytes)(using query.topology.builder.input.codec)
			partition = getPartition(request.partition, decoded)
		} yield Partitioned(partition, TaskCommand.Query(query, partition, decoded))
	}
	
	// TODO: no need to decode in ApiController if we received an explicit partition
	private def getPartition[A](explicit: Option[Int], decoded: Encoded[A]): PartitionId = {
		explicit.fold(
			config.partitionKey(decoded.value)
		)(PartitionId.apply)
	}
}

