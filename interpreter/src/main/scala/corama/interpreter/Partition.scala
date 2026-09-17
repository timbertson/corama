package corama.interpreter

import corama._

class PartitionInfo(
	val id: PartitionId,
	val isr: List[InstanceId],
	val replicas: List[InstanceId],
)
