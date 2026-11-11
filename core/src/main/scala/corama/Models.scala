package corama

import java.nio.file.Path
import cats.Eq
import cats.Order

case class DataRoot(value: Path)
object DataRoot {
	val cwd = DataRoot(Path.of("."))
}

case class PartitionId(id: Int)
object PartitionId {
	implicit val eq: Eq[PartitionId] = Eq.fromUniversalEquals
	implicit val ord: Order[PartitionId] = Order.by(_.id)
}

case class InstanceId(id: String)
object InstanceId {
	implicit val eq: Eq[InstanceId] = Eq.fromUniversalEquals
}
