package corama

import java.nio.file.Path

case class DataRoot(value: Path)
object DataRoot {
	val cwd = DataRoot(Path.of("."))
}

case class PartitionId(value: Int)

case class InstanceId(id: String)
