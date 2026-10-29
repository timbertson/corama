package corama.internal

object HexString {
	def apply(long: Long): String = {
		f"${long}%016x"
	}

	private[corama] def slowDebug(bytes: Array[Byte]): String = {
		bytes.map(b => String.format("%02x", b)).mkString
	}
}

trait HexId {
	val id: Long
	def hex: String = HexString(id)
}
