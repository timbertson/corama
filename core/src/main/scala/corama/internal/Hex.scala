package corama.internal

object HexString {
	def apply(long: Long): String = {
		f"${long}%016x"
	}
	
}

trait HexId {
	val id: Long
	def hex: String = HexString(id)
}
