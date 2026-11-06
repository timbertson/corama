package corama

final case class Depot[A](id: DepotId, codec: Codec[A]) {
	def localAppend(value: Var[A]): Flow[Unit, Etl] = Flow.LocalAppend(this, value)
}

