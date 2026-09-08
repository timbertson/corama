package corama

final case class Depot[A](id: Identity) {
	def localAppend(value: Var[A]): Flow[Unit, Etl] = Flow.LocalAppend(this, value)
}

