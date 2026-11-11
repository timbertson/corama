package corama.interpreter

import cats.effect.IO
import cats.effect.Resource
import cats.effect.kernel.Ref
import cats.effect.std.Random
import cats.syntax.all.*
import corama.*
import corama.error.Util.err

// manages incoming eval messages for a single instance
// TODO: do we need to prevent leaks? Should we cap slot availability?
class IncomingEvalRouter(slots: Ref[IO, Map[EvalId, LocalEvalSink[Any]]])(using Random[IO]) {
	def running[A](destination: LocalEvalSink[A]): Resource[IO, Unit] = {
		val acquire = for {
			_ <- slots.update(_.updated(destination.origin.evalId, destination.asInstanceOf[LocalEvalSink[Any]]))
		} yield ()
		Resource.make(acquire) { _ =>
			slots.update(_.removed(destination.origin.evalId))
		}
	}
	
	def invoke(evalId: EvalId, edge: EdgeId, value: Option[Bytes]): IO[Unit] = {
		for {
			map <- slots.get
			// TODO: classify as 404
			slot <- map.get(evalId).fold(IO.raiseError(err(s"No slot for $evalId")))(IO.pure)
			_ <- slot.notify(edge, value)
		} yield ()
	}
}

object IncomingEvalRouter {
	def apply: IO[IncomingEvalRouter] = {
		Ref[IO].of(Map.empty[EvalId, LocalEvalSink[Any]]).map(new IncomingEvalRouter(_))
	}
}

