package corama.interpreter

import cats.effect.std.Random
import cats.syntax.all.*
import cats.effect.IO
import cats.effect.kernel.Ref
import corama.*
import corama.internal.*
import corama.interpreter.Env

import flogger.*
import cats.effect.Resource
import corama.error.Util.err

// manages incoming eval messages for a single instance
class EvalReceivers(instance: InstanceId, slots: Ref[IO, Map[EvalId, LocalEvalSink[Any]]])(using Random[IO]) {
	// not exposing a bare resource so we can eagerly terminate the block if the server has too many outstanding evals
	def addRunning[A, B](destination: LocalEvalSink[A])(f: Origin => IO[B]): IO[B] = {
		val acquire = for {
			evalId <- EvalId.generate
			origin = Origin(instance, evalId)
			_ <- slots.update(_.updated(evalId, destination.asInstanceOf[LocalEvalSink[Any]]))
		} yield origin
		Resource.make(acquire) { origin =>
			slots.update(_.removed(origin.evalId))
		}.use(f)
	}
	
	def invoke(origin: Origin, edge: EdgeId, value: Option[Any]): IO[Unit] = {
		for {
			map <- slots.get
			slot <- map.get(origin.evalId).fold(IO.raiseError(err(s"No slot for ${origin}")))(IO.pure)
			_ <- slot.notify(edge, value)
		} yield ()
	}
}

object EvalReceivers {
	def apply(instance: InstanceId): Resource[IO, EvalReceivers] = {
		val acquire = Ref[IO].of(Map.empty[EvalId, LocalEvalSink[Any]]).map(new EvalReceivers(instance, _))
		Resource.make(acquire)(_ => IO.unit)
	}
}

