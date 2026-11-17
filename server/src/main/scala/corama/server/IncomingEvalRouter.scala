package corama.server

import cats.syntax.all.*
import cats.effect.IO
import cats.effect.Resource
import cats.effect.kernel.Ref
import cats.effect.std.Random
import corama.*
import corama.error.Util.err
import cats.effect.Clock

// manages incoming eval messages for a single instance
// TODO: do we need to prevent leaks? Should we cap slot availability?
class IncomingEvalRouter(config: ClusterConfig, slots: Ref[IO, Map[EvalId, LocalEvalSink[Any]]])(using Random[IO]) {
	private val timeoutError = new RuntimeException(s"Eval timed out after ${config.evalTimeout.toSeconds}s")
	
	def cleanupLoop: IO[Nothing] = {
		(IO.sleep(config.evalTimeout * 1.5) *> cleanupExpired).foreverM
	}
	
	private def cleanupExpired: IO[Unit] = {
		Clock[IO].realTimeInstant.flatMap { now =>
			slots.modify { slots =>
				val (keep, drop) = slots.partition { (_, slot) =>
					slot.deadline.isAfter(now)
				}
				(keep.toMap, drop)
			}.flatMap { dropped =>
				dropped.toList.traverse_(_._2.fail(timeoutError))
			}
		}
	}

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
			value <- value.traverse { bytes =>
				IO.fromTry(slot.agg.codec.decode(bytes))
			}
			_ <- slot.notify(edge, value)
		} yield ()
	}
}

object IncomingEvalRouter {
	def apply(config: ClusterConfig): ActiveResource[IO, IncomingEvalRouter] = {
		val acquire = Ref[IO].of(Map.empty[EvalId, LocalEvalSink[Any]]).map(new IncomingEvalRouter(config, _))
		ActiveResource(Resource.eval(acquire).map(router => (router, router.cleanupLoop)))
	}
}

