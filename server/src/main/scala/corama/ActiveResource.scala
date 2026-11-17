package corama

import cats.*
import cats.syntax.all.*
import cats.effect.*
import cats.data.NonEmptyList
import corama.error.Util

// A Resource[IO,A] with a background IO[Nothing]. When used, the background
// task is run concurrently with the foreground, and errors / cancelation propagate.
case class ActiveResource[F[_], A](resource: Resource[F, (A, F[Nothing])])(implicit F: Async[F]) {
	def use[B](action: A => F[B]): F[B] = {
		resource.use { case (res, task) =>
			F.race(action(res), task).map(_.merge)
		}
	}

	def map[B](f: A => B): ActiveResource[F, B] = ActiveResource(resource.map { case (a, task) => (f(a), task) })

}

object ActiveResource {
	def parTraverse[A, B](items: NonEmptyList[A])(f: A => ActiveResource[IO, B]): ActiveResource[IO, NonEmptyList[B]] = {
		ActiveResource(items.traverse { res =>
			f(res).resource
		}.map { tuples =>
			(tuples.map(_._1), tuples.map(_._2).parSequence_ *> Util.neverReached)
		})
	}

	def parSequence[A, B](items: NonEmptyList[ActiveResource[IO, A]]): ActiveResource[IO, NonEmptyList[A]] =
		parTraverse(items)(identity)
}
