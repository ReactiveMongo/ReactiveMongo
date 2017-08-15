/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo

package object util {
  import scala.language.implicitConversions

  /** Makes an option of the value matching the condition. */
  def option[T](cond: => Boolean, value: => T): Option[T] =
    if (cond) Some(value) else None

  import scala.concurrent.{
    ExecutionContext,
    Future,
    Promise,
    duration
  }, duration.Duration

  case class EitherMappableFuture[A](future: Future[A]) {
    def mapEither[E <: Throwable, B](f: A => Either[E, B])(implicit ec: ExecutionContext) = {
      future.flatMap(
        f(_) match {
          case Left(e)  => Future.failed(e)
          case Right(b) => Future.successful(b)
        })
    }
  }

  object EitherMappableFuture {
    implicit def futureToEitherMappable[A](future: Future[A]): EitherMappableFuture[A] = EitherMappableFuture(future)
  }

  object ExtendedFutures {
    import akka.actor.ActorSystem

    // better way to this?
    def DelayedFuture(millis: Long, system: ActorSystem): Future[Unit] = {
      implicit val ec = system.dispatcher
      val promise = Promise[Unit]()

      system.scheduler.scheduleOnce(Duration.apply(millis, "millis")) {
        promise.success({}); ()
      }

      promise.future
    }
  }
}
