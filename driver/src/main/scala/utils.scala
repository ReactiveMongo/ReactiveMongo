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
package reactivemongo.utils

import scala.concurrent._
import scala.concurrent.duration._

object `package` {
  /** Concats two array - fast way */
  def concat[T](a1: Array[T], a2: Array[T])(implicit m: Manifest[T]): Array[T] = {
    var i, j = 0
    val result = new Array[T](a1.length + a2.length)
    while (i < a1.length) {
      result(i) = a1(i)
      i = i + 1
    }
    while (j < a2.length) {
      result(i + j) = a2(j)
      j = j + 1
    }
    result
  }

  /** Makes an option of the value matching the condition. */
  def option[T](cond: => Boolean, value: => T): Option[T] = (if (cond) Some(value) else None)
}

case class LazyLogger(logger: org.slf4j.Logger) {
  def trace(s: => String) { if (logger.isTraceEnabled) logger.trace(s) }
  def debug(s: => String) { if (logger.isDebugEnabled) logger.debug(s) }
  def info(s: => String) { if (logger.isInfoEnabled) logger.info(s) }
  def warn(s: => String) { if (logger.isWarnEnabled) logger.warn(s) }
  def error(s: => String) { if (logger.isErrorEnabled) logger.error(s) }
  def error(s: => String, e: => Throwable) { if (logger.isErrorEnabled) logger.error(s, e) }
}

object LazyLogger {
  def apply(logger: String): LazyLogger = LazyLogger(org.slf4j.LoggerFactory.getLogger("ReadFileEntry"))
}

case class EitherMappableFuture[A](future: Future[A]) {
  def mapEither[E <: Throwable, B](f: A => Either[E, B])(implicit ec: ExecutionContext) = {
    future.flatMap(
      f(_) match {
        case Left(e) => Future.failed(e)
        case Right(b) => Future.successful(b)
      })
  }
}
object EitherMappableFuture {
  implicit def futureToEitherMappable[A](future: Future[A]): EitherMappableFuture[A] = EitherMappableFuture(future)
}

object ExtendedFutures {
  import akka.actor.{ ActorSystem, Scheduler }

  // better way to this?
  def DelayedFuture(millis: Long, system: ActorSystem): Future[Unit] = {
    implicit val ec = system.dispatcher
    val promise = Promise[Unit]()
    system.scheduler.scheduleOnce(Duration.apply(millis, "millis"))(promise.success())
    promise.future
  }
}