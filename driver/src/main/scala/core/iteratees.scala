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
package reactivemongo.core.iteratees

import play.api.libs.iteratee._
import play.api.libs.iteratee.Enumeratee.CheckDone
import scala.concurrent._
import scala.util.{ Failure, Success }

object CustomEnumeratee {
  object TakeTo {
    def apply[E](p: E => Boolean): Enumeratee[E, E] = new TakeTo(p)
  }

  /**
   * Take elements while the predicate is true, and stops including the last evaluated element.
   *
   * More formally, takes all the elements while the predicate is valid and the first for which the predicate returns false.
   */
  class TakeTo[E](p: E => Boolean) extends Enumeratee[E, E] {
    def step[A](k: K[E, A]): K[E, Iteratee[E, A]] = {
      case in @ Input.El(e) =>
        new CheckDone[E, E] {
          def continue[A](k: K[E, A]) =
            if (!p(e)) {
              Done(Cont(k), in)
            } else {
              Cont(step(k))
            }
        } &> k(in)

      case in @ Input.Empty =>
        new CheckDone[E, E] { def continue[A](k: K[E, A]) = Cont(step(k)) } &> k(in)

      case Input.EOF => Done(Cont(k), Input.EOF)
    }

    def continue[A](k: K[E, A]) = Cont(step(k))

    override def applyOn[A](it: Iteratee[E, A]): Iteratee[E, Iteratee[E, A]] =
      it.pureFlatFold {
        case Step.Cont(k) => continue(k)
        case _            => Done(it, Input.Empty)
      }

  }

}

object CustomEnumerator {
  private[iteratees] def intermediatePromise[A](future: Future[A])(implicit ec: ExecutionContext) = {
    val promise = Promise[A]()
    future.onComplete {
      case Success(s) =>
        promise.success(s)
      case Failure(f) =>
        promise.failure(f)
    }
    promise.future
  }

  class SEnumerator[C](zero: C)(next: C => Option[Future[C]])(implicit ec: ExecutionContext) extends Enumerator[C] {
    def apply[A](iteratee: Iteratee[C, A]): Future[Iteratee[C, A]] = {
      val promise = Promise[Iteratee[C, A]]

      def loop(current: C, iteratee: Iteratee[C, A]): Unit = {
        iteratee.fold {
          case Step.Cont(ƒ) =>
            next(current) match {
              case Some(future) =>
                val f = intermediatePromise(future.map { c =>
                  (c, ƒ(Input.El(c)))
                })
                f.onComplete {
                  case Success((c, i)) =>
                    loop(c, i)
                  case Failure(e) =>
                    promise.failure(e)
                }
                f
              case None =>
                val it = ƒ(Input.Empty)
                // enumerator is done, nothing to do
                promise.success(it)
                Future(it)
            }
          case Step.Done(a, e) =>
            val done = Done(a, e)
            promise.success(done)
            Future(done)
          case Step.Error(msg, e) =>
            val error = Error(msg, e)
            promise.success(error)
            Future(error)
        }
      }

      iteratee fold {
        case Step.Cont(ƒ) =>
          val it = ƒ(Input.El(zero))
          loop(zero, it)
          Future(it)
        case Step.Done(a, e) =>
          val done = Done(a, e)
          promise.success(done)
          Future(done)
        case Step.Error(msg, e) =>
          val error = Error(msg, e)
          promise.success(error)
          Future(error)
      }

      promise.future
    }
  }
  object SEnumerator {
    def apply[C](zero: C)(next: C => Option[Future[C]])(implicit ec: ExecutionContext) = new SEnumerator(zero)(next)
  }

  /*
   * Until the bug #917 of play is fixed (and so, when Scala SI-6932 is fixed).
   * See
   *   * https://issues.scala-lang.org/browse/SI-6932 and
   *   * https://play.lighthouseapp.com/projects/82401/tickets/917-bug-in-iteratee-library-with-large-amount-of-data#ticket-917-1
   *
   */
  class StateEnumerator[Chunk](zero: Future[Option[Chunk]])(nextChunk: Chunk => Future[Option[Chunk]])(implicit ec: ExecutionContext) extends Enumerator[Chunk] {
    def apply[A](i: Iteratee[Chunk, A]): Future[Iteratee[Chunk, A]] = {
      val promise = Promise[Iteratee[Chunk, A]]
      def inloop(step: Future[Option[Chunk]], iteratee: Iteratee[Chunk, A]): Unit = {
        iteratee.fold {
          case Step.Cont(f) => {
            val future = step.map {
              case Some(state) =>
                (Some(state), f(Input.El(state)))
              case None =>
                (None, f(Input.EOF))
            }
            future.onSuccess {
              case (state, iteratee) =>
                if (state.isDefined)
                  inloop(nextChunk(state.get), iteratee)
                else
                  promise.success(iteratee)
            }
            future.onFailure {
              case e => promise.failure(e)
            }
            future.map(_._2)
          }
          case Step.Done(a, e) => {
            val finalIteratee = Done(a, e)
            promise.success(finalIteratee)
            Future.successful(finalIteratee)
          }
          case Step.Error(msg, e) => {
            val finalIteratee = Error(msg, e)
            promise.success(finalIteratee)
            Future.successful(finalIteratee)
          }
        }
      }
      inloop(zero, i)
      promise.future
    }
  }

  object StateEnumerator {
    def apply[Chunk](zero: Future[Option[Chunk]])(nextChunk: Chunk => Future[Option[Chunk]])(implicit ec: ExecutionContext): Enumerator[Chunk] =
      new StateEnumerator(zero)(nextChunk)
  }
}