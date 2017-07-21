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

import scala.util.{ Failure, Success }

import scala.concurrent.{ ExecutionContext, Future, Promise }

object CustomEnumeratee {
  import ExecutionContext.{ global => globalEc }

  trait RecoverFromErrorFunction {
    def apply[E, A](throwable: Throwable, input: Input[E], continue: () => Iteratee[E, A]): Iteratee[E, A]
  }

  object RecoverFromErrorFunction {
    object StopOnError extends RecoverFromErrorFunction {
      def apply[E, A](throwable: Throwable, input: Input[E], continue: () => Iteratee[E, A]): Iteratee[E, A] =
        Error(throwable.getMessage(), input)
    }

    object ContinueOnError extends RecoverFromErrorFunction {
      def apply[E, A](throwable: Throwable, input: Input[E], continue: () => Iteratee[E, A]): Iteratee[E, A] =
        continue()
    }
  }

  def stopOnError[E]: Enumeratee[E, E] =
    recover(RecoverFromErrorFunction.StopOnError)(globalEc)

  def continueOnError[E]: Enumeratee[E, E] =
    recover(RecoverFromErrorFunction.ContinueOnError)(globalEc)

  def recover[E](ƒ: RecoverFromErrorFunction)(implicit ec: ExecutionContext): Enumeratee[E, E] = {
    new Enumeratee[E, E] {
      def applyOn[A](it: Iteratee[E, A]): Iteratee[E, Iteratee[E, A]] = {
        def step(it: Iteratee[E, A])(input: Input[E]): Iteratee[E, Iteratee[E, A]] = input match {
          case in @ (Input.El(_) | Input.Empty) =>
            val next: Future[Iteratee[E, Iteratee[E, A]]] = it.pureFlatFold[E, Iteratee[E, A]] {
              case Step.Cont(k) =>
                val n = k(in)
                n.pureFlatFold[E, Iteratee[E, A]] {
                  case Step.Cont(_) => Cont(step(n))
                  case _            => Done(n, Input.Empty)
                }
              case other => Done(other.it, in)
            }.unflatten.map({ s =>
              s.it
            }).recover({
              case e: Throwable =>
                ƒ(e, in, () => Cont(step(it)))
            })
            Iteratee.flatten(next)
          case Input.EOF =>
            Done(it, Input.Empty)
        }
        Cont(step(it))
      }
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

  class SEnumerator[C](zero: C)(next: C => Future[Option[C]], cleanUp: C => Unit)(implicit ec: ExecutionContext) extends Enumerator[C] {

    def apply[A](iteratee: Iteratee[C, A]): Future[Iteratee[C, A]] = {

      def loop(current: C, iteratee: Iteratee[C, A]): Future[Iteratee[C, A]] =
        iteratee.fold {
          case Step.Cont(ƒ) => next(current) flatMap {
            case Some(nnx) => loop(nnx, ƒ(Input.El(nnx)))
            case _ =>
              val it = ƒ(Input.Empty)
              cleanUp(current)
              Future.successful(it)
          }

          case Step.Done(a, e) =>
            val done = Done(a, e)
            cleanUp(current)
            Future.successful(done)

          case Step.Error(msg, e) =>
            val error = Error(msg, e)
            cleanUp(current)
            Future.successful(error)
        }

      iteratee fold {
        case Step.Cont(ƒ) =>
          loop(zero, ƒ(Input.El(zero)))
        case Step.Done(a, e) =>
          Future.successful(Done(a, e))

        case Step.Error(msg, e) =>
          Future.successful(Error(msg, e))
      }

    }
  }

  object SEnumerator {
    def apply[C](zero: C)(next: C => Future[Option[C]])(implicit ec: ExecutionContext) = new SEnumerator(zero)(next, _ => ())
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
              case Some(state) => (Some(state), f(Input.El(state)))
              case None        => (None, f(Input.EOF))
            }

            future.andThen {
              case Success((state, iteratee)) => {
                if (state.isDefined) inloop(nextChunk(state.get), iteratee)
                else promise.success(iteratee)
              }

              case Failure(e) => promise.failure(e)
            }.map(_._2)
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

        ()
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
