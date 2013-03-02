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
import scala.concurrent._
import scala.util.{ Failure, Success }

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

  /*
   * Until the bug #917 of play is fixed (and so, when Scala SI-6932 is fixed).
   * See
   *   * https://issues.scala-lang.org/browse/SI-6932 and
   *   * https://play.lighthouseapp.com/projects/82401/tickets/917-bug-in-iteratee-library-with-large-amount-of-data#ticket-917-1
   *
   */
  def unfoldM[S, E](s: S)(f: S => Future[Option[(S, E)]])(implicit ec: ExecutionContext): Enumerator[E] = {
    def process[A](loop: (Iteratee[E, A], S) => Future[Iteratee[E, A]], s: S, k: Input[E] => Iteratee[E, A]): Future[Iteratee[E, A]] = f(s).flatMap {
      case Some((newS, e)) => intermediatePromise(loop(k(Input.El(e)), newS))
      case None => Future(Cont(k))
    }
    new Enumerator[E] {
      def apply[A](it: Iteratee[E, A]): Future[Iteratee[E, A]] = {
        def step(it: Iteratee[E, A], state: S): Future[Iteratee[E, A]] = it.fold {
          case Step.Done(a, e) => Future(Done(a, e))
          case Step.Cont(k) => process[A](step, state, k)
          case Step.Error(msg, e) => Future(Error(msg, e))
        }
        step(it, s)
      }
    }
  }
}