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

import scala.collection.immutable.{ ListSet, Map, Set }

import scala.concurrent.ExecutionContext

private[reactivemongo] trait UtilCompat {
  import scala.collection.breakOut

  /** Compatibility type alias */
  type TraversableOnce[T] = scala.collection.TraversableOnce[T]

  @inline private[reactivemongo] def toListSet[A, B](in: Iterable[A])(f: A => B): ListSet[B] = in.map(f)(breakOut)

  @inline private[reactivemongo] def toMap[T, K, V](in: Iterable[T])(f: T => (K, V)): Map[K, V] = in.map(f)(breakOut)

  @inline private[reactivemongo] def toFlatMap[T, K, V](in: Iterable[T])(f: T => Iterable[(K, V)]): Map[K, V] = in.flatMap(f)(breakOut)

  @inline private[reactivemongo] def toStream[T](in: TraversableOnce[T]) =
    in.toStream

  @inline private[reactivemongo] def lazyZip[A, B](a: Set[A], b: Set[B]) = (a -> b).zipped

  @inline private[reactivemongo] def lazyZip[A, B](a: Iterable[A], b: Iterable[B]) = (a -> b).zipped

  private[reactivemongo] type ArrayOps[T] = scala.collection.mutable.ArrayOps[T]

  @inline private[reactivemongo] def sameThreadExecutionContext: ExecutionContext = SameThreadExecutionContext

  private object SameThreadExecutionContext extends ExecutionContext {
    def execute(command: Runnable): Unit = command.run()

    def reportFailure(t: Throwable): Unit =
      throw new IllegalStateException(
        "exception in sameThreadExecutionContext", t)
  }
}
