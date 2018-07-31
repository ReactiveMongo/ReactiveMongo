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
package reactivemongo.api

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.util.LazyLogger

import reactivemongo.core.protocol.Response

/**
 * Cursor over results from MongoDB.
 *
 * @tparam T the type parsed from each result document
 * @define maxDocsParam the maximum number of documents to be retrieved (-1 for unlimited)
 * @define maxDocsWarning The actual document count can exceed this, when this maximum devided by the batch size given a non-zero remainder
 * @define stopOnErrorParam States if may stop on non-fatal exception (default: true). If set to false, the exceptions are skipped, trying to get the next result.
 * @define errorHandlerParam The binary operator to be applied when failing to get the next response. Exception or [[reactivemongo.api.Cursor$.Fail Fail]] raised within the `suc` function cannot be recovered by this error handler.
 * @define zeroParam the initial value
 * @define getHead Returns the first document matching the query
 * @define collect Collects all the documents into a collection of type `M[T]`
 * @define resultTParam the result type of the binary operator
 * @define sucRespParam The binary operator to be applied when the next response is successfully read
 * @define foldResp Applies a binary operator to a start value and all responses handled by this cursor, going first to last.
 * @define sucSafeWarning This must be safe, and any error must be returned as `Future.failed[State[A]]`
 * @define foldBulks Applies a binary operator to a start value and all bulks of documents retrieved by this cursor, going first to last.
 * @define foldWhile Applies a binary operator to a start value and all elements retrieved by this cursor, going first to last.
 * @define sucDocParam The binary operator to be applied when the next document is successfully read
 */
trait Cursor[T] {
  // TODO: maxDocs; 0 for unlimited maxDocs
  import Cursor.{ ErrorHandler, FailOnError }

  /**
   * $collect.
   *
   * @param maxDocs $maxDocsParam.
   * @param err $errorHandlerParam
   *
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]()
   * // return the 3 first documents in a Vector[BSONDocument].
   * val vector = cursor.collect[Vector](3, Cursor.FailOnError[Vector[BSONDocument]]())
   * }}}
   */
  def collect[M[_]](maxDocs: Int, err: ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]]

  /**
   * $foldResp
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldResponses[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldResp
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam. $sucSafeWarning.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldResponsesM[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldBulks
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldBulks[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldBulks
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam. $sucSafeWarning.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldBulksM[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldWhile
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam.
   * @param suc $sucDocParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   *
   * {{{
   * cursor.foldWhile(Nil: Seq[Person])((s, p) => Cursor.Cont(s :+ p),
   *   { (l, e) => println("last valid value: " + l); Cursor.Fail(e) })
   * }}}
   */
  def foldWhile[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldWhile
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam.
   * @param suc $sucDocParam. $sucSafeWarning.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   *
   * {{{
   * cursor.foldWhileM(Nil: Seq[Person])(
   *   (s, p) => Future.successful(Cursor.Cont(s :+ p)),
   *   { (l, e) => Future {
   *     println("last valid value: " + l)
   *     Cursor.Fail(e)
   *   })
   * }}}
   */
  def foldWhileM[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldWhile
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam.
   * @param suc $sucDocParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   *
   * {{{
   * cursor.foldWhile(Nil: Seq[Person])((s, p) => Cursor.Cont(s :+ p),
   *   { (l, e) => println("last valid value: " + l); Cursor.Fail(e) })
   * }}}
   */
  def fold[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => A)(implicit ctx: ExecutionContext): Future[A] = foldWhile[A](z, maxDocs)(
    { (st, v) => Cursor.Cont[A](suc(st, v)) }, FailOnError[A]())

  /**
   * $getHead, or fails with [[Cursor.NoSuchResultException]] if none.
   *
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]
   * // return option of the first element.
   * val first: Future[BSONDocument] = cursor.head
   * }}}
   */
  def head(implicit ctx: ExecutionContext): Future[T]

  /**
   * $getHead, if any.
   *
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]
   * // return option of the first element.
   * val maybeFirst: Future[Option[BSONDocument]] = cursor.headOption
   * }}}
   */
  def headOption(implicit ctx: ExecutionContext): Future[Option[T]]
}

/** Cursor companion object */
object Cursor {
  private[api] val logger = LazyLogger("reactivemongo.api.Cursor")

  /**
   * @tparam A the state type
   * @see [[Cursor.foldWhile]]
   */
  type ErrorHandler[A] = (A, Throwable) => State[A]

  /**
   * Error handler to fail on error (see [[Cursor.foldWhile]] and [[reactivemongo.api.Cursor$.Fail Fail]]).
   *
   * @param callback the callback function applied on last (possibily initial) value and the encountered error
   */
  def FailOnError[A](callback: (A, Throwable) => Unit = (_: A, _: Throwable) => {}): ErrorHandler[A] = (v: A, e: Throwable) => { callback(v, e); Fail(e): State[A] }

  /**
   * Error handler to end on error (see [[Cursor.foldWhile]] and [[reactivemongo.api.Cursor$.Done Done]]).
   *
   * @param callback the callback function applied on last (possibily initial) value and the encountered error
   */
  def DoneOnError[A](callback: (A, Throwable) => Unit = (_: A, _: Throwable) => {}): ErrorHandler[A] = (v: A, e: Throwable) => { callback(v, e); Done(v): State[A] }

  /**
   * Error handler to continue on error (see [[Cursor.foldWhile]] and [[reactivemongo.api.Cursor$.Cont Cont]]).
   *
   * @param callback the callback function applied on last (possibily initial) value and the encountered error
   */
  def ContOnError[A](callback: (A, Throwable) => Unit = (_: A, _: Throwable) => {}): ErrorHandler[A] = (v: A, e: Throwable) => { callback(v, e); Cont(v): State[A] }

  /**
   * Value handler, ignoring the values (see [[Cursor.foldWhile]] and [[reactivemongo.api.Cursor$.Cont Cont]]).
   *
   * @param callback the callback function applied on each value.
   */
  def Ignore[A](callback: A => Unit = (_: A) => {}): (Unit, A) => State[Unit] = { (_, a) => Cont(callback(a)) }

  /**
   * Flattens the given future [[reactivemongo.api.Cursor]] to a [[reactivemongo.api.FlattenedCursor]].
   */
  def flatten[T, C[_] <: Cursor[_]](future: Future[C[T]])(implicit fs: CursorFlattener[C]): C[T] = fs.flatten(future)

  /** A state of the cursor processing. */
  sealed trait State[T] {
    /**
     * @param f the function applied on the statue value
     */
    def map[U](f: T => U): State[U]
  }

  /** Continue with given value */
  case class Cont[T](value: T) extends State[T] {
    def map[U](f: T => U): State[U] = Cont(f(value))
  }

  /** Successfully stop processing with given value */
  case class Done[T](value: T) extends State[T] {
    def map[U](f: T => U): State[U] = Done(f(value))
  }

  /** Ends processing due to failure of given `cause` */
  case class Fail[T](cause: Throwable) extends State[T] {
    def map[U](f: T => U): State[U] = Fail[U](cause)
  }

  /** Indicates that a required result cannot be found. */
  case object NoSuchResultException
    extends java.util.NoSuchElementException()
    with scala.util.control.NoStackTrace

  private[api] val DefaultBatchSize = 101
}

/** Allows to enrich a base cursor. */
trait CursorProducer[T] {
  type ProducedCursor <: Cursor[T]

  /** Produces a custom cursor from the `base` one. */
  @deprecated("The `base` cursor will be required to also provide [[CursorOps]].", "0.11.15") // See https://github.com/ReactiveMongo/ReactiveMongo-Streaming/blob/master/akka-stream/src/main/scala/package.scala#L14
  def produce(base: Cursor[T]): ProducedCursor
}

object CursorProducer {
  private[api] type Aux[T, C[_] <: Cursor[_]] = CursorProducer[T] {
    type ProducedCursor = C[T]
  }

  implicit def defaultCursorProducer[T]: CursorProducer.Aux[T, Cursor] =
    new CursorProducer[T] {
      type ProducedCursor = Cursor[T]
      def produce(base: Cursor[T]) = base
    }
}

/**
 * Flattening strategy for cursor.
 *
 * {{{
 * trait FooCursor[T] extends Cursor[T] { def foo: String }
 *
 * implicit def fooFlattener[T] = new CursorFlattener[FooCursor] {
 *   def flatten[T](future: Future[FooCursor[T]]): FooCursor[T] =
 *     new FlattenedCursor[T](future) with FooCursor[T] {
 *       def foo = "Flattened"
 *     }
 * }
 * }}}
 */
trait CursorFlattener[C[_] <: Cursor[_]] {
  /** Flatten a future of cursor as cursor. */
  def flatten[T](future: Future[C[T]]): C[T]
}

/** Flatteners helper */
object CursorFlattener {
  implicit object defaultCursorFlattener extends CursorFlattener[Cursor] {
    def flatten[T](future: Future[Cursor[T]]): Cursor[T] =
      new FlattenedCursor[T](future)
  }
}
