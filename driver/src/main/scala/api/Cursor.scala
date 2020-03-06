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

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.util.LazyLogger

import reactivemongo.core.protocol.Response

/**
 * Cursor over results from MongoDB.
 *
 * {{{
 * import scala.concurrent.{ ExecutionContext, Future }
 *
 * import reactivemongo.api.Cursor
 * import reactivemongo.api.bson.{ BSONDocument, Macros }
 * import reactivemongo.api.bson.collection.BSONCollection
 *
 * case class User(name: String, pass: String)
 *
 * implicit val handler = Macros.reader[User]
 *
 * def findUsers(coll: BSONCollection)(
 *   implicit ec: ExecutionContext): Future[List[User]] =
 *   coll.find(BSONDocument("enabled" -> true)).
 *     cursor[User](). // obtain cursor for User results
 *     collect[List](
 *       maxDocs = 10,
 *       err = Cursor.FailOnError[List[User]]())
 * }}}
 *
 * @tparam T the type parsed from each result document
 * @define maxDocsParam the maximum number of documents to be retrieved (-1 for unlimited)
 * @define maxDocsWarning The actual document count can exceed this, when this maximum devided by the batch size given a non-zero remainder
 * @define stopOnErrorParam States if may stop on non-fatal exception (default: true). If set to false, the exceptions are skipped, trying to get the next result.
 * @define errorHandlerParam The binary operator to be applied when failing to get the next response. Exception or [[reactivemongo.api.Cursor$.Fail Fail]] raised within the `suc` function cannot be recovered by this error handler.
 * @define zeroParam the initial value
 * @define getHead Returns the first document matching the query
 * @define resultTParam the result type of the binary operator
 * @define sucRespParam The binary operator to be applied when the next response is successfully read
 * @define foldResp Applies a binary operator to a start value and all responses handled by this cursor, going first to last.
 * @define sucSafeWarning This must be safe, and any error must be returned as `Future.failed[State[A]]`
 * @define foldBulks Applies a binary operator to a start value and all bulks of documents retrieved by this cursor, going first to last.
 * @define foldWhile Applies a binary operator to a start value and all elements retrieved by this cursor, going first to last.
 * @define sucDocParam The binary operator to be applied when the next document is successfully read
 */
trait Cursor[T] extends CursorCompatAPI[T] {
  import Cursor.{ ErrorHandler, FailOnError }

  /**
   * $foldResp
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  @deprecated("Internal: will be made private", "0.19.4")
  def foldResponses[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[A]

  /**
   * $foldResp
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam. $sucSafeWarning.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  @deprecated("Internal: will be made private", "0.19.4")
  def foldResponsesM[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[A]

  /**
   * $foldBulks
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   *
   * {{{
   * import reactivemongo.api.Cursor
   * import scala.concurrent.ExecutionContext
   *
   * case class Person(name: String, age: Int)
   *
   * def foo(cursor: Cursor[Person])(implicit ec: ExecutionContext) =
   *   cursor.foldBulks(Nil: Seq[Person])(
   *     (s, bulk: Iterator[Person]) => Cursor.Cont(s ++ bulk),
   *     { (l, e) => println("last valid value: " + l); Cursor.Fail(e) })
   * }}}
   */
  def foldBulks[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[A]

  /**
   * $foldBulks
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam. $sucSafeWarning.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.Cursor
   *
   * case class Person(name: String, age: Int)
   *
   * def foo(cursor: Cursor[Person])(implicit ec: ExecutionContext) =
   *   cursor.foldBulksM(Nil: Seq[Person])(
   *     { (s, bulk: Iterator[Person]) =>
   *      Future.successful(Cursor.Cont(s ++ bulk))
   *    },
   *     { (l, e) =>
   *       println("last valid value: " + l)
   *       Cursor.Fail[Seq[Person]](e)
   *     })
   * }}}
   */
  def foldBulksM[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[A]

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
   * import reactivemongo.api.Cursor
   * import scala.concurrent.ExecutionContext
   *
   * case class Person(name: String, age: Int)
   *
   * def foo(cursor: Cursor[Person])(implicit ec: ExecutionContext) =
   *   cursor.foldWhile(Nil: Seq[Person])((s, p) => Cursor.Cont(s :+ p),
   *     { (l, e) => println("last valid value: " + l); Cursor.Fail(e) })
   * }}}
   */
  def foldWhile[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[A]

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
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.Cursor
   *
   * case class Person(name: String, age: Int)
   *
   * def foo(cursor: Cursor[Person])(implicit ec: ExecutionContext) =
   *   cursor.foldWhileM(Nil: Seq[Person])(
   *     (s, p) => Future.successful(Cursor.Cont(s :+ p)),
   *     { (l, e) =>
   *       println("last valid value: " + l)
   *       Cursor.Fail[Seq[Person]](e)
   *     })
   * }}}
   */
  def foldWhileM[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[A]

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
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.Cursor
   *
   * case class Person(name: String, age: Int)
   *
   * def foo(cursor: Cursor[Person])(implicit ec: ExecutionContext) =
   *   cursor.foldWhile(Nil: Seq[Person])((s, p) => Cursor.Cont(s :+ p),
   *     { (l, e) => println("last valid value: " + l); Cursor.Fail(e) })
   * }}}
   */
  def fold[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => A)(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[A] = foldWhile[A](z, maxDocs)(
    { (st, v) => Cursor.Cont[A](suc(st, v)) }, FailOnError[A]())

  /**
   * $getHead, or fails with [[Cursor.NoSuchResultException]] if none.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.bson.BSONDocument
   * import reactivemongo.api.collections.bson.BSONCollection
   *
   * def first(query: BSONDocument)(collection: BSONCollection)(
   *   implicit ec: ExecutionContext): Future[BSONDocument] = {
   *   val cursor = collection.find(query).cursor[BSONDocument]()
   *   // return option of the first element.
   *   cursor.head
   * }
   * }}}
   */
  def head(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[T]

  /**
   * $getHead, if any.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.bson.BSONDocument
   *
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def maybeFirst(query: BSONDocument)(collection: BSONCollection)(
   *   implicit ec: ExecutionContext): Future[Option[BSONDocument]] = {
   *   val cursor = collection.find(query).cursor[BSONDocument]()
   *   // return option of the first element.
   *   cursor.headOption
   * }
   * }}}
   */
  def headOption(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[Option[T]]
}

/** Cursor companion object */
object Cursor {
  type WithOps[T] = Cursor[T] with CursorOps[T]

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
   * Flattens the given future [[reactivemongo.api.Cursor]]
   * to a [[reactivemongo.api.FlattenedCursor]].
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.Cursor
   *
   * def flatCursor[T](cursor: Future[Cursor[T]]): Cursor[T] =
   *   Cursor.flatten(cursor)
   * }}}
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
  class Cont[T] private[api] (val value: T)
    extends State[T] with Product1[T] with Serializable {

    def map[U](f: T => U): State[U] = Cont(f(value))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = value

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Cont[T] => true
      case _          => false
    }

  }

  object Cont {
    def apply[T](value: T): Cont[T] = new Cont[T](value)

    def unapply[T](cont: Cont[T]): Option[T] = Option(cont).map(_.value)
  }

  /** Successfully stop processing with given value */
  class Done[T] private[api] (val value: T)
    extends State[T] with Product1[T] with Serializable {

    def map[U](f: T => U): State[U] = Done(f(value))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = value

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Done[T] => true
      case _          => false
    }

  }

  object Done {
    def apply[T](value: T): Done[T] = new Done[T](value)

    def unapply[T](done: Done[T]): Option[T] = Option(done).map(_.value)
  }

  /** Ends processing due to failure of given `cause` */
  class Fail[T] private[api] (val cause: Throwable)
    extends State[T] with Product1[Throwable] with Serializable {

    def map[U](f: T => U): State[U] = Fail[U](cause)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = cause

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Fail[T] => true
      case _          => false
    }
  }

  object Fail {
    def apply[T](cause: Throwable): Fail[T] = new Fail[T](cause)

    def unapply[T](fail: Fail[T]): Option[Throwable] = Option(fail).map(_.cause)
  }

  /** Indicates that a required result cannot be found. */
  case object NoSuchResultException
    extends java.util.NoSuchElementException()
    with scala.util.control.NoStackTrace

  private[api] val DefaultBatchSize = 101

  /** '''EXPERIMENTAL''' */
  final class Reference(
    val collectionName: String,
    val cursorId: Long,
    val numberToReturn: Int,
    val tailable: Boolean,
    val pinnedNode: Option[String]) {

    override def equals(that: Any): Boolean = that match {
      case null => false

      case other: Reference =>
        (this.cursorId == other.cursorId) && (
          this.tailable == other.tailable) && (
            this.numberToReturn == other.numberToReturn) && (
              (this.pinnedNode == null && other.pinnedNode == null) ||
              (this.pinnedNode != null &&
                this.pinnedNode == other.pinnedNode)) && (
                  (this.collectionName == null &&
                    other.collectionName == null) ||
                    (this.collectionName != null &&
                      this.collectionName == other.collectionName))

      case _ => false
    }

    override def hashCode: Int = {
      import scala.util.hashing.MurmurHash3

      val nh1 = MurmurHash3.mix(MurmurHash3.productSeed, cursorId.##)
      val nh2 = MurmurHash3.mix(nh1, numberToReturn)
      val nh3 = MurmurHash3.mix(nh2, tailable.##)
      val nh4 = MurmurHash3.mix(nh3, collectionName.##)

      MurmurHash3.mixLast(nh4, pinnedNode.##)
    }

    override def toString: String = s"""Result(collection = $collectionName, cursorId = $cursorId, numberToReturn = $numberToReturn, tailable = $tailable, pinnedNode = ${pinnedNode.getOrElse("<none>")})"""
  }

  /** '''EXPERIMENTAL''' */
  final class Result[T](
    val value: T,
    val reference: Reference) {

    override def equals(that: Any): Boolean = that match {
      case null => false

      case other: Result[_] =>
        ((this.value == null && other.value == null) || (
          this.value != null && this.value == other.value)) && ((
            this.reference == null && other.reference == null) || (
              this.reference != null && this.reference == other.reference))

      case _ => false
    }

    override def hashCode: Int = {
      import scala.util.hashing.MurmurHash3

      val nh1 = MurmurHash3.mix(MurmurHash3.productSeed, value.##)
      MurmurHash3.mixLast(nh1, reference.##)
    }

    override def toString: String = s"Result($value, $reference)"
  }

  /** '''EXPERIMENTAL''' */
  object Result {
    def unapply[T](other: Result[T]): Option[(T, Reference)] =
      Option(other).map { r => r.value -> r.reference }
  }
}
