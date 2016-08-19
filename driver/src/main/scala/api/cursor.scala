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

import scala.util.{ Failure, Success, Try }
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder
import scala.concurrent.{ ExecutionContext, Future }

import play.api.libs.iteratee.{ Enumerator, Enumeratee, Error, Input, Iteratee }

import reactivemongo.core.actors.Exceptions
import reactivemongo.core.iteratees.{ CustomEnumeratee, CustomEnumerator }
import reactivemongo.core.netty.BufferSequence
import reactivemongo.core.protocol.{
  GetMore,
  KillCursors,
  Query,
  QueryFlags,
  RequestMaker,
  Response,
  ReplyDocumentIterator,
  ReplyDocumentIteratorExhaustedException
}
import reactivemongo.util.{
  ExtendedFutures,
  LazyLogger
}, ExtendedFutures.DelayedFuture

/**
 * Cursor over results from MongoDB.
 *
 * @tparam T the type parsed from each result document
 * @define maxDocsParam The maximum number of documents to be retrieved
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
  import Cursor.{ ContOnError, ErrorHandler, FailOnError }

  /**
   * Produces an Enumerator of documents.
   * The returned enumerator may process up to `maxDocs`.
   * If `stopOnError` is false, then documents that cause error are dropped,
   * so the enumerator may emit a little less than `maxDocs`,
   * even if it processes `maxDocs` documents.
   *
   * @param maxDocs $maxDocsParam.
   * @param stopOnError $stopOnErrorParam.
   * @return an [[play.api.libs.iteratee.Enumerator]] of documents
   */
  @deprecated(message = "Use `.enumerator` from Play Iteratees module", since = "0.11.10")
  def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[T]

  /**
   * Produces an Enumerator of Iterator of documents.
   * Given the `stopOnError` parameter, this Enumerator may stop on any non-fatal exception, or skip and continue.
   *
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param stopOnError $stopOnErrorParam.
   * @return an [[play.api.libs.iteratee.Enumerator]] of Iterators of documents
   */
  @deprecated(message = "Use `.bulkEnumerator` from the Play Iteratees module", since = "0.11.10")
  def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[T]]

  /**
   * Produces an Enumerator of responses from the database.
   * Given the `stopOnError` parameter, this Enumerator may stop on any non-fatal exception, or skip and continue.
   *
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param stopOnError $stopOnErrorParam.
   * @return an [[play.api.libs.iteratee.Enumerator]] of Responses.
   */
  @deprecated(message = "Use `.responseEnumerator` from the Play Iteratees module", since = "0.11.10")
  def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response]

  /**
   * $collect.
   *
   * @param maxDocs $maxDocsParam.
   * @param err $errorHandlerParam
   *
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]
   * // return the 3 first documents in a Vector[BSONDocument].
   * val vector = cursor.collect[Vector](3, Cursor.FailOnError())
   * }}}
   */
  def collect[M[_]](maxDocs: Int, err: ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]]

  /**
   * $collect.
   *
   * @param maxDocs $maxDocsParam.
   * @param stopOnError $stopOnErrorParam.
   *
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]
   * // return the 3 first documents in a Vector[BSONDocument].
   * val vector = cursor.collect[Vector](3)
   * }}}
   */
  @deprecated(message = "Use `collect` with an [[Cursor.ErrorHandler]].", since = "0.12-RC1")
  def collect[M[_]](maxDocs: Int = Int.MaxValue, stopOnError: Boolean = true)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = {
    val err = if (stopOnError) FailOnError[M[T]]() else ContOnError[M[T]]()
    collect[M](maxDocs, err)
  }

  /**
   * $foldResp
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldResponses[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldResp
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam. $sucSafeWarning.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldResponsesM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldBulks
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldBulks[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

  /**
   * $foldBulks
   *
   * @param z $zeroParam
   * @param maxDocs $maxDocsParam. $maxDocsWarning.
   * @param suc $sucRespParam. $sucSafeWarning.
   * @param err $errorHandlerParam
   * @tparam A $resultTParam
   */
  def foldBulksM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

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
  def foldWhile[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Cursor.State[A], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

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
  def foldWhileM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Future[Cursor.State[A]], err: ErrorHandler[A] = FailOnError[A]())(implicit ctx: ExecutionContext): Future[A]

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
  def fold[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => A)(implicit ctx: ExecutionContext): Future[A] = foldWhile[A](z, maxDocs)(
    { (st, v) => Cursor.Cont[A](suc(st, v)) }, FailOnError[A]()
  )

  /** Returns the list of the matching documents. */
  @deprecated("consider using collect[List] instead", "0.10.0")
  def toList(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = true)(implicit ctx: ExecutionContext): Future[List[T]] = collect[List](maxDocs, stopOnError)

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
  def headOption(implicit ctx: ExecutionContext): Future[Option[T]] =
    Future.successful(Option.empty[T]) // TODO: Remove body

  /**
   * Produces an Enumerator of responses from the database.
   * An Enumeratee for error handling should be used to prevent silent failures.
   *
   * @param maxDocs $maxDocsParam
   */
  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response]
}

/** Internal cursor operations. */
sealed trait CursorOps[T] { cursor: Cursor[T] =>
  /** Sends the initial request. */
  private[reactivemongo] def makeRequest(maxDocs: Int)(implicit ctx: ExecutionContext): Future[Response]

  private[reactivemongo] def nextResponse(maxDocs: Int): (ExecutionContext, Response) => Future[Option[Response]]

  /**
   * Returns an iterator to read the response documents,
   * according the provided read for the element type `T`.
   */
  private[reactivemongo] def documentIterator(response: Response): Iterator[T]

  /**
   * Kills the server resources associated with the specified cursor.
   *
   * @param cursorID the cursor ID
   */
  def kill(cursorID: Long): Unit

  /** Indicates whether the underlying cursor is taible. */
  def tailable: Boolean
}

object CursorOps {
  /**
   * Wraps exception that has already been passed to the current error handler
   * and should not be recovered.
   */
  private[reactivemongo] case class Unrecoverable(cause: Throwable)
    extends scala.RuntimeException(cause)
    with scala.util.control.NoStackTrace

}

class FlattenedCursor[T](cursor: Future[Cursor[T]]) extends Cursor[T] {
  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[T] =
    Enumerator.flatten(cursor.map(_.enumerate(maxDocs, stopOnError)))

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[T]] =
    Enumerator.flatten(cursor.map(_.enumerateBulks(maxDocs, stopOnError)))

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response] =
    Enumerator.flatten(cursor.map(_.enumerateResponses(maxDocs, stopOnError)))

  def head(implicit ctx: ExecutionContext): Future[T] = cursor.flatMap(_.head)

  // TODO: Remove override
  override def headOption(implicit ctx: ExecutionContext): Future[Option[T]] =
    cursor.flatMap(_.headOption)

  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = cursor.flatMap(_.collect[M](maxDocs, err))

  def foldResponses[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldResponses(z, maxDocs)(suc, err))

  def foldResponsesM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldResponsesM(z, maxDocs)(suc, err))

  def foldBulks[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulks(z, maxDocs)(suc, err))

  def foldBulksM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulksM(z, maxDocs)(suc, err))

  def foldWhile[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhile(z, maxDocs)(suc, err))

  def foldWhileM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhileM(z, maxDocs)(suc, err))

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
    Enumerator.flatten(cursor.map(_.rawEnumerateResponses(maxDocs)))

}

/**
 * Cursor wrapper, to help to define custom cursor classes.
 * @see CursorProducer
 */
trait WrappedCursor[T] extends Cursor[T] {
  /** The underlying cursor */
  def wrappee: Cursor[T]

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[T] =
    wrappee.enumerate(maxDocs, stopOnError)

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[T]] =
    wrappee.enumerateBulks(maxDocs, stopOnError)

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response] =
    wrappee.enumerateResponses(maxDocs, stopOnError)

  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = wrappee.collect[M](maxDocs, err)

  // TODO: Remove
  override def collect[M[_]](maxDocs: Int = Int.MaxValue, stopOnError: Boolean = true)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = wrappee.collect[M](maxDocs, stopOnError)

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] = wrappee.rawEnumerateResponses(maxDocs)

  def foldResponses[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldResponses(z, maxDocs)(suc, err)

  def foldResponsesM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldResponsesM(z, maxDocs)(suc, err)

  def foldBulks[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldBulks(z, maxDocs)(suc, err)

  def foldBulksM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldBulksM(z, maxDocs)(suc, err)

  def foldWhile[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldWhile(z, maxDocs)(suc, err)

  def foldWhileM[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldWhileM(z, maxDocs)(suc, err)

  def head(implicit ctx: ExecutionContext): Future[T] = wrappee.head

  // TODO: Remove override
  override def headOption(implicit ctx: ExecutionContext): Future[Option[T]] =
    wrappee.headOption
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
}

object DefaultCursor {
  import Cursor.{ ErrorHandler, State, Cont, Done, Fail, logger }
  import reactivemongo.api.commands.ResultCursor
  import CursorOps.Unrecoverable

  @deprecated(message = "Use [[query]]", since = "0.11.10")
  def apply[P <: SerializationPack, A](
    pack: P,
    query: Query,
    documents: BufferSequence,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean
  )(implicit reader: pack.Reader[A]): Cursor[A] =
    DefaultCursor.query[P, A](pack, query, documents, readPreference,
      mongoConnection, failover, isMongo26WriteOp)

  def query[P <: SerializationPack, A](
    pack: P,
    query: Query,
    requestBuffer: BufferSequence,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean
  )(implicit reader: pack.Reader[A]) = new Impl[A] {

    val preference = readPreference
    val connection = mongoConnection
    val failoverStrategy = failover
    val mongo26WriteOp = isMongo26WriteOp
    val fullCollectionName = query.fullCollectionName
    val numberToReturn = query.numberToReturn
    val tailable = (query.flags &
      QueryFlags.TailableCursor) == QueryFlags.TailableCursor

    val makeIterator = { response: Response =>
      ReplyDocumentIterator(pack)(response.reply, response.documents)(reader)
    }

    @inline def makeRequest(maxDocs: Int)(implicit ctx: ExecutionContext): Future[Response] = Failover2(mongoConnection, failoverStrategy) { () =>
      val nrt = query.numberToReturn
      val q = { // normalize the number of docs to return
        if (nrt > 0 && nrt <= maxDocs) query
        else query.copy(numberToReturn = maxDocs)
      }

      mongoConnection.sendExpectingResponse(
        RequestMaker(q, requestBuffer, readPreference), isMongo26WriteOp
      )
    }.future
  }

  /**
   * @param channelId the ID of the current channel
   * @param toReturn the number to return
   */
  def getMore[P <: SerializationPack, A](
    pack: P,
    preload: => Response,
    result: ResultCursor,
    toReturn: Int,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean
  )(implicit reader: pack.Reader[A]) = new Impl[A] {
    val preference = readPreference
    val connection = mongoConnection
    val failoverStrategy = failover
    val mongo26WriteOp = isMongo26WriteOp
    val fullCollectionName = result.fullCollectionName
    val numberToReturn = toReturn
    val tailable = false // ??

    private lazy val response = preload

    val makeIterator = { response: Response =>
      ReplyDocumentIterator(pack)(response.reply, response.documents)(reader)
    }

    private var req: Int => ExecutionContext => Future[Response] = { maxDocs =>
      { implicit ec =>
        this.req = makeReq
        Future.successful(response)
      }
    }

    private val op = GetMore(fullCollectionName, toReturn, result.cursorId)
    private lazy val reqMaker =
      RequestMaker(op).copy(channelIdHint = Some(response.info.channelId))

    private val makeReq = { maxDocs: Int =>
      { implicit ctx: ExecutionContext =>
        logger.trace(s"[Cursor] Calling next on ${result.cursorId}, op=$op")

        Failover2(connection, failoverStrategy) { () =>
          connection.sendExpectingResponse(reqMaker, mongo26WriteOp)
        }.future
      }
    }

    def makeRequest(maxDocs: Int)(implicit ctx: ExecutionContext): Future[Response] = req(maxDocs)(ctx)
  }

  private[reactivemongo] trait Impl[A] extends Cursor[A] with CursorOps[A] {
    /** The read preference */
    def preference: ReadPreference

    def connection: MongoConnection

    def failoverStrategy: FailoverStrategy

    def mongo26WriteOp: Boolean

    def fullCollectionName: String

    def numberToReturn: Int

    def tailable: Boolean

    def makeIterator: Response => Iterator[A]

    def documentIterator(response: Response): Iterator[A] =
      makeIterator(response)

    private def next(response: Response, maxDocs: Int)(implicit ctx: ExecutionContext): Future[Option[Response]] = {
      if (response.reply.cursorID != 0) {
        val toReturn =
          if (numberToReturn > 0) numberToReturn
          else maxDocs - (
            response.reply.numberReturned + response.reply.startingFrom
          )

        val op = GetMore(fullCollectionName, toReturn, response.reply.cursorID)

        logger.trace(s"[Cursor] Calling next on ${response.reply.cursorID}, op=$op")

        Failover2(connection, failoverStrategy) { () =>
          connection.sendExpectingResponse(
            RequestMaker(
              op,
              readPreference = preference,
              channelIdHint = Some(response.info.channelId)
            ),
            mongo26WriteOp
          )

        }.future.map(Some(_))
      } else {
        logger.error("[Cursor] Call to next() but cursorID is 0, there is probably a bug")
        Future.successful(Option.empty[Response])
      }
    }

    @inline
    private def hasNext(response: Response, maxDocs: Int): Boolean =
      (response.reply.cursorID != 0) && (
        response.reply.numberReturned + response.reply.startingFrom
      ) < maxDocs

    /** Returns next response using tailable mode */
    private def tailResponse(current: Response, maxDocs: Int)(implicit context: ExecutionContext): Future[Option[Response]] = {
      {
        @inline def closed = Future.successful {
          logger.warn("[tailResponse] Connection is closed")
          Option.empty[Response]
        }

        if (connection.killed) closed
        else if (hasNext(current, maxDocs)) {
          next(current, maxDocs).recoverWith {
            case _: Exceptions.ClosedException => closed
            case err =>
              Future.failed[Option[Response]](err)
          }
        } else {
          logger.debug("[tailResponse] Current cursor exhausted, renewing...")
          DelayedFuture(500, connection.actorSystem).
            flatMap { _ => makeRequest(maxDocs).map(Some(_)) }
        }
      }
    }

    def kill(cursorID: Long): Unit = killCursors(cursorID, "Cursor")

    private def killCursors(cursorID: Long, logCat: String): Unit = {
      if (cursorID != 0) {
        logger.debug(s"[$logCat] Clean up ${cursorID}, sending KillCursors")

        val killReq = RequestMaker(
          KillCursors(Set(cursorID)),
          readPreference = preference
        )

        connection.send(killReq)
      } else logger.trace(s"[$logCat] Cursor exhausted (${cursorID})")
    }

    def head(implicit ctx: ExecutionContext): Future[A] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.failed[A](Cursor.NoSuchResultException)
        } else Future(result.next())
      }

    // TODO: Remove override
    override def headOption(implicit ctx: ExecutionContext): Future[Option[A]] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.successful(Option.empty[A])
        } else Future(result.next()).map(Some(_))
      }

    @inline private def syncSuccess[A, B](f: (A, B) => State[A])(implicit ec: ExecutionContext): (A, B) => Future[State[A]] = { (a: A, b: B) => Future(f(a, b)) }

    def foldResponses[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, Response) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = new FoldResponses(z, maxDocs, syncSuccess(suc), err)(ctx)()

    def foldResponsesM[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, Response) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = new FoldResponses(z, maxDocs, suc, err)(ctx)()

    def foldBulks[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, Iterator[A]) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = foldBulksM[T](z, maxDocs)(syncSuccess[T, Iterator[A]](suc), err)

    def foldBulksM[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, Iterator[A]) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = foldResponsesM(z, maxDocs)({ (s, r) =>
      Try(makeIterator(r)) match {
        case Success(it) => suc(s, it)
        case Failure(e)  => Future.successful[State[T]](Fail(e))
      }
    }, err)

    def foldWhile[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, A) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = foldWhileM[T](z, maxDocs)(syncSuccess[T, A](suc), err)

    def foldWhileM[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, A) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = {
      def go(v: T, it: Iterator[A]): Future[State[T]] =
        if (!it.hasNext) Future.successful(Cont(v))
        else Try(it.next) match {
          case Failure(
            x @ ReplyDocumentIteratorExhaustedException(_)) =>
            Future.successful(Fail(x))

          case Failure(e) => err(v, e) match {
            case Cont(cv) => go(cv, it)
            case f @ Fail(Unrecoverable(_)) =>
              /* already marked unrecoverable */ Future.successful(f)
            case Fail(u) => Future.successful(Fail(Unrecoverable(u)))
            case st      => Future.successful(st)
          }

          case Success(a) => suc(v, a).flatMap {
            case Cont(cv)    => go(cv, it)
            case f @ Fail(_) => Future.successful(f)
            case st          => Future.successful(st)
          }
        }

      foldBulksM(z, maxDocs)(go, err)
    }

    @deprecated(message = "Only for internal use", since = "0.11.10")
    def simpleCursorEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
      Enumerator.flatten(makeRequest(maxDocs).
        map(new CustomEnumerator.SEnumerator(_)(
          next = response => {
          if (hasNext(response, maxDocs)) next(response, maxDocs)
          else Future.successful(Option.empty[Response])
        }, cleanUp = { resp => killCursors(resp.reply.cursorID, "Cursor") }
        )))

    @deprecated(message = "Only for internal use", since = "0.11.10")
    def tailableCursorEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
      Enumerator.flatten(makeRequest(maxDocs).map { response =>
        new CustomEnumerator.SEnumerator(response -> 0)(
          next = { current =>
          val (r, c) = current
          if (c < maxDocs) {
            tailResponse(r, maxDocs).
              map(_.map((_, c + r.reply.numberReturned)))

          } else Future.successful(Option.empty[(Response, Int)])
        },
          cleanUp = { current =>
          val (r, _) = current
          killCursors(r.reply.cursorID, "Tailable Cursor")
        }
        ).map(_._1)
      })

    @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
    def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
      if (tailable) tailableCursorEnumerateResponses(maxDocs)
      else simpleCursorEnumerateResponses(maxDocs)

    @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
    def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response] =
      rawEnumerateResponses(maxDocs) &> {
        if (stopOnError)
          CustomEnumeratee.stopOnError
        else CustomEnumeratee.recover {
          new CustomEnumeratee.RecoverFromErrorFunction {
            def apply[E, A](throwable: Throwable, input: Input[E], continue: () => Iteratee[E, A]): Iteratee[E, A] = throwable match {
              case e: ReplyDocumentIteratorExhaustedException => {
                val errstr = "ReplyDocumentIterator exhausted! Was this enumerator applied to many iteratees concurrently? Stopping to prevent infinite recovery."
                logger.error(errstr, e)
                Error(errstr, input)
              }

              case e =>
                logger.debug("There was an exception during the stream, dropping it since stopOnError is false", e)
                continue()
            }
          }
        }
      }

    @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
    def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[A]] =
      enumerateResponses(maxDocs, stopOnError) &> Enumeratee.map(makeIterator)

    @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
    def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[A] = {
      @annotation.tailrec
      def _next(it: Iterator[A], stopOnError: Boolean): Option[Try[A]] = {
        if (it.hasNext) {
          val tried = Try(it.next)

          if (tried.isFailure && !stopOnError) _next(it, stopOnError)
          else Some(tried)
        } else None
      }

      enumerateResponses(maxDocs, stopOnError) &>
        Enumeratee.mapFlatten { response =>
          val iterator = makeIterator(response)

          if (!iterator.hasNext) Enumerator.empty
          else CustomEnumerator.SEnumerator(iterator.next) { _ =>
            _next(iterator, stopOnError).
              fold(Future.successful(Option.empty[A])) {
                case Success(mt) => Future.successful(Some(mt))
                case Failure(e)  => Future.failed(e)
              }
          }
        }
    }

    def collect[M[_]](maxDocs: Int, err: ErrorHandler[M[A]])(implicit cbf: CanBuildFrom[M[_], A, M[A]], ec: ExecutionContext): Future[M[A]] =
      foldWhile[Builder[A, M[A]]](cbf(), maxDocs)(
        { (builder, a) => Cont(builder += a) },
        { (b: Builder[A, M[A]], t: Throwable) =>
          err(b.result(), t).map[Builder[A, M[A]]](_ => b)
        }
      ).map(_.result())

    def nextResponse(maxDocs: Int): (ExecutionContext, Response) => Future[Option[Response]] = {
      if (!tailable) { (ec: ExecutionContext, r: Response) =>
        if (!hasNext(r, maxDocs)) Future.successful(Option.empty[Response])
        else next(r, maxDocs)(ec)
      } else { (ec: ExecutionContext, r: Response) =>
        tailResponse(r, maxDocs)(ec)
      }
    }

    private class FoldResponses[T](
        z: => T, maxDocs: Int,
        suc: (T, Response) => Future[State[T]],
        err: ErrorHandler[T]
    )(implicit ctx: ExecutionContext) {

      private val nextResp: Response => Future[Option[Response]] =
        nextResponse(maxDocs)(ctx, _: Response)

      @inline def ok(r: Response, v: T) = {
        // Releases cursor before ending
        killCursors(r.reply.cursorID, "FoldResponses")
        Future.successful(v)
      }

      @inline def kill(r: Response, f: Throwable) = {
        killCursors(r.reply.cursorID, "FoldResponses")
        Future.failed[T](f)
      }

      def procResp(resp: Response, cur: T, c: Int): Future[T] = {
        logger.trace(s"Process response: $resp")

        suc(cur, resp).transform(resp -> _, { error =>
          killCursors(resp.reply.cursorID, "FoldResponses")
          error
        }).flatMap {
          case (r, next) =>
            val nc = c + resp.reply.numberReturned

            next match {
              case Done(d)                    => ok(r, d)
              case Cont(v) if (nc >= maxDocs) => ok(r, v)
              case Fail(u @ Unrecoverable(_)) =>
                /* already marked recoverable */ kill(r, u)
              case Fail(f) => kill(r, Unrecoverable(f))
              case Cont(v) =>
                nextResp(r).flatMap(_.fold(Future successful v) { x =>
                  procResponses(Future.successful(x), v, nc)
                })
            }
        }.recoverWith {
          case Unrecoverable(e) => Future.failed(e)
          case e =>
            val nc = c + 1 // resp.reply.numberReturned

            err(cur, e) match {
              case Done(d)                    => Future.successful(d)
              case Cont(v) if (nc >= maxDocs) => Future.successful(v)
              case Fail(f)                    => Future.failed[T](f)
              case Cont(v) =>
                procResp(resp, cur, nc) // retry
            }
        }
      }

      def procResponses(done: Future[Response], cur: T, c: Int): Future[T] =
        done.map[Try[Response]](Success(_)).
          recover { case err => Failure(err) }.flatMap {
            case Success(r) => procResp(r, cur, c)
            case Failure(error) => {
              logger.error("fails to send request", error)

              err(cur, error) match {
                case Done(v) => Future.successful(v)
                case Fail(e) => Future.failed(e)
                case Cont(v) => {
                  logger.warn(
                    "cannot continue after fatal request error", error
                  )

                  Future.successful(v)
                }
              }
            }
          }

      def apply(): Future[T] =
        Future(z).flatMap(v => procResponses(makeRequest(maxDocs), v, 0))
    }
  }
}

/** Allows to enrich a base cursor. */
trait CursorProducer[T] {
  type ProducedCursor <: Cursor[T]

  /** Produces a custom cursor from the `base` one. */
  @deprecated("The `base` cursor will require [[CursorOps]].", "0.11.15")
  def produce(base: Cursor[T]): ProducedCursor
}

object CursorProducer {
  implicit def defaultCursorProducer[T]: CursorProducer[T] =
    new CursorProducer[T] {
      type ProducedCursor = Cursor[T]
      def produce(base: Cursor[T]) = base
    }
}

/** Flattening strategy for cursor. */
trait CursorFlattener[C[_] <: Cursor[_]] {
  /** Flatten a future of cursor as cursor. */
  def flatten[T](future: Future[C[T]]): C[T]
}

/** Flatteners helper */
object CursorFlattener {
  implicit object defaultCursorFlattener extends CursorFlattener[Cursor] {
    def flatten[T](future: Future[Cursor[T]]): Cursor[T] =
      new FlattenedCursor(future)
  }
}
