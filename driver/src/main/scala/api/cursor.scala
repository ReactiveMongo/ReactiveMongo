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

import scala.util.{ Failure, Success, Try }

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

import scala.concurrent.{ ExecutionContext, Future }

import akka.actor.ActorSystem

import reactivemongo.bson.BSONDocument

import reactivemongo.core.actors.{
  Exceptions,
  RequestMakerExpectingResponse
}
import reactivemongo.core.netty.BufferSequence
import reactivemongo.core.protocol.{
  GetMore,
  KillCursors,
  MongoWireVersion,
  Query,
  QueryFlags,
  RequestMaker,
  RequestOp,
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

/** Internal cursor operations. */
sealed trait CursorOps[T] { cursor: Cursor[T] =>
  /** Sends the initial request. */
  private[reactivemongo] def makeRequest(maxDocs: Int)(implicit ctx: ExecutionContext): Future[Response]

  /**
   * Returns a function that can be used to get the next response,
   * if allowed according the `maxDocs` and the cursor options
   * (cursor not exhausted, tailable, ...)
   */
  private[reactivemongo] def nextResponse(maxDocs: Int): (ExecutionContext, Response) => Future[Option[Response]]

  /**
   * Returns an iterator to read the response documents,
   * according the provided read for the element type `T`.
   */
  private[reactivemongo] def documentIterator(response: Response): Iterator[T]

  @deprecated("Use [[killCursor]]", "0.16.0")
  def kill(cursorID: Long): Unit

  /**
   * Kills the server resources associated with the specified cursor.
   *
   * @param id the cursor ID
   */
  private[reactivemongo] def killCursor(id: Long)(implicit ctx: ExecutionContext): Unit

  /** Indicates whether the underlying cursor is taible. */
  def tailable: Boolean

  /** Returns the underlying connection. */
  def connection: MongoConnection

  /** Returns the strategy to failover the cursor operations. */
  def failoverStrategy: FailoverStrategy
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

object DefaultCursor {
  import Cursor.{ ErrorHandler, State, Cont, Fail, logger }
  import reactivemongo.api.commands.ResultCursor
  import CursorOps.Unrecoverable

  /**
   * @param collectionName the fully qualified collection name (even if `query.fullCollectionName` is `\$cmd`)
   */
  def query[P <: SerializationPack, A](
    pack: P,
    query: Query,
    requestBuffer: Int => BufferSequence,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean,
    collectionName: String)(implicit reader: pack.Reader[A]): Impl[A] =
    new Impl[A] {
      val preference = readPreference
      val connection = mongoConnection
      val failoverStrategy = failover
      val mongo26WriteOp = isMongo26WriteOp
      val fullCollectionName = collectionName

      val numberToReturn = prepareNumberToReturn(mongoConnection)(query)
      val tailable = (query.flags &
        QueryFlags.TailableCursor) == QueryFlags.TailableCursor

      val makeIterator = ReplyDocumentIterator.parse(pack)(_: Response)(reader)

      @inline def makeRequest(maxDocs: Int)(implicit ctx: ExecutionContext): Future[Response] = Failover2(mongoConnection, failoverStrategy) { () =>
        val ntr = toReturn(numberToReturn, maxDocs, 0)

        // MongoDB2.6: Int.MaxValue

        val op = query.copy(numberToReturn = ntr)
        val req = RequestMakerExpectingResponse(
          RequestMaker(op, requestBuffer(maxDocs), readPreference),
          isMongo26WriteOp)

        requester(0, maxDocs, req)(ctx)
      }.future.flatMap {
        case Response.CommandError(_, _, _, cause) =>
          Future.failed[Response](cause)

        case response =>
          Future.successful(response)
      }

      // TODO: maxTimeMS
      val getMoreOpCmd: Function2[Long, Int, (RequestOp, BufferSequence)] = {
        if (lessThenV32) { (cursorId, ntr) =>
          GetMore(fullCollectionName, ntr, cursorId) -> BufferSequence.empty
        } else {
          val moreQry = query.copy(numberToSkip = 0, numberToReturn = 1)
          val collName = fullCollectionName.span(_ != '.')._2.tail

          { (cursorId, ntr) =>
            val cmd = BSONDocument(
              "getMore" -> cursorId,
              "collection" -> collName,
              "batchSize" -> ntr) // TODO: maxTimeMS

            moreQry -> BufferSequence.single(cmd)
          }
        }
      }
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
    isMongo26WriteOp: Boolean)(implicit reader: pack.Reader[A]): Impl[A] =
    new Impl[A] {
      val preference = readPreference
      val connection = mongoConnection
      val failoverStrategy = failover
      val mongo26WriteOp = isMongo26WriteOp
      val fullCollectionName = result.fullCollectionName
      val numberToReturn = toReturn
      val tailable = false // ??

      private lazy val response = preload

      val makeIterator = ReplyDocumentIterator.parse(pack)(_: Response)(reader)

      private var req: Int => ExecutionContext => Future[Response] = { maxDocs =>
        { implicit ec =>
          this.req = makeReq
          Future.successful(response)
        }
      }

      // TODO: maxTimeMS
      val getMoreOpCmd: Function2[Long, Int, (RequestOp, BufferSequence)] = {
        if (lessThenV32) { (cursorId, ntr) =>
          GetMore(fullCollectionName, ntr, cursorId) -> BufferSequence.empty
        } else {
          val collName = fullCollectionName.span(_ != '.')._2.tail

          { (cursorId, ntr) =>
            val cmd = BSONDocument(
              "getMore" -> cursorId,
              "collection" -> collName,
              "batchSize" -> ntr) // TODO: maxTimeMS

            GetMore(
              fullCollectionName, 1, cursorId) -> BufferSequence.single(cmd)
          }
        }
      }

      private val makeReq = { maxDocs: Int =>
        { implicit ctx: ExecutionContext =>
          Failover2(connection, failoverStrategy) { () =>
            val (op, cmd) = getMoreOpCmd(result.cursorId, toReturn)

            logger.trace(s"Calling next on #${result.cursorId}, op=$op")

            val req = RequestMakerExpectingResponse(
              RequestMaker(op, cmd).
                copy(channelIdHint = Some(response.info._channelId)),
              isMongo26WriteOp)

            requester(response.reply.startingFrom, maxDocs, req)(ctx)
          }.future
        }
      }

      def makeRequest(maxDocs: Int)(implicit ctx: ExecutionContext): Future[Response] = req(maxDocs)(ctx)
    }

  private[api] def prepareNumberToReturn(mongoConnection: MongoConnection): Query => Int = {
    val version = mongoConnection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    if (version.compareTo(MongoWireVersion.V32) < 0) { q: Query =>
      // see QueryOpts.batchSizeN

      /*if (q.numberToReturn == Int.MaxValue) {
       q.numberToReturn min Cursor.DefaultBatchSize*/

      if (q.numberToReturn <= 0) {
        Cursor.DefaultBatchSize
      } else q.numberToReturn
    } else {
      (_: Query) => 1 // nested 'cursor' document
    }
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

    def makeIterator: Response => Iterator[A] // Unsafe

    final def documentIterator(response: Response): Iterator[A] =
      makeIterator(response)

    protected final lazy val version = connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    @inline protected def lessThenV32: Boolean =
      version.compareTo(MongoWireVersion.V32) < 0

    protected lazy val requester: (Int, Int, RequestMakerExpectingResponse) => ExecutionContext => Future[Response] = {
      @inline def base(req: RequestMakerExpectingResponse): Future[Response] =
        connection.sendExpectingResponse(req)

      if (lessThenV32) {
        { (off: Int, maxDocs: Int, req: RequestMakerExpectingResponse) =>
          val max = if (maxDocs > 0) maxDocs else Int.MaxValue

          { implicit ec: ExecutionContext =>
            base(req).map { response =>
              val fetched = // See nextBatchOffset
                response.reply.numberReturned + response.reply.startingFrom

              if (fetched < max) {
                response
              } else response match {
                case error @ Response.CommandError(_, _, _, _) => error

                case r => {
                  // Normalizes as MongoDB 2.x doesn't offer the 'limit'
                  // on query, which allows with MongoDB 3 to exhaust
                  // the cursor with a last partial batch

                  r.cursorID(0L)
                }
              }
            }
          }
        }
      } else { (startingFrom: Int, _: Int, req: RequestMakerExpectingResponse) =>
        { implicit ec: ExecutionContext =>
          base(req).map {
            // Normalizes as 'new' cursor doesn't indicate such property
            _.startingFrom(startingFrom)
          }
        }
      }
    }

    // cursorId: Long, toReturn: Int
    protected def getMoreOpCmd: Function2[Long, Int, (RequestOp, BufferSequence)]

    private def next(response: Response, maxDocs: Int)(implicit ctx: ExecutionContext): Future[Option[Response]] = {
      if (response.reply.cursorID != 0) {
        // numberToReturn=1 for new find command,
        // so rather use batchSize from the previous reply
        val reply = response.reply
        val nextOffset = nextBatchOffset(response)
        val ntr = toReturn(reply.numberReturned, maxDocs, nextOffset)

        val (op, cmd) = getMoreOpCmd(reply.cursorID, ntr)

        logger.trace(s"Asking for the next batch of $ntr documents on cursor #${reply.cursorID}, after ${nextOffset}: $op")

        def req = RequestMakerExpectingResponse(
          RequestMaker(op, cmd,
            readPreference = preference,
            channelIdHint = Some(response.info._channelId)),
          mongo26WriteOp)

        Failover2(connection, failoverStrategy) { () =>
          requester(nextOffset, maxDocs, req)(ctx)
        }.future.map(Some(_))
      } else {
        logger.warn("Call to next() but cursorID is 0, there is probably a bug")
        Future.successful(Option.empty[Response])
      }
    }

    @inline private def hasNext(response: Response, maxDocs: Int): Boolean =
      (response.reply.cursorID != 0) && (
        maxDocs < 0 || (nextBatchOffset(response) < maxDocs))

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

    def kill(cursorID: Long): Unit = // DEPRECATED
      killCursor(cursorID)(connection.actorSystem.dispatcher)

    def killCursor(id: Long)(implicit ec: ExecutionContext): Unit =
      killCursors(id, "Cursor")

    private def killCursors(
      cursorID: Long,
      logCat: String)(implicit ec: ExecutionContext): Unit = {
      if (cursorID != 0) {
        logger.debug(s"[$logCat] Clean up $cursorID, sending KillCursors")

        val killReq = RequestMakerExpectingResponse(
          RequestMaker(KillCursors(Set(cursorID)), readPreference = preference),
          false)

        connection.sendExpectingResponse(killReq).onComplete {
          case Failure(cause) => logger.warn(
            s"[$logCat] Fails to kill cursor #${cursorID}", cause)

          case _ => ()
        }
      } else logger.trace(s"[$logCat] Nothing to release: cursor already exhausted ($cursorID)")
    }

    def head(implicit ctx: ExecutionContext): Future[A] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.failed[A](Cursor.NoSuchResultException)
        } else Future(result.next())
      }

    final def headOption(implicit ctx: ExecutionContext): Future[Option[A]] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.successful(Option.empty[A])
        } else {
          Future(Some(result.next()))
        }
      }

    @inline private def syncSuccess[T, U](f: (T, U) => State[T])(implicit ec: ExecutionContext): (T, U) => Future[State[T]] = { (a: T, b: U) => Future(f(a, b)) }

    def foldResponses[T](z: => T, maxDocs: Int = -1)(suc: (T, Response) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = FoldResponses(z, makeRequest(maxDocs)(_: ExecutionContext),
      nextResponse(maxDocs), killCursors _, syncSuccess(suc), err, maxDocs)(
        connection.actorSystem, ctx)

    def foldResponsesM[T](z: => T, maxDocs: Int = -1)(suc: (T, Response) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = FoldResponses(z, makeRequest(maxDocs)(_: ExecutionContext),
      nextResponse(maxDocs), killCursors _, suc, err, maxDocs)(
        connection.actorSystem, ctx)

    def foldBulks[T](z: => T, maxDocs: Int = -1)(suc: (T, Iterator[A]) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = foldBulksM[T](z, maxDocs)(syncSuccess[T, Iterator[A]](suc), err)

    def foldBulksM[T](z: => T, maxDocs: Int = -1)(suc: (T, Iterator[A]) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = foldResponsesM(z, maxDocs)({ (s, r) =>
      Try(makeIterator(r)) match {
        case Success(it) => suc(s, it)
        case Failure(e)  => Future.successful[State[T]](Fail(e))
      }
    }, err)

    def foldWhile[T](z: => T, maxDocs: Int = -1)(suc: (T, A) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = foldWhileM[T](z, maxDocs)(syncSuccess[T, A](suc), err)

    def foldWhileM[T](z: => T, maxDocs: Int = -1)(suc: (T, A) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = {
      def go(v: T, it: Iterator[A]): Future[State[T]] = {
        if (!it.hasNext) {
          Future.successful(Cont(v))
        } else Try(it.next) match {
          case Failure(
            x @ ReplyDocumentIteratorExhaustedException(_)) =>
            Future.successful(Fail(x))

          case Failure(e) => err(v, e) match {
            case Cont(cv) => go(cv, it)
            case f @ Fail(Unrecoverable(_)) =>
              /* already marked unrecoverable */ Future.successful(f)

            case Fail(u) =>
              Future.successful(Fail(Unrecoverable(u)))

            case st => Future.successful(st)
          }

          case Success(a) => suc(v, a).recover {
            case cause if it.hasNext => err(v, cause)
          }.flatMap {
            case Cont(cv) => go(cv, it)

            case Fail(cause) =>
              // Prevent error handler at bulk/response level to recover
              Future.successful(Fail(Unrecoverable(cause)))

            case st => Future.successful(st)
          }
        }
      }

      foldBulksM(z, maxDocs)(go, err)
    }

    def collect[M[_]](maxDocs: Int, err: ErrorHandler[M[A]])(implicit cbf: CanBuildFrom[M[_], A, M[A]], ec: ExecutionContext): Future[M[A]] =
      foldWhile[Builder[A, M[A]]](cbf(), maxDocs)(
        { (builder, a) => Cont(builder += a) },
        { (b: Builder[A, M[A]], t: Throwable) =>
          err(b.result(), t).map[Builder[A, M[A]]](_ => b)
        }).map(_.result())

    def nextResponse(maxDocs: Int): (ExecutionContext, Response) => Future[Option[Response]] = {
      if (!tailable) { (ec: ExecutionContext, r: Response) =>
        if (!hasNext(r, maxDocs)) {
          Future.successful(Option.empty[Response])
        } else {
          next(r, maxDocs)(ec)
        }
      } else { (ec: ExecutionContext, r: Response) =>
        tailResponse(r, maxDocs)(ec)
      }
    }
  }

  @inline private def nextBatchOffset(response: Response): Int =
    response.reply.numberReturned + response.reply.startingFrom

  @inline private def toReturn(
    batchSizeN: Int, maxDocs: Int, offset: Int): Int = {
    // Normalizes the max number of documents
    val max = if (maxDocs < 0) Int.MaxValue else maxDocs

    if (batchSizeN > 0 && (offset + batchSizeN) <= max) {
      // Valid `numberToReturn` and next batch won't exceed the max
      batchSizeN
    } else {
      max - offset
    }
  }
}

private[api] final class FoldResponses[T](
  nextResponse: (ExecutionContext, Response) => Future[Option[Response]],
  killCursors: (Long, String) => Unit,
  maxDocs: Int,
  suc: (T, Response) => Future[Cursor.State[T]],
  err: Cursor.ErrorHandler[T])(implicit actorSys: ActorSystem, ec: ExecutionContext) { self =>
  import Cursor.{ Cont, Done, Fail, State, logger }
  import CursorOps.Unrecoverable

  private val promise = scala.concurrent.Promise[T]()
  lazy val result: Future[T] = promise.future

  private val handle: Any => Unit = {
    case ProcResponses(makeReq, cur, c, id) =>
      procResponses(makeReq(), cur, c, id)

    case HandleResponse(last, cur, c) =>
      handleResponse(last, cur, c)

    case ProcNext(last, cur, next, c) =>
      procNext(last, cur, next, c)

    case OnError(last, cur, error, c) =>
      onError(last, cur, error, c)
  }

  @inline private def kill(cursorID: Long): Unit = try {
    killCursors(cursorID, "FoldResponses")
  } catch {
    case cause: Throwable =>
      logger.warn(s"fails to kill cursor: $cursorID", cause)
  }

  @inline private def ok(r: Response, v: T): Unit = {
    kill(r.reply.cursorID) // Releases cursor before ending
    promise.success(v)
    ()
  }

  @inline private def ko(r: Response, f: Throwable): Unit = {
    kill(r.reply.cursorID) // Releases cursor before ending
    promise.failure(f)
    ()
  }

  @inline private def handleResponse(last: Response, cur: T, c: Int): Unit = {
    logger.trace(s"Process response: $last")

    val handled: Future[State[T]] = try {
      suc(cur, last)
    } catch {
      case unsafe: Exception /* see makeIterator */ =>
        Future.failed[State[T]](unsafe)
    }

    def nc = c + last.reply.numberReturned

    handled.onComplete({
      case Success(next)  => self ! ProcNext(last, cur, next, nc)
      case Failure(error) => self ! OnError(last, cur, error, nc)
    })(ec)
  }

  @inline
  private def onError(last: Response, cur: T, error: Throwable, c: Int): Unit =
    error match {
      case Unrecoverable(e) =>
        ko(last, e) // already marked recoverable

      case _ => err(cur, error) match {
        case Done(d) => ok(last, d)

        case Fail(f) => ko(last, f)

        case next @ Cont(v) =>
          self ! ProcNext(last, v /*cur*/ , next, c)
      }
    }

  @inline private def fetch(
    c: Int,
    ec: ExecutionContext,
    r: Response): Future[Option[Response]] = {
    // Enforce maxDocs check as r.reply.startingFrom (checked in hasNext),
    // will always be set to 0 by the server for tailable cursor/capped coll
    if (c < maxDocs) {
      // nextResponse will take care of cursorID, ...
      nextResponse(ec, r)
    } else {
      Future.successful(Option.empty[Response])
    }
  }

  @inline private def procNext(last: Response, cur: T, next: State[T], c: Int): Unit = next match {
    case Done(d) => ok(last, d)

    case Fail(f) => self ! OnError(last, cur, f, c)

    case Cont(v) => fetch(c, ec, last).onComplete({
      case Success(Some(r)) => self ! ProcResponses(
        () => Future.successful(r), v, c, r.reply.cursorID)

      case Success(_) => ok(last, v)
      case Failure(e) => ko(last, e)
    })(ec)
  }

  @inline private def procResponses(last: Future[Response], cur: T, c: Int, lastID: Long): Unit = last.onComplete({
    case Success(r) => self ! HandleResponse(r, cur, c)

    case Failure(error) => {
      logger.error("fails to send request", error)

      err(cur, error) match {
        case Done(v) => {
          if (lastID > 0) kill(lastID)
          promise.success(v)
        }

        case Fail(e) => {
          if (lastID > 0) kill(lastID)
          promise.failure(e)
        }

        case Cont(v) => {
          logger.warn("cannot continue after fatal request error", error)

          promise.success(v)
        }
      }
    }
  })(ec)

  /**
   * Enqueues a `message` to be processed while fold the cursor results.
   */
  def !(message: Any): Unit = {
    actorSys.scheduler.scheduleOnce(
      // TODO: on retry, add some delay according FailoverStrategy
      scala.concurrent.duration.Duration.Zero)(handle(message))(ec)

    ()
  }

  // Messages

  /**
   * @param requester the function the perform the next request
   * @param cur $curParam
   * @param c $cParam
   * @param lastID the last ID for the cursor (or `-1` if unknown)
   */
  private[api] case class ProcResponses(
    requester: () => Future[Response],
    cur: T,
    c: Int,
    lastID: Long)

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param c $cParam
   */
  private case class HandleResponse(last: Response, cur: T, c: Int)

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param next the next state
   * @param c $cParam
   */
  private case class ProcNext(last: Response, cur: T, next: State[T], c: Int)

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param error the error details
   * @param c $cParam
   */
  private case class OnError(last: Response, cur: T, error: Throwable, c: Int)
}

private[api] object FoldResponses {
  def apply[T](
    z: => T,
    makeRequest: ExecutionContext => Future[Response],
    nextResponse: (ExecutionContext, Response) => Future[Option[Response]],
    killCursors: (Long, String) => Unit,
    suc: (T, Response) => Future[Cursor.State[T]],
    err: Cursor.ErrorHandler[T],
    maxDocs: Int)(implicit actorSys: ActorSystem, ec: ExecutionContext): Future[T] = {
    Future(z)(ec).flatMap({ v =>
      val max = if (maxDocs > 0) maxDocs else Int.MaxValue
      val f = new FoldResponses[T](
        nextResponse, killCursors, max, suc, err)(actorSys, ec)

      f ! f.ProcResponses(() => makeRequest(ec), v, 0, -1L)

      f.result
    })(ec)
  }
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
