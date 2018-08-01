package reactivemongo.api

import scala.language.higherKinds

import scala.util.{ Failure, Success, Try }

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.bson.BSONDocument

import reactivemongo.util.ExtendedFutures.DelayedFuture

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

import reactivemongo.core.actors.{
  Exceptions,
  RequestMakerExpectingResponse
}

import reactivemongo.api.commands.ResultCursor

@deprecated("Will be private/internal", "0.16.0")
object DefaultCursor {
  import Cursor.{ ErrorHandler, State, Cont, Fail, logger }
  import CursorOps.Unrecoverable

  @deprecated("No longer implemented", "0.16.0")
  def query[P <: SerializationPack, A](
    pack: P,
    query: Query,
    requestBuffer: Int => BufferSequence,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean,
    collectionName: String)(implicit reader: pack.Reader[A]): Impl[A] =
    throw new UnsupportedOperationException("Use query with DefaultDB")

  /**
   * @param collectionName the fully qualified collection name (even if `query.fullCollectionName` is `\$cmd`)
   */
  private[reactivemongo] def query[P <: SerializationPack, A](
    pack: P,
    query: Query,
    requestBuffer: Int => BufferSequence,
    readPreference: ReadPreference,
    db: DB,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean,
    collectionName: String)(implicit reader: pack.Reader[A]): Impl[A] =
    new Impl[A] {
      val preference = readPreference
      val database = db
      val failoverStrategy = failover
      val mongo26WriteOp = isMongo26WriteOp
      val fullCollectionName = collectionName

      val numberToReturn = {
        val version = connection._metadata.
          fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

        if (version.compareTo(MongoWireVersion.V32) < 0) {
          // see QueryOpts.batchSizeN

          if (query.numberToReturn <= 0) {
            Cursor.DefaultBatchSize
          } else query.numberToReturn
        } else {
          1 // nested 'cursor' document
        }
      }

      val tailable = (query.flags &
        QueryFlags.TailableCursor) == QueryFlags.TailableCursor

      val makeIterator = ReplyDocumentIterator.parse(pack)(_: Response)(reader)

      @inline def makeRequest(maxDocs: Int)(implicit ctx: ExecutionContext): Future[Response] = Failover2(connection, failoverStrategy) { () =>
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

  @deprecated("No longer implemented", "0.16.0")
  def getMore[P <: SerializationPack, A](
    pack: P,
    preload: => Response,
    result: ResultCursor,
    toReturn: Int,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean)(implicit reader: pack.Reader[A]): Impl[A] =
    throw new UnsupportedOperationException("No longer implemented")

  private[reactivemongo] trait Impl[A] extends Cursor[A] with CursorOps[A] {
    /** The read preference */
    def preference: ReadPreference

    def database: DB

    @inline def connection: MongoConnection = database.connection

    def failoverStrategy: FailoverStrategy

    def mongo26WriteOp: Boolean

    def fullCollectionName: String

    def numberToReturn: Int

    def tailable: Boolean

    def makeIterator: Response => Iterator[A] // Unsafe

    final def documentIterator(response: Response): Iterator[A] =
      makeIterator(response)

    protected final lazy val version = connection._metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    @inline protected def lessThenV32: Boolean =
      version.compareTo(MongoWireVersion.V32) < 0

    protected lazy val requester: (Int, Int, RequestMakerExpectingResponse) => ExecutionContext => Future[Response] = {
      val base: ExecutionContext => RequestMakerExpectingResponse => Future[Response] = { implicit ec: ExecutionContext =>
        database.session match {
          case Some(session) => { req: RequestMakerExpectingResponse =>
            connection.sendExpectingResponse(req).flatMap {
              Session.updateOnResponse(session, _).map(_._2)
            }
          }

          case _ =>
            connection.sendExpectingResponse(_: RequestMakerExpectingResponse)
        }
      }

      if (lessThenV32) {
        { (off: Int, maxDocs: Int, req: RequestMakerExpectingResponse) =>
          val max = if (maxDocs > 0) maxDocs else Int.MaxValue

          { implicit ec: ExecutionContext =>
            base(ec)(req).map { response =>
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
          base(ec)(req).map {
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

        def send() = connection.sendExpectingResponse(
          RequestMakerExpectingResponse(RequestMaker(
            KillCursors(Set(cursorID)),
            readPreference = preference), false))

        val result = database.session match {
          case Some(session) => send().flatMap {
            Session.updateOnResponse(session, _)
          }.map(_._2)

          case _ => send()
        }

        result.onComplete {
          case Failure(cause) => logger.warn(
            s"[$logCat] Fails to kill cursor #${cursorID}", cause)

          case _ => ()
        }
      } else {
        logger.trace(s"[$logCat] Nothing to release: cursor already exhausted ($cursorID)")
      }
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
