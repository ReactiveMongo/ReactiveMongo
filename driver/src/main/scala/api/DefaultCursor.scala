package reactivemongo.api

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ ExecutionContext, Future }

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
  ExpectingResponse
}

private[reactivemongo] object DefaultCursor {
  import Cursor.{ State, Cont, Fail, logger }
  import CursorOps.Unrecoverable

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
    collectionName: String,
    maxTimeMS: Option[Long])(implicit reader: pack.Reader[A]): Impl[A] =
    new Impl[A] {
      val preference = readPreference
      val database = db
      val failoverStrategy = failover
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

      @inline def makeRequest(maxDocs: Int)(implicit ec: ExecutionContext): Future[Response] =
        Failover(connection, failoverStrategy) { () =>
          val ntr = toReturn(numberToReturn, maxDocs, 0)

          // MongoDB2.6: Int.MaxValue
          val op = query.copy(numberToReturn = ntr)
          val req = new ExpectingResponse(
            requestMaker = RequestMaker(
              op, requestBuffer(maxDocs), readPreference),
            pinnedNode = transaction.flatMap(_.pinnedNode))

          requester(0, maxDocs, req)(ec)
        }.future.flatMap {
          case Response.CommandError(_, _, _, cause) =>
            Future.failed[Response](cause)

          case response =>
            Future.successful(response)
        }

      val builder = pack.newBuilder

      val getMoreOpCmd: Function2[Long, Int, (RequestOp, BufferSequence)] = {
        if (lessThenV32) { (cursorId, ntr) =>
          GetMore(fullCollectionName, ntr, cursorId) -> BufferSequence.empty
        } else {
          // TODO: re-check numberToReturn = 1
          val moreQry = query.copy(numberToSkip = 0, numberToReturn = 1)
          val collName = fullCollectionName.span(_ != '.')._2.tail

          { (cursorId, ntr) =>
            import builder.{ elementProducer => elem, int, long, string }

            val cmdOpts = Seq.newBuilder[pack.ElementProducer] ++= Seq(
              elem("getMore", long(cursorId)),
              elem("collection", string(collName)),
              elem("batchSize", int(ntr)))

            maxTimeMS.foreach { ms =>
              cmdOpts += elem("maxTimeMS", long(ms))
            }

            val cmd = builder.document(cmdOpts.result())

            moreQry -> BufferSequence.single[pack.type](pack)(cmd)
          }
        }
      }
    }

  private[reactivemongo] abstract class GetMoreCursor[A](
    db: DB,
    _ref: Cursor.Reference,
    readPreference: ReadPreference,
    failover: FailoverStrategy,
    maxTimeMS: Option[Long]) extends Impl[A] {
    protected type P <: SerializationPack
    protected val _pack: P
    protected def reader: _pack.Reader[A]

    val preference = readPreference
    val database = db
    val failoverStrategy = failover

    @inline def fullCollectionName = _ref.collectionName

    val numberToReturn = {
      val version = connection._metadata.
        fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

      if (version.compareTo(MongoWireVersion.V32) < 0) {
        // see QueryOpts.batchSizeN

        if (_ref.numberToReturn <= 0) {
          Cursor.DefaultBatchSize
        } else _ref.numberToReturn
      } else {
        1 // nested 'cursor' document
      }
    }

    @inline def tailable: Boolean = _ref.tailable

    val makeIterator = ReplyDocumentIterator.parse(_pack)(_: Response)(reader)

    @inline def makeRequest(maxDocs: Int)(implicit ec: ExecutionContext): Future[Response] = {
      val pinnedNode = transaction.flatMap(_.pinnedNode)

      Failover(connection, failoverStrategy) { () =>
        // MongoDB2.6: Int.MaxValue
        val op = getMoreOpCmd(_ref.cursorId, maxDocs)
        val req = new ExpectingResponse(
          requestMaker = RequestMaker(op._1, op._2, readPreference),
          pinnedNode = pinnedNode)

        requester(0, maxDocs, req)(ec)
      }.future.flatMap {
        case Response.CommandError(_, _, _, cause) =>
          Future.failed[Response](cause)

        case response =>
          Future.successful(response)
      }
    }

    lazy val builder = _pack.newBuilder

    val getMoreOpCmd: Function2[Long, Int, (RequestOp, BufferSequence)] = {
      if (lessThenV32) { (cursorId, ntr) =>
        GetMore(fullCollectionName, ntr, cursorId) -> BufferSequence.empty
      } else {
        val collName = fullCollectionName.span(_ != '.')._2.tail

        { (cursorId, ntr) =>
          import builder.{ elementProducer => elem, int, long, string }

          val moreQry = GetMore(fullCollectionName, ntr, cursorId)

          val cmdOpts = Seq.newBuilder[_pack.ElementProducer] ++= Seq(
            elem("getMore", long(cursorId)),
            elem("collection", string(collName)),
            elem("batchSize", int(ntr)))

          maxTimeMS.foreach { ms =>
            cmdOpts += elem("maxTimeMS", long(ms))
          }

          val cmd = builder.document(cmdOpts.result())

          moreQry -> BufferSequence.single[_pack.type](_pack)(cmd)
        }
      }
    }
  }

  private[reactivemongo] trait Impl[A]
    extends Cursor[A] with CursorOps[A] with CursorCompat[A] {

    /** The read preference */
    def preference: ReadPreference

    def database: DB

    @inline protected final def transaction =
      database.session.flatMap(_.transaction.toOption)

    @inline def connection: MongoConnection = database.connection

    def failoverStrategy: FailoverStrategy

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

    protected lazy val requester: (Int, Int, ExpectingResponse) => ExecutionContext => Future[Response] = {
      val base: ExecutionContext => ExpectingResponse => Future[Response] = { implicit ec: ExecutionContext =>
        database.session match {
          case Some(session) => { req: ExpectingResponse =>
            connection.sendExpectingResponse(req).flatMap {
              Session.updateOnResponse(session, _).map(_._2)
            }
          }

          case _ =>
            connection.sendExpectingResponse(_: ExpectingResponse)
        }
      }

      if (lessThenV32) {
        { (_: Int, maxDocs: Int, req: ExpectingResponse) =>
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
      } else { (from: Int, _: Int, req: ExpectingResponse) =>
        { implicit ec: ExecutionContext =>
          base(ec)(req).map {
            // Normalizes as 'new' cursor doesn't indicate such property
            _.startingFrom(from)
          }
        }
      }
    }

    // cursorId: Long, toReturn: Int
    protected def getMoreOpCmd: Function2[Long, Int, (RequestOp, BufferSequence)]

    private def next(response: Response, maxDocs: Int)(implicit ec: ExecutionContext): Future[Option[Response]] = {
      if (response.reply.cursorID != 0) {
        // numberToReturn=1 for new find command,
        // so rather use batchSize from the previous reply
        val reply = response.reply
        val nextOffset = nextBatchOffset(response)
        val ntr = toReturn(reply.numberReturned, maxDocs, nextOffset)

        val (op, cmd) = getMoreOpCmd(reply.cursorID, ntr)

        logger.trace(s"Asking for the next batch of $ntr documents on cursor #${reply.cursorID}, after ${nextOffset}: $op")

        def req = new ExpectingResponse(
          requestMaker = RequestMaker(op, cmd,
            readPreference = preference,
            channelIdHint = Some(response.info.channelId)),
          pinnedNode = transaction.flatMap(_.pinnedNode))

        Failover(connection, failoverStrategy) { () =>
          requester(nextOffset, maxDocs, req)(ec)
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
    private def tailResponse(current: Response, maxDocs: Int)(implicit ec: ExecutionContext): Future[Option[Response]] = {
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

    def killCursor(id: Long)(implicit ec: ExecutionContext): Unit =
      killCursors(id, "Cursor")

    private def killCursors(
      cursorID: Long,
      logCat: String)(implicit ec: ExecutionContext): Unit = {
      if (cursorID != 0) {
        logger.debug(s"[$logCat] Clean up $cursorID, sending KillCursors")

        val result = connection.sendExpectingResponse(
          new ExpectingResponse(
            requestMaker = RequestMaker(
              KillCursors(Set(cursorID)),
              readPreference = preference),
            pinnedNode = transaction.flatMap(_.pinnedNode)))

        result.onComplete {
          case Failure(cause) => logger.warn(
            s"[$logCat] Fails to kill cursor #${cursorID}", cause)

          case _ => ()
        }
      } else {
        logger.trace(s"[$logCat] Nothing to release: cursor already exhausted ($cursorID)")
      }
    }

    def head(implicit ec: ExecutionContext): Future[A] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.failed[A](Cursor.NoSuchResultException)
        } else Future(result.next())
      }

    final def headOption(implicit ec: ExecutionContext): Future[Option[A]] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.successful(Option.empty[A])
        } else {
          Future(Some(result.next()))
        }
      }

    @inline private def syncSuccess[T, U](f: (T, U) => State[T])(implicit ec: ExecutionContext): (T, U) => Future[State[T]] = { (a: T, b: U) => Future(f(a, b)) }

    private def foldResponsesM[T](z: => T, maxDocs: Int)(suc: (T, Response) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ec: ExecutionContext): Future[T] = FoldResponses(failoverStrategy, z,
      makeRequest(maxDocs)(_: ExecutionContext),
      nextResponse(maxDocs), killCursors _, suc, err, maxDocs)(
        connection.actorSystem, ec)

    def foldBulks[T](z: => T, maxDocs: Int = -1)(suc: (T, Iterator[A]) => State[T], err: (T, Throwable) => State[T])(implicit ec: ExecutionContext): Future[T] = foldBulksM[T](z, maxDocs)(syncSuccess[T, Iterator[A]](suc), err)

    def foldBulksM[T](z: => T, maxDocs: Int = -1)(suc: (T, Iterator[A]) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ec: ExecutionContext): Future[T] = foldResponsesM(z, maxDocs)({ (s, r) =>
      Try(makeIterator(r)) match {
        case Success(it) => suc(s, it)
        case Failure(e)  => Future.successful[State[T]](Fail(e))
      }
    }, err)

    def foldWhile[T](z: => T, maxDocs: Int = -1)(suc: (T, A) => State[T], err: (T, Throwable) => State[T])(implicit ec: ExecutionContext): Future[T] = foldWhileM[T](z, maxDocs)(syncSuccess[T, A](suc), err)

    def foldWhileM[T](z: => T, maxDocs: Int = -1)(suc: (T, A) => Future[State[T]], err: (T, Throwable) => State[T])(implicit ec: ExecutionContext): Future[T] = {
      def go(v: T, it: Iterator[A]): Future[State[T]] = {
        if (!it.hasNext) {
          Future.successful(Cont(v))
        } else Try(it.next) match {
          case Failure(
            x: ReplyDocumentIteratorExhaustedException) =>
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
