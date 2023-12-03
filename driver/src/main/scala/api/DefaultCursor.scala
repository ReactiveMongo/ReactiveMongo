package reactivemongo.api

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.core.actors.{ Exceptions, ExpectingResponse }
import reactivemongo.core.protocol.{
  GetMore,
  KillCursors,
  Message,
  MongoWireVersion,
  Query,
  QueryFlags,
  ReplyDocumentIterator,
  ReplyDocumentIteratorExhaustedException,
  RequestMaker,
  Response
}

import reactivemongo.api.bson.buffer.WritableBuffer
import reactivemongo.api.commands.{ CommandCodecs, CommandKind }

import reactivemongo.util.ExtendedFutures.delayedFuture

private[reactivemongo] object DefaultCursor {
  import Cursor.{ State, Cont, Fail, logger }
  import CursorOps.UnrecoverableException

  /**
   * Since MongoDB 6+
   */
  private[reactivemongo] def query[P <: SerializationPack, A](
      pack: P,
      query: Message,
      requestBuffer: Int => ByteBuf,
      readPreference: ReadPreference,
      db: DB,
      failover: FailoverStrategy,
      collectionName: String,
      _maxAwaitTimeMs: Option[Long],
      _tailable: Boolean
    )(implicit
      reader: pack.Reader[A]
    ): Impl[A] =
    new Impl[A] {
      val preference = readPreference
      val database = db
      val failoverStrategy = failover
      val fullCollectionName = collectionName
      val maxAwaitTimeMs = _maxAwaitTimeMs

      val numberToReturn = 1

      val tailable = _tailable

      val makeIterator = ReplyDocumentIterator.parse(pack)(_: Response)(reader)

      @inline def makeRequest(
          maxDocs: Int
        )(implicit
          ec: ExecutionContext
        ): Future[Response] =
        Failover(connection, failoverStrategy) { () =>
          val req = new ExpectingResponse(
            requestMaker = RequestMaker(
              CommandKind.Query,
              query,
              requestBuffer(maxDocs),
              readPreference,
              channelIdHint = None,
              callerSTE = callerSTE
            ),
            pinnedNode = transaction.flatMap(_.pinnedNode)
          )

          requester(0, maxDocs, req)(ec)
        }.future.flatMap {
          case Response.CommandError(_, _, _, cause) =>
            Future.failed[Response](cause)

          case response => {
            val cursorID = response.reply.cursorID

            if (response.reply.numberReturned == maxDocs && cursorID != 0) {
              // See https://jira.mongodb.org/browse/SERVER-80713
              def res = response.cursorID(0)

              sendCursorKill(cursorID).map(_ => res).recover { case _ => res }
            } else {
              Future.successful(response)
            }
          }
        }

      private lazy val builder = pack.newBuilder

      private lazy val makeCursorKill = DefaultCursor.makeCursorKill(
        pack,
        version = connection._metadata
          .fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion),
        dbName = db.name,
        collectionName = collectionName,
        readPreference = preference
      )

      def sendCursorKill(cursorID: Long): Future[Response] =
        connection.sendExpectingResponse(
          new ExpectingResponse(
            requestMaker = makeCursorKill(cursorID),
            pinnedNode = transaction.flatMap(_.pinnedNode)
          )
        )

      override val getMoreOpCmd: Function2[Long, Int, RequestMaker] = {
        import reactivemongo.api.collections.QueryCodecs

        val writeReadPref = QueryCodecs.writeReadPref(builder)

        def baseElmts: Seq[pack.ElementProducer] = db.session match {
          case Some(session) =>
            CommandCodecs.writeSession(builder)(session)

          case _ =>
            Seq.empty[pack.ElementProducer]
        }

        // numberToSkip = 0, numberToReturn = 1)
        val moreQry = query.copy(flags = 0)
        val collName = fullCollectionName.span(_ != '.')._2.tail

        { (cursorId, ntr) =>
          import builder.{ elementProducer => elem, int, long, string }

          val pref = writeReadPref(readPreference)

          val cmdOpts = Seq.newBuilder[pack.ElementProducer] ++= Seq(
            elem("getMore", long(cursorId)),
            elem(f"$$db", string(database.name)),
            elem(f"$$readPreference", pref),
            elem("collection", string(collName)),
            elem("batchSize", int(ntr))
          ) ++= baseElmts

          maxAwaitTimeMs.foreach { ms =>
            cmdOpts += elem("maxTimeMS", long(ms))
          }

          val cmd = builder.document(cmdOpts.result())
          val buffer = WritableBuffer.empty

          buffer.writeByte(0) // OpMsg payload type
          pack.writeToBuffer(buffer, cmd)

          RequestMaker(
            kind = CommandKind.Query,
            op = moreQry,
            section = buffer.buffer,
            readPreference,
            channelIdHint = None,
            callerSTE = Seq.empty
          )
        }
      }
    }

  /**
   * @param collectionName the fully qualified collection name (even if `query.fullCollectionName` is `\$cmd`)
   */
  private[reactivemongo] def query[P <: SerializationPack, A](
      pack: P,
      query: Query,
      requestBuffer: Int => ByteBuf,
      readPreference: ReadPreference,
      db: DB,
      failover: FailoverStrategy,
      collectionName: String,
      _maxAwaitTimeMs: Option[Long]
    )(implicit
      reader: pack.Reader[A]
    ): Impl[A] =
    new Impl[A] {
      val preference = readPreference
      val database = db
      val failoverStrategy = failover
      val fullCollectionName = collectionName
      val maxAwaitTimeMs = _maxAwaitTimeMs

      val numberToReturn = {
        val version = connection._metadata
          .fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

        if (version.compareTo(MongoWireVersion.V32) < 0) {
          // see QueryBuilder.batchSizeN

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

      @inline def makeRequest(
          maxDocs: Int
        )(implicit
          ec: ExecutionContext
        ): Future[Response] =
        Failover(connection, failoverStrategy) { () =>
          val ntr = toReturn(numberToReturn, maxDocs, 0)

          // MongoDB2.6: Int.MaxValue
          val op = query.copy(numberToReturn = ntr)
          val req = new ExpectingResponse(
            requestMaker = RequestMaker(
              CommandKind.Query,
              op,
              requestBuffer(maxDocs),
              readPreference,
              channelIdHint = None,
              callerSTE = callerSTE
            ),
            pinnedNode = transaction.flatMap(_.pinnedNode)
          )

          requester(0, maxDocs, req)(ec)
        }.future.flatMap {
          case Response.CommandError(_, _, _, cause) =>
            Future.failed[Response](cause)

          case response =>
            Future.successful(response)
        }

      private lazy val builder = pack.newBuilder

      private lazy val makeCursorKill = DefaultCursor.makeCursorKill(
        pack,
        version = connection._metadata
          .fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion),
        dbName = db.name,
        collectionName = collectionName,
        readPreference = preference
      )

      def sendCursorKill(cursorID: Long): Future[Response] =
        connection.sendExpectingResponse(
          new ExpectingResponse(
            requestMaker = makeCursorKill(cursorID),
            pinnedNode = transaction.flatMap(_.pinnedNode)
          )
        )

      val getMoreOpCmd: Function2[Long, Int, RequestMaker] = {
        def baseElmts: Seq[pack.ElementProducer] = db.session match {
          case Some(session) =>
            CommandCodecs.writeSession(builder)(session)

          case _ =>
            Seq.empty[pack.ElementProducer]
        }

        if (lessThenV32) { (cursorId, ntr) =>
          RequestMaker(
            op = GetMore(fullCollectionName, ntr, cursorId),
            document = Unpooled.EMPTY_BUFFER,
            readPreference,
            channelIdHint = None
          )

        } else {
          val moreQry = query.copy(numberToSkip = 0, numberToReturn = 1)
          val collName = fullCollectionName.span(_ != '.')._2.tail

          { (cursorId, ntr) =>
            import builder.{ elementProducer => elem, int, long, string }

            val cmdOpts = Seq.newBuilder[pack.ElementProducer] ++= Seq(
              elem("getMore", long(cursorId)),
              elem("collection", string(collName)),
              elem("batchSize", int(ntr))
            ) ++= baseElmts

            maxAwaitTimeMs.foreach { ms =>
              cmdOpts += elem("maxTimeMS", long(ms))
            }

            val cmd = builder.document(cmdOpts.result())
            val buf = WritableBuffer.empty

            pack.writeToBuffer(buf, cmd)

            RequestMaker(
              kind = CommandKind.Query,
              op = moreQry,
              document = buf.buffer,
              readPreference,
              channelIdHint = None,
              callerSTE = Seq.empty
            )
          }
        }
      }
    }

  private[reactivemongo] abstract class GetMoreCursor[A](
      db: DB,
      _ref: Cursor.Reference,
      readPreference: ReadPreference,
      failover: FailoverStrategy,
      maxTimeMS: Option[Long])
      extends Impl[A] {
    protected type P <: SerializationPack
    protected val _pack: P
    protected def reader: _pack.Reader[A]

    val preference = readPreference
    val database = db
    val failoverStrategy = failover
    val maxAwaitTimeMs = Option.empty[Long]

    @inline def fullCollectionName = _ref.collectionName

    val numberToReturn = {
      if (version.compareTo(MongoWireVersion.V32) < 0) {
        // see QueryBuilder.batchSizeN

        if (_ref.numberToReturn <= 0) {
          Cursor.DefaultBatchSize
        } else _ref.numberToReturn
      } else {
        1 // nested 'cursor' document
      }
    }

    @inline def tailable: Boolean = _ref.tailable

    val makeIterator = ReplyDocumentIterator.parse(_pack)(_: Response)(reader)

    @inline def makeRequest(
        maxDocs: Int
      )(implicit
        ec: ExecutionContext
      ): Future[Response] = {
      val pinnedNode = transaction.flatMap(_.pinnedNode)

      Failover(connection, failoverStrategy) { () =>
        // MongoDB2.6: Int.MaxValue
        val req = new ExpectingResponse(
          requestMaker = getMoreOpCmd(_ref.cursorId, maxDocs),
          pinnedNode = pinnedNode
        )

        requester(0, maxDocs, req)(ec)
      }.future.flatMap {
        case Response.CommandError(_, _, _, cause) =>
          Future.failed[Response](cause)

        case response =>
          Future.successful(response)
      }
    }

    final lazy val builder = _pack.newBuilder

    private lazy val makeCursorKill = DefaultCursor.makeCursorKill(
      _pack,
      version = connection._metadata
        .fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion),
      dbName = db.name,
      collectionName = fullCollectionName,
      readPreference = preference
    )

    def sendCursorKill(cursorID: Long): Future[Response] =
      connection.sendExpectingResponse(
        new ExpectingResponse(
          requestMaker = makeCursorKill(cursorID),
          pinnedNode = transaction.flatMap(_.pinnedNode)
        )
      )

    lazy val getMoreOpCmd: Function2[Long, Int, RequestMaker] = {
      import reactivemongo.api.collections.QueryCodecs

      def baseElmts: Seq[_pack.ElementProducer] = db.session match {
        case Some(session) =>
          CommandCodecs.writeSession(builder)(session)

        case _ =>
          Seq.empty[_pack.ElementProducer]
      }

      val writeReadPref = QueryCodecs.writeReadPref(builder)

      if (lessThenV32) { (cursorId, ntr) =>
        RequestMaker(
          GetMore(fullCollectionName, ntr, cursorId),
          Unpooled.EMPTY_BUFFER,
          readPreference,
          None
        )
      } else {
        val collName = fullCollectionName.span(_ != '.')._2.tail

        import builder.{ elementProducer => elem, int, long, string }

        if (version.compareTo(MongoWireVersion.V60) < 0) { (cursorId, ntr) =>
          val cmdOpts = Seq.newBuilder[_pack.ElementProducer] ++= Seq(
            elem("getMore", long(cursorId)),
            elem("collection", string(collName)),
            elem("batchSize", int(ntr))
          ) ++= baseElmts

          maxTimeMS.foreach { ms => cmdOpts += elem("maxTimeMS", long(ms)) }

          val cmd = builder.document(cmdOpts.result())
          val buf = WritableBuffer.empty

          _pack.writeToBuffer(buf, cmd)

          RequestMaker(
            GetMore(fullCollectionName, ntr, cursorId),
            buf.buffer,
            readPreference,
            None
          )

        } else { (cursorId, ntr) =>
          val cmdOpts = Seq.newBuilder[_pack.ElementProducer] ++= Seq(
            elem("getMore", long(cursorId)),
            elem("collection", string(collName)),
            elem("batchSize", int(ntr))
          ) ++= baseElmts

          maxTimeMS.foreach { ms => cmdOpts += elem("maxTimeMS", long(ms)) }

          val moreQry = Message(
            flags = 0,
            checksum = None,
            requiresPrimary = !readPreference.slaveOk
          )

          val pref = writeReadPref(readPreference)

          cmdOpts ++= Seq(
            elem(f"$$db", string(db.name)),
            elem(f"$$readPreference", pref)
          )

          val cmd = builder.document(cmdOpts.result())
          val buffer = WritableBuffer.empty

          buffer.writeByte(0) // OpMsg payload type
          _pack.writeToBuffer(buffer, cmd)

          RequestMaker(
            kind = CommandKind.Query,
            op = moreQry,
            section = buffer.buffer,
            readPreference,
            channelIdHint = None,
            callerSTE = Seq.empty
          )
        }
      }
    }
  }

  private[reactivemongo] trait Impl[A]
      extends Cursor[A]
      with CursorOps[A]
      with CursorCompat[A] {

    /** The read preference */
    def preference: ReadPreference

    def database: DB

    @inline protected final def transaction =
      database.session.flatMap(_.transaction.toOption)

    @inline def connection: MongoConnection = database.connection

    def failoverStrategy: FailoverStrategy

    def fullCollectionName: String

    def numberToReturn: Int

    def maxAwaitTimeMs: Option[Long]

    def tailable: Boolean

    def makeIterator: Response => Iterator[A] // Unsafe

    final def documentIterator(response: Response): Iterator[A] =
      makeIterator(response)

    protected final lazy val version = connection._metadata
      .fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    protected final val callerSTE: Seq[StackTraceElement] =
      reactivemongo.util.Trace.currentTraceElements.drop(2).take(15)

    @inline protected def lessThenV32: Boolean =
      version.compareTo(MongoWireVersion.V32) < 0

    protected lazy val requester: (Int, Int, ExpectingResponse) => ExecutionContext => Future[Response] = {
      val base: ExecutionContext => ExpectingResponse => Future[Response] = {
        implicit ec: ExecutionContext =>
          database.session match {
            case Some(session) => { (req: ExpectingResponse) =>
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
              } else
                response match {
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
      } else { (from: Int, _: Int, req32: ExpectingResponse) =>
        { implicit ec: ExecutionContext =>
          base(ec)(req32).map {
            // Normalizes as 'new' cursor doesn't indicate such property
            _.startingFrom(from)
          }
        }
      }
    }

    // cursorId: Long, toReturn: Int
    protected def getMoreOpCmd: Function2[Long, Int, RequestMaker]

    private def next(
        response: Response,
        maxDocs: Int
      )(implicit
        ec: ExecutionContext
      ): Future[Option[Response]] = {
      if (response.reply.cursorID != 0) {
        // numberToReturn=1 for new find command,
        // so rather use batchSize from the previous reply
        val reply = response.reply
        val nextOffset = nextBatchOffset(response)
        val ntr = toReturn(reply.numberReturned, maxDocs, nextOffset)

        val reqMaker = getMoreOpCmd(reply.cursorID, ntr)

        logger.trace(s"Asking for the next batch of $ntr documents on cursor #${reply.cursorID}, after ${nextOffset}: ${reqMaker.op}")

        def req = new ExpectingResponse(
          requestMaker = reqMaker,
          pinnedNode = transaction.flatMap(_.pinnedNode)
        )

        Failover(connection, failoverStrategy) { () =>
          requester(nextOffset, maxDocs, req)(ec)
        }.future.map(Some(_))
      } else {
        logger.warn("Call to next() but cursorID is 0, there is probably a bug")

        Future.successful(Option.empty[Response])
      }
    }

    @inline private def hasNext(response: Response, maxDocs: Int): Boolean =
      (response.reply.cursorID != 0) && (maxDocs < 0 || (nextBatchOffset(
        response
      ) < maxDocs))

    private lazy val renewTimeMs = maxAwaitTimeMs.getOrElse(500L)

    /** Returns next response using tailable mode */
    private def tailResponse(
        current: Response,
        maxDocs: Int
      )(implicit
        ec: ExecutionContext
      ): Future[Option[Response]] = {
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

          delayedFuture(renewTimeMs, connection.actorSystem).flatMap { _ =>
            makeRequest(maxDocs).map(Some(_))
          }
        }
      }
    }

    def killCursor(
        id: Long
      )(implicit
        ec: ExecutionContext
      ): Unit = killCursors(id, "Cursor")

    private def killCursors(
        cursorID: Long,
        logCat: String
      )(implicit
        ec: ExecutionContext
      ): Unit = {
      if (cursorID != 0) {
        logger.debug(s"[$logCat] Clean up $cursorID, sending KillCursors")

        sendCursorKill(cursorID).onComplete {
          case Failure(cause) =>
            logger.warn(s"[$logCat] Fails to kill cursor #${cursorID}", cause)

          case _ => ()
        }
      } else {
        logger.trace(
          s"[$logCat] Nothing to release: cursor already exhausted ($cursorID)"
        )
      }
    }

    protected def sendCursorKill(cursorID: Long): Future[Response]

    def head(
        implicit
        ec: ExecutionContext
      ): Future[A] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.failed[A](Cursor.NoSuchResultException)
        } else Future(result.next())
      }

    final def headOption(
        implicit
        ec: ExecutionContext
      ): Future[Option[A]] =
      makeRequest(1).flatMap { response =>
        val result = documentIterator(response)

        if (!result.hasNext) {
          Future.successful(Option.empty[A])
        } else {
          Future(Some(result.next()))
        }
      }

    @inline private def syncSuccess[T, U](
        f: (T, U) => State[T]
      )(implicit
        ec: ExecutionContext
      ): (T, U) => Future[State[T]] = { (a: T, b: U) => Future(f(a, b)) }

    private def foldResponsesM[T](
        z: => T,
        maxDocs: Int
      )(suc: (T, Response) => Future[State[T]],
        err: (T, Throwable) => State[T]
      )(implicit
        ec: ExecutionContext
      ): Future[T] = FoldResponses(
      failoverStrategy,
      z,
      makeRequest(maxDocs)(_: ExecutionContext),
      nextResponse(maxDocs),
      killCursors _,
      suc,
      err,
      maxDocs
    )(connection.actorSystem, ec)

    def foldBulks[T](
        z: => T,
        maxDocs: Int = -1
      )(suc: (T, Iterator[A]) => State[T],
        err: (T, Throwable) => State[T]
      )(implicit
        ec: ExecutionContext
      ): Future[T] =
      foldBulksM[T](z, maxDocs)(syncSuccess[T, Iterator[A]](suc), err)

    def foldBulksM[T](
        z: => T,
        maxDocs: Int = -1
      )(suc: (T, Iterator[A]) => Future[State[T]],
        err: (T, Throwable) => State[T]
      )(implicit
        ec: ExecutionContext
      ): Future[T] = foldResponsesM(z, maxDocs)(
      { (s, r) =>
        Try(makeIterator(r)) match {
          case Success(it) => suc(s, it)
          case Failure(e)  => Future.successful[State[T]](Fail(e))
        }
      },
      err
    )

    def foldWhile[T](
        z: => T,
        maxDocs: Int = -1
      )(suc: (T, A) => State[T],
        err: (T, Throwable) => State[T]
      )(implicit
        ec: ExecutionContext
      ): Future[T] = foldWhileM[T](z, maxDocs)(syncSuccess[T, A](suc), err)

    def foldWhileM[T](
        z: => T,
        maxDocs: Int = -1
      )(suc: (T, A) => Future[State[T]],
        err: (T, Throwable) => State[T]
      )(implicit
        ec: ExecutionContext
      ): Future[T] = {
      def go(v: T, it: Iterator[A]): Future[State[T]] = {
        if (!it.hasNext) {
          Future.successful(Cont(v))
        } else
          Try(it.next()) match {
            case Failure(x: ReplyDocumentIteratorExhaustedException) =>
              Future.successful(Fail(x))

            case Failure(e) =>
              err(v, e) match {
                case Cont(cv)                            => go(cv, it)
                case f @ Fail(UnrecoverableException(_)) =>
                  /* already marked unrecoverable */
                  Future.successful(f)

                case Fail(u) =>
                  Future.successful(Fail(UnrecoverableException(u)))

                case st => Future.successful(st)
              }

            case Success(a) =>
              suc(v, a).recover {
                case cause if it.hasNext => err(v, cause)
              }.flatMap {
                case Cont(cv) => go(cv, it)

                case Fail(cause) =>
                  // Prevent error handler at bulk/response level to recover
                  Future.successful(Fail(UnrecoverableException(cause)))

                case st => Future.successful(st)
              }
          }
      }

      foldBulksM(z, maxDocs)(go, err)
    }

    def nextResponse(
        maxDocs: Int
      ): (ExecutionContext, Response) => Future[Option[Response]] = {
      if (!tailable) { (ec1: ExecutionContext, r1: Response) =>
        if (!hasNext(r1, maxDocs)) {
          Future.successful(Option.empty[Response])
        } else {
          next(r1, maxDocs)(ec1).map({
            case Some(nextResp)
                if (nextResp.reply.cursorID != 0 && maxDocs > 0 && (nextResp.reply.startingFrom + nextResp.reply.numberReturned) >= maxDocs) => {
              // See https://jira.mongodb.org/browse/SERVER-80713
              killCursor(nextResp.reply.cursorID)(ec1)

              Some(nextResp.cursorID(0))
            }

            case nextResp =>
              nextResp
          })(ec1)
        }
      } else { (ec2: ExecutionContext, r2: Response) =>
        tailResponse(r2, maxDocs)(ec2)
      }
    }
  }

  @inline private def nextBatchOffset(response: Response): Int =
    response.reply.numberReturned + response.reply.startingFrom

  @inline private def toReturn(
      batchSizeN: Int,
      maxDocs: Int,
      offset: Int
    ): Int = {
    // Normalizes the max number of documents
    val max = if (maxDocs < 0) Int.MaxValue else maxDocs

    if (batchSizeN > 0 && (offset + batchSizeN) <= max) {
      // Valid `numberToReturn` and next batch won't exceed the max
      batchSizeN
    } else {
      max - offset
    }
  }

  private[api] def makeCursorKill[P <: SerializationPack](
      pack: P,
      version: MongoWireVersion,
      dbName: String,
      collectionName: String,
      readPreference: ReadPreference
    ): Long => RequestMaker = {
    if (version.compareTo(MongoWireVersion.V51) < 0) { (cursorID: Long) =>
      RequestMaker(
        KillCursors(Set(cursorID)),
        readPreference = readPreference
      )
    } else {
      val builder = pack.newBuilder

      import builder.{ elementProducer => elem, long, string }

      { (cursorID: Long) =>
        val cmdOpts = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elem("killCursors", string(collectionName)),
          elem("cursors", builder.array(Seq(long(cursorID))))
        )

        val cmd = builder.document(cmdOpts.result())
        val buffer = WritableBuffer.empty

        buffer.writeByte(0) // OpMsg payload type
        pack.writeToBuffer(buffer, cmd)

        RequestMaker(
          kind = CommandKind.Query,
          op = Query(0, dbName + f"$$cmd", 0, 1),
          buffer.buffer,
          readPreference = readPreference,
          channelIdHint = None,
          callerSTE = Seq.empty
        )
      }
    }
  }
}
