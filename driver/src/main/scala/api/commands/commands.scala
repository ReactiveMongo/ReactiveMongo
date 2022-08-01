package reactivemongo.api.commands

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{
  Cursor,
  CursorOptions,
  Collection,
  DB,
  SerializationPack,
  Session,
  ReadPreference
}
import reactivemongo.api.bson.buffer.WritableBuffer

import reactivemongo.core.protocol.{ Reply, Response, MongoWireVersion }
import reactivemongo.core.actors.ExpectingResponse
import reactivemongo.core.errors.GenericDriverException

trait Command {
  protected[reactivemongo] def commandKind: CommandKind
}

trait CollectionCommand extends Command

trait CommandWithResult[R] { _self: Command => }
trait CommandWithPack[P <: SerializationPack] { _self: Command => }

/**
 * @param response the response associated with the result
 * @param numberToReturn the number of documents to return
 * @param value the value parsed from the response
 */
private[reactivemongo] case class ResponseResult[R](
  response: Response,
  numberToReturn: Int,
  value: R)

/**
 * Fetches a cursor from MongoDB results.
 *
 * @tparam P the type of the serialization pack
 * @tparam C the type of the cursor implementation
 */
sealed trait CursorFetcher[P <: SerializationPack, +C[_] <: Cursor[_]] {
  val pack: P

  def one[A](readPreference: ReadPreference)(implicit reader: pack.Reader[A], ec: ExecutionContext): Future[A]

  def cursor[A](readPreference: ReadPreference)(implicit reader: pack.Reader[A]): C[A]

  protected def defaultReadPreference: ReadPreference
}

private[reactivemongo] object Command {
  import reactivemongo.api.{
    DefaultCursor,
    Failover,
    FailoverStrategy
  }
  import reactivemongo.core.netty.BufferSequence
  import reactivemongo.core.protocol.{
    Message,
    Query,
    QueryFlags,
    RequestMaker,
    Response
  }

  private[commands] lazy val logger =
    reactivemongo.util.LazyLogger("reactivemongo.api.commands")

  def defaultCursorFetcher[P <: SerializationPack, A](db: DB, p: P, kind: CommandKind, command: A, failover: FailoverStrategy)(implicit writer: p.Writer[A]): CursorFetcher[p.type, DefaultCursor.Impl] = fetchCursor[p.type, A](db, db.name + f".$$cmd", p, kind, command, failover, CursorOptions.empty, maxAwaitTimeMS = None)

  /**
   * @param fullCollectionName the fully qualified collection name (even if `query.fullCollectionName` is `\$cmd`)
   */
  def fetchCursor[P <: SerializationPack, A](
    db: DB,
    fullCollectionName: String,
    p: P,
    kind: CommandKind,
    command: A,
    failover: FailoverStrategy,
    options: CursorOptions,
    maxAwaitTimeMS: Option[Long])(implicit writer: p.Writer[A]): CursorFetcher[p.type, DefaultCursor.Impl] = new CursorFetcher[p.type, DefaultCursor.Impl] {
    val pack: p.type = p

    @inline protected def defaultReadPreference = db.defaultReadPreference

    def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = {
      def requestMaker: RequestMaker = {
        if (db.connectionState.metadata.maxWireVersion.compareTo(MongoWireVersion.V60) >= 0) {
          buildOpMsgMaker(pack)(kind, command, writer, readPreference, db.name)
        } else {
          buildRequestMaker(pack)(
            kind, command, writer, readPreference, db.name)
        }
      }

      val contextSTE = reactivemongo.util.Trace.currentTraceElements

      Failover(db.connection, failover) { () =>
        db.connection.sendExpectingResponse(new ExpectingResponse(
          requestMaker = requestMaker,
          pinnedNode = for {
            s <- db.session
            t <- s.transaction.toOption
            n <- t.pinnedNode
          } yield n))
      }.future.recoverWith {
        case cause => Future.failed[Response] {
          cause.setStackTrace(contextSTE.toArray)
          cause
        }
      }.flatMap {
        case Response.CommandError(_, _, _, cause) =>
          cause.originalDocument match {
            case pack.IsDocument(doc) =>
              // Error document as result
              Future(pack.deserialize(doc, reader))

            case _ => Future.failed[T] {
              cause.setStackTrace(contextSTE.toArray)
              cause
            }
          }

        case response @ Response.Successful(_, Reply(_, _, _, 0), _, _) =>
          Future.failed[T](new GenericDriverException(
            s"Cannot parse empty response: $response"))

        case response => db.session match {
          case Some(session) =>
            Session.updateOnResponse(session, response).map {
              case (_, resp) => pack.readAndDeserialize(resp, reader)
            }

          case _ =>
            Future(pack.readAndDeserialize(response, reader))
        }
      }
    }

    def cursor[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T]): DefaultCursor.Impl[T] = {
      if (db.connectionState.metadata.maxWireVersion.compareTo(MongoWireVersion.V60) >= 0) {
        import reactivemongo.api.collections.QueryCodecs

        val op = Message(
          flags = 0 /* TODO: OpMsg flags */ ,
          checksum = None /* TODO: OpMsg checksum */ ,
          requiresPrimary = !readPreference.slaveOk)

        val builder = pack.newBuilder
        val writeReadPref = QueryCodecs.writeReadPref(builder)

        val payload = (_: Int) => {
          import builder.{ elementProducer => elem }

          val buffer = WritableBuffer.empty
          val doc = pack.serialize(command, writer)
          val pref = writeReadPref(readPreference)

          val section: pack.Document = builder.document(
            Seq(
              elem(doc),
              elem(f"$$db", builder.string(db.name)),
              elem(f"$$readPreference", pref)))

          pack.writeToBuffer(buffer, section)

          BufferSequence(buffer.buffer)
        }

        val tailable = (options.
          flags & QueryFlags.TailableCursor) == QueryFlags.TailableCursor

        DefaultCursor.query(
          pack, op, payload, readPreference, db,
          failover, fullCollectionName, maxAwaitTimeMS, tailable)

      } else {
        val flags = {
          if (readPreference.slaveOk) options.slaveOk.flags
          else options.flags
        }

        val op = Query(flags, db.name + f".$$cmd", 0, 1)

        val payload = (_: Int) => {
          val buffer = WritableBuffer.empty
          pack.serializeAndWrite(buffer, command, writer)

          BufferSequence(buffer.buffer)
        }

        DefaultCursor.query(
          pack, op, payload, readPreference, db,
          failover, fullCollectionName, maxAwaitTimeMS)

      }
    }
  }

  final class CommandWithPackRunner[P <: SerializationPack](val pack: P, failover: FailoverStrategy = FailoverStrategy()) {
    def apply[R, C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(db, pack, command.commandKind, command, failover).one[R](rp)

    def apply[C <: Command](db: DB, command: C)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(db, pack, command.commandKind, command, failover)

    // collection
    def apply[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(collection.db, pack, command.commandKind, new ResolvedCollectionCommand(collection.name, command), failover).one[R](rp)

    def apply[C <: CollectionCommand](collection: Collection, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(collection.db, pack, command.commandKind, new ResolvedCollectionCommand(collection.name, command), failover)

    /**
     * Executes the `command` and returns its result
     * along with the MongoDB response.
     */
    def cursor[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C, options: CursorOptions, rp: ReadPreference, maxAwaitTimeMS: Option[Long])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): DefaultCursor.Impl[R] = fetchCursor(collection.db, collection.fullCollectionName, pack,
      command.commandKind,
      new ResolvedCollectionCommand(collection.name, command),
      failover, options, maxAwaitTimeMS).cursor[R](rp)

    /**
     * Executes the `command` and returns its result
     * along with the MongoDB response.
     */
    def withResponse[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C, rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = {
      val cursor = defaultCursorFetcher(collection.db, pack,
        command.commandKind,
        new ResolvedCollectionCommand(collection.name, command), failover).
        cursor[R](rp)

      for {
        resp <- cursor.makeRequest(cursor.numberToReturn)
        iterator = cursor.documentIterator(resp)
        result <- {
          if (!iterator.hasNext) {
            Future.failed(new GenericDriverException("missing result"))
          } else Future.successful(iterator.next())
        }
      } yield ResponseResult(resp, cursor.numberToReturn, result)
    }

    def rawCommand[T](input: T)(implicit writer: pack.Writer[T]): RawCommand =
      RawCommand(pack.serialize(input, writer))

    case class RawCommand(document: pack.Document) extends Command {
      val commandKind = CommandKind.Undefined
    }

    object RawCommand {
      implicit val writer: pack.Writer[RawCommand] = pack.writer(_.document)
    }
  }

  /*
   * Returns a command runner.
   *
   * @param pack the serialization pack
   * @param failover the failover strategy
   *
   * {{{
   * import reactivemongo.api.FailoverStrategy
   * import reactivemongo.api.commands.Command
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONSerializationPack
   *
   * val runner = Command.run(BSONSerializationPack, FailoverStrategy.default)
   * val cmd: runner.RawCommand =
   *   runner.rawCommand(BSONDocument(f"$$count" -> "coll"))
   *
   * def foo(db: reactivemongo.api.DB) = runner(db, cmd)
   * }}}
   */
  def run[P <: SerializationPack](pack: P, failover: FailoverStrategy): CommandWithPackRunner[pack.type] = new CommandWithPackRunner(pack, failover)

  /**
   * @param command the command to be requested
   * @param db the database name
   */
  def buildRequestMaker[P <: SerializationPack, A](pack: P)(
    kind: CommandKind,
    command: A,
    writer: pack.Writer[A],
    readPreference: ReadPreference,
    db: String): RequestMaker = {
    val buffer = WritableBuffer.empty

    pack.serializeAndWrite(buffer, command, writer)

    val documents = BufferSequence(buffer.buffer)
    val flags = if (readPreference.slaveOk) QueryFlags.SlaveOk else 0
    val query = Query(flags, db + f".$$cmd", 0, 1)

    RequestMaker(kind, query, documents, readPreference,
      channelIdHint = None,
      callerSTE = Seq.empty)
  }

  /**
   * @param command the command to be requested
   * @param db the database name
   */
  def buildOpMsgMaker[P <: SerializationPack, A](pack: P)(
    kind: CommandKind,
    command: A,
    writer: pack.Writer[A],
    readPreference: ReadPreference,
    db: String): RequestMaker = {
    import reactivemongo.api.collections.QueryCodecs

    val buffer = WritableBuffer.empty
    val builder = pack.newBuilder
    val writeReadPref = QueryCodecs.writeReadPref(builder)

    import builder.{ elementProducer => elem }

    val doc = pack.serialize(command, writer)
    val pref = writeReadPref(readPreference)

    val section: pack.Document = builder.document(
      Seq(
        elem(doc),
        elem(f"$$db", builder.string(db)),
        elem(f"$$readPreference", pref)))

    pack.writeToBuffer(buffer, section)

    val document = BufferSequence(buffer.buffer)

    val msg = Message(
      flags = 0 /* TODO: OpMsg flags */ ,
      checksum = None /* TODO: OpMsg checksum */ ,
      requiresPrimary = !readPreference.slaveOk)

    RequestMaker(kind, msg, document, readPreference,
      channelIdHint = None, callerSTE = Seq.empty)
  }
}

/**
 * @param collection the name of the collection against which the command is executed
 * @param command the executed command
 */
final class ResolvedCollectionCommand[C <: CollectionCommand](
  val collection: String,
  val command: C) extends Command {

  @inline def commandKind = command.commandKind

  private lazy val tupled = collection -> command

  override def hashCode: Int = tupled.hashCode

  @SuppressWarnings(Array("ComparingUnrelatedTypes"))
  override def equals(that: Any): Boolean = that match {
    case other: ResolvedCollectionCommand[_] =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  @inline override def toString: String =
    s"ResolvedCollectionCommand${tupled.toString}"
}
