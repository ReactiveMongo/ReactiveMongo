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

import reactivemongo.core.protocol.{ Reply, Response }
import reactivemongo.core.actors.ExpectingResponse
import reactivemongo.core.errors.GenericDriverException

trait Command
trait CollectionCommand extends Command

trait CommandWithResult[R] { _: Command => }
trait CommandWithPack[P <: SerializationPack] { _: Command => }

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
    Query,
    QueryFlags,
    RequestMaker,
    Response
  }

  private[commands] lazy val logger =
    reactivemongo.util.LazyLogger("reactivemongo.api.commands")

  def defaultCursorFetcher[P <: SerializationPack, A](db: DB, p: P, command: A, failover: FailoverStrategy)(implicit writer: p.Writer[A]): CursorFetcher[p.type, DefaultCursor.Impl] = fetchCursor[p.type, A](db, db.name + f".$$cmd", p, command, failover, CursorOptions.empty, maxTimeMS = None)

  /**
   * @param fullCollectionName the fully qualified collection name (even if `query.fullCollectionName` is `\$cmd`)
   */
  def fetchCursor[P <: SerializationPack, A](
    db: DB,
    fullCollectionName: String,
    p: P,
    command: A,
    failover: FailoverStrategy,
    options: CursorOptions,
    maxTimeMS: Option[Long])(implicit writer: p.Writer[A]): CursorFetcher[p.type, DefaultCursor.Impl] = new CursorFetcher[p.type, DefaultCursor.Impl] {
    val pack: p.type = p

    protected def defaultReadPreference = db.connection.options.readPreference

    /* TODO: Static binding to collect traces
    @inline private def stackTrace() =
      new Throwable().getStackTrace().drop(3).reverse
     */

    def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = {
      val requestMaker = buildRequestMaker(pack)(
        command, writer, readPreference, db.name)

      /* TODO: Static binding
      val contextSTE = stackTrace() */

      Failover(db.connection, failover) { () =>
        db.connection.sendExpectingResponse(new ExpectingResponse(
          requestMaker = requestMaker,
          pinnedNode = for {
            s <- db.session
            t <- s.transaction.toOption
            n <- t.pinnedNode
          } yield n))
      }.future /* TODO: Static binding; .recoverWith {
        case cause => Future.failed[Response] {
          cause.setStackTrace(contextSTE)
          cause
        }
      }*/ .flatMap {
          case Response.CommandError(_, _, _, cause) =>
            cause.originalDocument match {
              case pack.IsDocument(doc) =>
                // Error document as result
                Future(pack.deserialize(doc, reader))

              case _ => Future.failed[T] {
                /* TODO: Static binding
                cause.setStackTrace(contextSTE) */
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
      val buffer = WritableBuffer.empty
      pack.serializeAndWrite(buffer, command, writer)

      val bs = BufferSequence(buffer.buffer)
      val flags = {
        if (readPreference.slaveOk) options.slaveOk.flags
        else options.flags
      }

      val op = Query(flags, db.name + f".$$cmd", 0, 1)

      DefaultCursor.query(pack, op, (_: Int) => bs,
        readPreference, db, failover, fullCollectionName, maxTimeMS)

    }
  }

  final class CommandWithPackRunner[P <: SerializationPack](val pack: P, failover: FailoverStrategy = FailoverStrategy()) {
    def apply[R, C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(db, pack, command, failover).one[R](rp)

    def apply[C <: Command](db: DB, command: C)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(db, pack, command, failover)

    // collection
    def apply[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(collection.db, pack, new ResolvedCollectionCommand(collection.name, command), failover).one[R](rp)

    def apply[C <: CollectionCommand](collection: Collection, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(collection.db, pack, new ResolvedCollectionCommand(collection.name, command), failover)

    /**
     * Executes the `command` and returns its result
     * along with the MongoDB response.
     */
    def cursor[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C, options: CursorOptions, rp: ReadPreference, maxTimeMS: Option[Long])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): DefaultCursor.Impl[R] = fetchCursor(
      collection.db, collection.fullCollectionName, pack,
      new ResolvedCollectionCommand(collection.name, command),
      failover, options, maxTimeMS).
      cursor[R](rp)

    /**
     * Executes the `command` and returns its result
     * along with the MongoDB response.
     */
    def withResponse[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C, rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = {
      val cursor = defaultCursorFetcher(collection.db, pack,
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

    case class RawCommand(document: pack.Document) extends Command

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

  def buildRequestMaker[P <: SerializationPack, A](pack: P)(command: A, writer: pack.Writer[A], readPreference: ReadPreference, db: String): RequestMaker = {
    val buffer = WritableBuffer.empty

    pack.serializeAndWrite(buffer, command, writer)

    val documents = BufferSequence(buffer.buffer)
    val flags = if (readPreference.slaveOk) QueryFlags.SlaveOk else 0
    val query = Query(flags, db + f".$$cmd", 0, 1)

    RequestMaker(query, documents, readPreference)
  }
}

/**
 * @param collection the name of the collection against which the command is executed
 * @param command the executed command
 */
final class ResolvedCollectionCommand[C <: CollectionCommand](
  val collection: String,
  val command: C) extends Command {

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
