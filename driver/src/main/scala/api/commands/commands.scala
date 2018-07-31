package reactivemongo.api.commands

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{
  Cursor,
  Collection,
  DB,
  ReadPreference,
  SerializationPack
}

import reactivemongo.core.protocol.Response
import reactivemongo.core.actors.RequestMakerExpectingResponse
import reactivemongo.core.errors.ReactiveMongoException

@deprecated("Will be removed; See `Command`", "0.16.0")
sealed trait AbstractCommand

trait Command extends AbstractCommand
trait CollectionCommand extends AbstractCommand

trait CommandWithResult[R] { self: AbstractCommand => }
trait CommandWithPack[P <: SerializationPack] { self: AbstractCommand => }

trait BoxedAnyVal[A <: AnyVal] {
  def value: A
}

/**
 * @param response the response associated with the result
 * @param numberToReturn the number of documents to return
 * @param value the value parsed from the response
 */
case class ResponseResult[R](
  response: Response,
  numberToReturn: Int,
  value: R)

import scala.language.higherKinds

/**
 * Fetches a cursor from MongoDB results.
 * @tparam P the type of the serialization pack
 * @tparam C the type of the cursor implementation
 */
trait CursorFetcher[P <: SerializationPack, +C[_] <: Cursor[_]] {
  val pack: P

  def one[A](readPreference: ReadPreference)(implicit reader: pack.Reader[A], ec: ExecutionContext): Future[A]

  def cursor[A](readPreference: ReadPreference)(implicit reader: pack.Reader[A]): C[A]

  protected def defaultReadPreference: ReadPreference
}

/**
 * @param cursorId the ID of the cursor
 * @param fullCollectionName the namespace of the collection
 */
case class ResultCursor(cursorId: Long, fullCollectionName: String)

trait ImplicitCommandHelpers[P <: SerializationPack] {
  import scala.language.implicitConversions

  val pack: P

  trait ImplicitlyDocumentProducer {
    def produce: pack.Document
  }

  object ImplicitlyDocumentProducer {
    implicit def producer[A](a: A)(implicit writer: pack.Writer[A]): ImplicitlyDocumentProducer = new ImplicitlyDocumentProducer {
      def produce = pack.serialize(a, writer)
    }
  }
}

object UnitBox extends BoxedAnyVal[Unit] {
  def value: Unit = ()
}

object Command {
  import reactivemongo.api.{
    DefaultCursor,
    Failover2,
    FailoverStrategy
  }
  import reactivemongo.core.netty.{
    BufferSequence,
    ChannelBufferWritableBuffer
  }
  import reactivemongo.core.protocol.{
    Query,
    QueryFlags,
    RequestMaker,
    Response
  }

  private[commands] lazy val logger =
    reactivemongo.util.LazyLogger("reactivemongo.api.commands")

  def defaultCursorFetcher[P <: SerializationPack, A](db: DB, p: P, command: A, failover: FailoverStrategy)(implicit writer: p.Writer[A]): CursorFetcher[p.type, DefaultCursor.Impl] = fetchCursor[p.type, A](db, db.name + ".$cmd", p, command, failover)

  /**
   * @param fullCollectionName the fully qualified collection name (even if `query.fullCollectionName` is `\$cmd`)
   */
  private[reactivemongo] def fetchCursor[P <: SerializationPack, A](db: DB, fullCollectionName: String, p: P, command: A, failover: FailoverStrategy)(implicit writer: p.Writer[A]): CursorFetcher[p.type, DefaultCursor.Impl] = new CursorFetcher[p.type, DefaultCursor.Impl] with CommandCodecs[p.type] {
    val pack: p.type = p

    protected def defaultReadPreference = db.connection.options.readPreference

    def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = {
      val (requestMaker, m26WriteCommand) =
        buildRequestMaker(pack)(command, writer, readPreference, db.name)

      Failover2(db.connection, failover) { () =>
        db.connection.sendExpectingResponse(
          RequestMakerExpectingResponse(requestMaker, m26WriteCommand))

      }.future.flatMap {
        case Response.CommandError(_, _, _, cause) =>
          cause.originalDocument match {
            case Some(doc) =>
              Future(pack.deserialize(pack.document(doc), reader))

            case _ => Future.failed[T](cause)
          }

        case response => Future(pack.readAndDeserialize(response, reader))
      }
    }

    def cursor[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T]): DefaultCursor.Impl[T] = {
      val buffer = ChannelBufferWritableBuffer()
      pack.serializeAndWrite(buffer, command, writer)

      val bs = BufferSequence(buffer.buffer)
      val flags = if (readPreference.slaveOk) QueryFlags.SlaveOk else 0
      val op = Query(flags, db.name + ".$cmd", 0, 1)
      val mongo26WriteCommand = command match {
        case _: Mongo26WriteCommand => true
        case _                      => false
      }

      DefaultCursor.query(pack, op, (_: Int) /*TODO: max?*/ => bs,
        if (mongo26WriteCommand) ReadPreference.primary else readPreference,
        db, failover, mongo26WriteCommand, fullCollectionName)

    }
  }

  case class CommandWithPackRunner[P <: SerializationPack](pack: P, failover: FailoverStrategy = FailoverStrategy()) {
    def apply[R, C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(db, pack, command, failover).one[R](rp)

    def apply[C <: Command](db: DB, command: C)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(db, pack, command, failover)

    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R with BoxedAnyVal[A]], rp: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = defaultCursorFetcher(db, pack, command, failover).one[R](rp).map(_.value)

    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R with BoxedAnyVal[A]], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] =
      defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R](rp).map(_.value)

    // collection
    def apply[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R](rp)

    def apply[C <: CollectionCommand](collection: Collection, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover)

    /**
     * Executes the `command` and returns its result
     * along with the MongoDB response.
     */
    private[reactivemongo] def cursor[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C, rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): DefaultCursor.Impl[R] = fetchCursor(
      collection.db, collection.fullCollectionName, pack,
      ResolvedCollectionCommand(collection.name, command), failover).
      cursor[R](rp)

    /**
     * Executes the `command` and returns its result
     * along with the MongoDB response.
     */
    def withResponse[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C, rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = {
      val cursor = defaultCursorFetcher(collection.db, pack,
        ResolvedCollectionCommand(collection.name, command), failover).
        cursor[R](rp)

      for {
        resp <- cursor.makeRequest(cursor.numberToReturn)
        iterator = cursor.documentIterator(resp)
        result <- {
          if (!iterator.hasNext) {
            Future.failed(ReactiveMongoException("missing result"))
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

  /**
   * Returns a command runner.
   *
   * @param pack the serialization pack
   * @param failover the failover strategy
   *
   * {{{
   * import reactivemongo.bson.BSONDocument
   * import reactivemongo.api.BSONSerializationPack
   * import reactivemongo.api.commands.{ Command, Count }
   *
   * Command.run(BSONSerializationPack).
   *   unboxed(aCollection, Count(BSONDocument("bulk" -> true)))
   * }}}
   */
  def run[P <: SerializationPack](pack: P, failover: FailoverStrategy): CommandWithPackRunner[pack.type] = CommandWithPackRunner(pack, failover)

  private[reactivemongo] def buildRequestMaker[P <: SerializationPack, A](pack: P)(command: A, writer: pack.Writer[A], readPreference: ReadPreference, db: String): (RequestMaker, Boolean) = {
    val buffer = ChannelBufferWritableBuffer()

    pack.serializeAndWrite(buffer, command, writer)

    val documents = BufferSequence(buffer.buffer)
    val flags = if (readPreference.slaveOk) QueryFlags.SlaveOk else 0
    val query = Query(flags, db + ".$cmd", 0, 1)
    val mongo26WriteCommand = command match {
      case _: Mongo26WriteCommand => true
      case _                      => false
    }

    RequestMaker(query, documents, readPreference) -> mongo26WriteCommand
  }
}

/**
 * @param collection the name of the collection against which the command is executed
 * @param command the executed command
 */
final case class ResolvedCollectionCommand[C <: CollectionCommand](
  collection: String,
  command: C) extends Command

@deprecated(message = "", since = "0.12.7")
trait Mongo26WriteCommand

object `package` {
  type WriteConcern = GetLastError
  val WriteConcern = GetLastError
}
