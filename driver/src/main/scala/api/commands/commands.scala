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
import reactivemongo.core.errors.ReactiveMongoException

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

  @deprecated("Use the alternative with `ReadPreference`", "0.12.0")
  def one[A](implicit reader: pack.Reader[A], ec: ExecutionContext): Future[A] = one[A](defaultReadPreference)

  def cursor[A](readPreference: ReadPreference)(implicit reader: pack.Reader[A]): C[A]

  @deprecated("Use the alternative with `ReadPreference`", "0.12.0")
  def cursor[A](implicit reader: pack.Reader[A]): C[A] =
    cursor(defaultReadPreference)

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
  import reactivemongo.core.actors.RequestMakerExpectingResponse
  import reactivemongo.bson.lowlevel.LoweLevelDocumentIterator
  import reactivemongo.core.netty.{
    BufferSequence,
    ChannelBufferReadableBuffer,
    ChannelBufferWritableBuffer
  }
  import reactivemongo.core.protocol.{
    RequestMaker,
    Query,
    QueryFlags
  }

  def defaultCursorFetcher[P <: SerializationPack, A](db: DB, p: P, command: A, failover: FailoverStrategy)(implicit writer: p.Writer[A]): CursorFetcher[p.type, DefaultCursor.Impl] = new CursorFetcher[p.type, DefaultCursor.Impl] {
    val pack: p.type = p

    protected def defaultReadPreference = db.connection.options.readPreference

    def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = {
      val (requestMaker, m26WriteCommand) =
        buildRequestMaker(pack)(command, writer, readPreference, db.name)

      Failover2(db.connection, failover) { () =>
        db.connection.sendExpectingResponse(requestMaker, m26WriteCommand)
      }.future.map { response =>
        pack.readAndDeserialize(
          LoweLevelDocumentIterator(ChannelBufferReadableBuffer(
            response.documents)).next, reader)
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

      DefaultCursor.query(pack, op, bs,
        if (mongo26WriteCommand) ReadPreference.primary else readPreference,
        db.connection, failover, mongo26WriteCommand)

    }
  }

  case class CommandWithPackRunner[P <: SerializationPack](pack: P, failover: FailoverStrategy = FailoverStrategy()) {
    // database
    @deprecated("Use alternative with `ReadPreference`", "0.12.0")
    def apply[R, C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = apply[R, C](db, command, ReadPreference.primary)

    def apply[R, C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(db, pack, command, failover).one[R](rp)

    def apply[C <: Command](db: DB, command: C)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(db, pack, command, failover)

    @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = unboxed[A, R, C](db, command, ReadPreference.primary)

    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R with BoxedAnyVal[A]], rp: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = defaultCursorFetcher(db, pack, command, failover).one[R](rp).map(_.value)

    @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = unboxed[A, R, C](collection, command, ReadPreference.primary)

    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R with BoxedAnyVal[A]], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] =
      defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R](rp).map(_.value)

    // collection
    def apply[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R](rp)

    @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
    def apply[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = apply[R, C](collection, command, ReadPreference.primary)

    def apply[C <: CollectionCommand](collection: Collection, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover)

    /**
     * Executes the `command` and returns its result
     * along with the MongoDB response.
     */
    def withResponse[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C, rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = {
      val cursor = defaultCursorFetcher(collection.db, pack,
        ResolvedCollectionCommand(collection.name, command), failover).
        cursor[R](rp)

      for {
        response <- cursor.makeRequest(cursor.numberToReturn)
        iterator = cursor.documentIterator(response)
        result <- {
          if (!iterator.hasNext) Future.failed(ReactiveMongoException("missing result"))
          else Future.successful(iterator.next())
        }
      } yield ResponseResult(response, cursor.numberToReturn, result)
    }

    @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
    def withResponse[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = withResponse[R, C](collection, command)

    def rawCommand[T](input: T)(implicit writer: pack.Writer[T]): RawCommand =
      RawCommand(pack.serialize(input, writer))

    case class RawCommand(document: pack.Document) extends Command

    object RawCommand {
      implicit val writer: pack.Writer[RawCommand] = pack.writer(_.document)
    }
  }

  @deprecated(message = "Use `run` with the `failoverStrategy` parameter", since = "0.12-RC0")
  def run[P <: SerializationPack](pack: P): CommandWithPackRunner[pack.type] =
    CommandWithPackRunner(pack, FailoverStrategy())

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

  private[reactivemongo] def deserialize[P <: SerializationPack, A](pack: P, response: Response)(implicit reader: pack.Reader[A]): A =
    pack.readAndDeserialize(response, reader)

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

  private[reactivemongo] case class CommandWithPackMaker[P <: SerializationPack](pack: P) {
    def apply[C <: Command](db: DB, command: C, readPreference: ReadPreference)(implicit writer: pack.Writer[C]): RequestMakerExpectingResponse =
      onDatabase(db.name, command, readPreference)

    def apply[C <: Command with Mongo26WriteCommand](db: DB, command: C)(implicit writer: pack.Writer[C]): RequestMakerExpectingResponse =
      onDatabase(db.name, command)

    def apply[C <: CollectionCommand](collection: Collection, command: C, readPreference: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): RequestMakerExpectingResponse =
      onCollection(collection.db.name, collection.name, command, readPreference)

    def apply[C <: CollectionCommand with Mongo26WriteCommand](collection: Collection, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): RequestMakerExpectingResponse =
      onCollection(collection.db.name, collection.name, command)

    def onDatabase[C <: Command](db: String, command: C, readPreference: ReadPreference)(implicit writer: pack.Writer[C]): RequestMakerExpectingResponse = {
      val (requestMaker, mongo26WriteCommand) = buildRequestMaker(pack)(command, writer, readPreference, db)
      RequestMakerExpectingResponse(requestMaker, mongo26WriteCommand)
    }

    def onDatabase[C <: Command with Mongo26WriteCommand](db: String, command: C)(implicit writer: pack.Writer[C]): RequestMakerExpectingResponse = {
      val requestMaker = buildRequestMaker(pack)(command, writer, ReadPreference.primary, db)._1
      RequestMakerExpectingResponse(requestMaker, true)
    }

    def onCollection[C <: CollectionCommand](db: String, collection: String, command: C, readPreference: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): RequestMakerExpectingResponse = {
      val (requestMaker, mongo26WriteCommand) = buildRequestMaker(pack)(ResolvedCollectionCommand(collection, command), writer, readPreference, db)
      RequestMakerExpectingResponse(requestMaker, mongo26WriteCommand)
    }

    def onCollection[C <: CollectionCommand with Mongo26WriteCommand](db: String, collection: String, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): RequestMakerExpectingResponse = {
      val requestMaker = buildRequestMaker(pack)(ResolvedCollectionCommand(collection, command), writer, ReadPreference.primary, db)._1
      RequestMakerExpectingResponse(requestMaker, true)
    }
  }

  private[reactivemongo] def requestMaker[P <: SerializationPack](pack: P): CommandWithPackMaker[P] = CommandWithPackMaker(pack)
}

/**
 * @param collection the name of the collection against which the command is executed
 * @param command the executed command
 */
final case class ResolvedCollectionCommand[C <: CollectionCommand](
  collection: String,
  command: C) extends Command

object `package` {
  type WriteConcern = GetLastError
  val WriteConcern = GetLastError
}
