package reactivemongo.api.commands

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NoStackTrace

import reactivemongo.api.{
  BSONSerializationPack,
  Cursor,
  SerializationPack,
  DB,
  Collection
}
import reactivemongo.bson.{ BSONDocumentReader, BSONDocumentWriter }

sealed trait AbstractCommand

trait Command extends AbstractCommand
trait CollectionCommand extends AbstractCommand

trait CommandWithResult[R] { self: AbstractCommand => }
trait CommandWithPack[P <: SerializationPack] { self: AbstractCommand => }
trait CursorCommand { self: AbstractCommand =>
  def needsCursor: Boolean
}

trait BoxedAnyVal[A <: AnyVal] {
  def value: A
}

trait CommandError extends Exception with NoStackTrace {
  def code: Option[Int]
  def errmsg: Option[String]

  override def getMessage = s"CommandError[code=${code.getOrElse("<unknown>")}, errmsg=${errmsg.getOrElse("<unknown>")}]"
}

// TODO: move to package `api`
trait CursorFetcher[P <: SerializationPack, C[A] <: Cursor[A]] {
  val pack: P
  def one[A](implicit reader: pack.Reader[A], ec: ExecutionContext): Future[A]
  def cursor[A](implicit reader: pack.Reader[A]): C[A]
}

trait ImplicitCommandHelpers[P <: SerializationPack] {
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
    FailoverStrategy,
    ReadPreference
  }
  import reactivemongo.core.actors.RequestMakerExpectingResponse
  import reactivemongo.bson.lowlevel.LoweLevelDocumentIterator
  import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }
  import reactivemongo.core.netty.{
    BufferSequence,
    ChannelBufferReadableBuffer,
    ChannelBufferWritableBuffer
  }
  import reactivemongo.core.protocol.{
    RequestMaker,
    Query,
    QueryFlags,
    Response
  }

  def defaultCursorFetcher[P <: SerializationPack, A](db: DB, p: P, command: A, failover: FailoverStrategy)(implicit writer: p.Writer[A]): CursorFetcher[p.type, Cursor] = new CursorFetcher[p.type, Cursor] {
    val pack: p.type = p

    @inline private def defaultReadPreference: ReadPreference =
      db.connection.options.readPreference

    def one[A](readPreference: ReadPreference)(implicit reader: pack.Reader[A], ec: ExecutionContext): Future[A] = {
      val (requestMaker, mongo26WriteCommand) =
        buildRequestMaker(pack)(command, writer, readPreference, db.name)

      Failover2(db.connection, failover) { () =>
        db.connection.sendExpectingResponse(
          requestMaker, mongo26WriteCommand).map { response =>
            pack.readAndDeserialize(
              LoweLevelDocumentIterator(ChannelBufferReadableBuffer(
                response.documents)).next, reader)

          }
      }.future
    }

    def one[A](implicit reader: pack.Reader[A], ec: ExecutionContext): Future[A] = one[A](defaultReadPreference)

    def cursor[A](readPreference: ReadPreference)(implicit reader: pack.Reader[A]): Cursor[A] = {
      val buffer = ChannelBufferWritableBuffer()
      pack.serializeAndWrite(buffer, command, writer)
      val bs = BufferSequence(buffer.buffer)
      val flags = if (readPreference.slaveOk) QueryFlags.SlaveOk else 0
      val op = Query(flags, db.name + ".$cmd", 0, 1)
      val mongo26WriteCommand = command match {
        case _: Mongo26WriteCommand => true
        case _                      => false
      }
      DefaultCursor(pack, op, bs, if (mongo26WriteCommand) ReadPreference.primary else readPreference, db.connection, failover, mongo26WriteCommand)
    }

    def cursor[A](implicit reader: pack.Reader[A]): Cursor[A] = cursor(defaultReadPreference)
  }

  case class CommandWithPackRunner[P <: SerializationPack](pack: P, failover: FailoverStrategy = FailoverStrategy()) {
    // database
    def apply[R, C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] =
      defaultCursorFetcher(db, pack, command, failover).one[R]

    def apply[C <: Command](db: DB, command: C)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] =
      defaultCursorFetcher(db, pack, command, failover)

    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](db: DB, command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] =
      defaultCursorFetcher(db, pack, command, failover).one[R].map(_.value)

    // collection
    def apply[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] =
      defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R]

    def apply[C <: CollectionCommand](collection: Collection, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] =
      defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover)

    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] =
      defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R].map(_.value)

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
  def run[P <: SerializationPack](pack: P): CommandWithPackRunner[pack.type] =
    CommandWithPackRunner(pack)

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
