package reactivemongo.api.commands

import concurrent.{ ExecutionContext, Future }
import ExecutionContext.Implicits.global
import reactivemongo.api.{ BSONSerializationPack, Cursor, SerializationPack, DB, Collection }
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

trait CursorFetcher[P <: SerializationPack, C[A] <: Cursor[A]] {
  val pack: P
  def one[A](implicit reader: pack.Reader[A]): Future[A]
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

trait RawCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Raw(doc: ImplicitlyDocumentProducer) extends Command with CommandWithPack[P]
}

object Command {
  import reactivemongo.api.{ DefaultCursor, FailoverStrategy, ReadPreference }
  import reactivemongo.core.actors.RequestMakerExpectingResponse
  import reactivemongo.api.collections.{ BufferReader, BufferWriter }
  import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }
  import reactivemongo.core.netty._
  import reactivemongo.core.protocol.{ RequestMaker, Query, QueryFlags, Response }

  def defaultCursorFetcher[P <: SerializationPack, A](db: DB, p: P, command: A, failover: FailoverStrategy)(implicit writer: p.Writer[A]): CursorFetcher[p.type, Cursor] = new CursorFetcher[p.type, Cursor] {
    val pack: p.type = p
    private implicit def bufferWriter[A](implicit w: pack.Writer[A]) = new BufferWriter[A] {
      def write[B <: WritableBuffer](document: A, buffer: B): B =
        pack.serializeAndWrite(buffer, document, w).asInstanceOf[B] // TODO !!!!!!!!!
    }
    private implicit def bufferReader[A](implicit r: pack.Reader[A]) = new BufferReader[A] {
      def read(buffer: ReadableBuffer): A =
        pack.readAndDeserialize(buffer, r)
    }
    def one[A](implicit reader: pack.Reader[A]): Future[A] = cursor.collect[Iterable](1, true).map(_.head)
    def cursor[A](implicit reader: pack.Reader[A]): Cursor[A] = {
      val buffer = ChannelBufferWritableBuffer()
      pack.serializeAndWrite(buffer, command, writer)
      val bs = BufferSequence(buffer.buffer)
      val op = Query(0, db.name + ".$cmd", 0, 1)
      val mongo26WriteCommand = command match {
        case _: Mongo26WriteCommand => true
        case _ => false
      }
      // TODO customize ReadPreference
      DefaultCursor(pack, op, bs, ReadPreference.primary, db.connection, failover, mongo26WriteCommand)
    }
  }

  def deserialize[P <: SerializationPack, A](pack: P, response: Response)(implicit reader: pack.Reader[A]): A = {
    pack.readAndDeserialize(response, reader)
  }

  case class CommandWithPackRunner[P <: SerializationPack](pack: P, failover: FailoverStrategy = FailoverStrategy()) {
    def apply[R, C <: Command with CommandWithResult[R]]
      (db: DB, command: C with CommandWithResult[R])
      (implicit writer: pack.Writer[C], reader: pack.Reader[R]): Future[R] =
        defaultCursorFetcher(db, pack, command, failover).one

    def apply[R, C <: Command with CommandWithResult[R] with CommandWithPack[P]]
      (db: DB, command: C with CommandWithResult[R] with CommandWithPack[P])
      (implicit writer: pack.Writer[C], reader: pack.Reader[R], ev: C <:< Command with CommandWithResult[R] with CommandWithPack[P]): Future[R] =
        defaultCursorFetcher(db, pack, command, failover).one

    def apply[C <: Command with CommandWithPack[P]]
      (db: DB, command: C with CommandWithPack[P])
      (implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] =
        defaultCursorFetcher(db, pack, command, failover)

    // collection
    def unboxed[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]]
      (collection: Collection, command: C with CommandWithResult[R with BoxedAnyVal[A]])
      (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): Future[A] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R].map(_.value)

    def apply[R, C <: CollectionCommand with CommandWithResult[R]]
      (collection: Collection, command: C with CommandWithResult[R])
      (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): Future[R] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover).one[R]

    def apply[C <: CollectionCommand]
      (collection: Collection, command: C)
      (implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command), failover)
  }

  def run[P <: SerializationPack](pack: P): CommandWithPackRunner[pack.type] = CommandWithPackRunner(pack)


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
      val buffer = ChannelBufferWritableBuffer()
      pack.serializeAndWrite(buffer, command, writer)
      val documents = BufferSequence(buffer.buffer)
      val query = Query(0, db + ".$cmd", 0, 1)
      val mongo26WriteCommand = command match {
        case _: Mongo26WriteCommand => true
        case _ => false
      }
      RequestMakerExpectingResponse(RequestMaker(query, documents, readPreference), mongo26WriteCommand)
    }
    def onDatabase[C <: Command with Mongo26WriteCommand](db: String, command: C)(implicit writer: pack.Writer[C]): RequestMakerExpectingResponse = {
      val buffer = ChannelBufferWritableBuffer()
      pack.serializeAndWrite(buffer, command, writer)
      val documents = BufferSequence(buffer.buffer)
      val query = Query(0, db + ".$cmd", 0, 1)
      RequestMakerExpectingResponse(RequestMaker(query, documents, ReadPreference.primary), true)
    }
    def onCollection[C <: CollectionCommand](db: String, collection: String, command: C, readPreference: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): RequestMakerExpectingResponse = {
      val buffer = ChannelBufferWritableBuffer()
      pack.serializeAndWrite(buffer, ResolvedCollectionCommand(collection, command), writer)
      val documents = BufferSequence(buffer.buffer)
      val query = Query(0, db + ".$cmd", 0, 1)
      val mongo26WriteCommand = command match {
        case _: Mongo26WriteCommand => true
        case _ => false
      }
      RequestMakerExpectingResponse(RequestMaker(query, documents, readPreference), mongo26WriteCommand)
    }
    def onCollection[C <: CollectionCommand with Mongo26WriteCommand](db: String, collection: String, command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): RequestMakerExpectingResponse = {
      val buffer = ChannelBufferWritableBuffer()
      pack.serializeAndWrite(buffer, ResolvedCollectionCommand(collection, command), writer)
      val documents = BufferSequence(buffer.buffer)
      val query = Query(0, db + ".$cmd", 0, 1)
      RequestMakerExpectingResponse(RequestMaker(query, documents, ReadPreference.primary), true)
    }
  }

  private[reactivemongo] def requestMaker[P <: SerializationPack](pack: P): CommandWithPackMaker[P] =
    CommandWithPackMaker(pack)
}

final case class ResolvedCollectionCommand[
  C <: CollectionCommand
] (
  collection: String,
  command: C
) extends Command


object `package` {
  /*implicit def resolvedCollectionCommand[
    P <: SerializationPack,
    R,
    C <: CollectionCommand with CommandWithResult[R] with CommandWithPack[P]](implicit writer: P#Writer[C]):
    ResolvedCollectionCommandWithPackAndResult[P, R, C] = ???

  implicit def resolvedCollectionCommand[
    P <: SerializationPack,
    C <: CollectionCommand with CommandWithPack[P]](implicit writer: P#Writer[C]):
    ResolvedCollectionCommandWithPack[P, C] = ???*/

  type FullCollectionCommand[C <: CollectionCommand] = (String, C)

  type WriteConcern = GetLastError
  val WriteConcern = GetLastError

  type SerializationPackObject = SerializationPack with Singleton

  object test {
    import reactivemongo.bson._
    import BSONFindAndModify._
    val db: DB = ???
    val collection: Collection = ???
    val fam = BSONFindAndModify(BSONDocument("hey" -> "hey"), true)
    val mmm = Command.run(BSONSerializationPack)(collection, fam)
    //mmm : Short
    //mmm.map(_.result[BSONDocument]) : String
    //mmm:String
  }
}