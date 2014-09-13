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


/*object Command {
  //private def
  import reactivemongo.api.collections.default.BSONCollection

  // generic commands
  def run[R, C <: Command](db: DB, command: C with CommandWithResult[R])
    (implicit writer: BSONDocumentWriter[C with CommandWithResult[R]], reader: BSONDocumentReader[R]): Future[R] = {
      db.collection[BSONCollection]("any").find(command).one
      /*val buffer = ChannelBufferWritableBuffer()
      BSONSerializationPack.serializeAndWrite(buffer, command, writer)
      BufferSequence(buffer)
      val buffer = write(merge, ChannelBufferWritableBuffer())*/
      ???
    }
    /*val op = Query(flags, collection.fullCollectionName, options.skipN, options.batchSizeN)

    new DefaultCursor(op, documents, readPreference, collection.db.connection, failover)

    Failover(RequestMaker(query, documents, readPreference), mongoConnection, failoverStrategy).future*/

  def run[R, C <: Command, P <: SerializationPack](db: DB, command: C with CommandWithResult[R] with CommandWithPack[P])
    (implicit writer: P#Writer[C], reader: P#Reader[R]): Future[R] = ???

  def run[P <: SerializationPack, C <: Command](pack: P)(db: DB, command: Command)
    (implicit writer: pack.Writer[C]): CursorFetcher[P, Cursor] = ???

  def runRaw[P <: SerializationPack, C <: Command](pack: P)(db: DB, command: Command)
    (implicit writer: pack.Writer[C]): CursorFetcher[P, Cursor] = ???

  def runRaw[C <: Command](db: DB, command: C)
    (implicit writer: BSONDocumentWriter[C]): CursorFetcher[BSONSerializationPack.type, Cursor] = ???

  // collection aware commands
  def run[R, C <: CollectionCommand with CommandWithResult[R]](collection: Collection, command: C with CommandWithResult[R])
    (implicit writer: BSONDocumentWriter[ResolvedCollectionCommand[C with CommandWithResult[R]]], reader: BSONDocumentReader[R]): Future[R] = {
      val comm = ResolvedCollectionCommand(collection.name, command)
      ???
    }

  def run[R, C <: CollectionCommand with CommandWithResult[R] with CommandWithPack[P], P <: SerializationPack]
    (collection: Collection, command: C with CommandWithResult[R] with CommandWithPack[P])
    (implicit writer: P#Writer[ResolvedCollectionCommand[C with CommandWithResult[R] with CommandWithPack[P]]], reader: P#Reader[R]): Future[R] = {
      val comm = ResolvedCollectionCommand(collection.name, command)
      ???
    }

  def run[C <: CollectionCommand with CommandWithPack[P], P <: SerializationPack]
    (collection: Collection, command: C with CommandWithPack[P])
    (implicit writer: P#Writer[ResolvedCollectionCommand[C with CommandWithPack[P]]]): CursorFetcher[P, Cursor] = {
      val comm = ResolvedCollectionCommand(collection.name, command)
      ???
    }
}*/

trait RawCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Raw(doc: ImplicitlyDocumentProducer) extends Command with CommandWithPack[P]
}

object Command {
  import reactivemongo.api.{ DefaultCursor, FailoverStrategy, ReadPreference }
  import reactivemongo.api.collections.{ BufferReader, BufferWriter }
  import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }
  import reactivemongo.core.netty._
  import reactivemongo.core.protocol.{ RequestMaker, Query, QueryFlags, Response }

  def defaultCursorFetcher[P <: SerializationPack, A](db: DB, p: P, command: A)(implicit writer: p.Writer[A]): CursorFetcher[p.type, Cursor] = new CursorFetcher[p.type, Cursor] {
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
      val failover = FailoverStrategy()
      new DefaultCursor(op, bs, ReadPreference.primary, db.connection, failover)//.headOption2
    }
  }

  case class CommandWithPackRunner[P <: SerializationPack](pack: P) {
    def apply[R, C <: Command with CommandWithResult[R]]
      (db: DB, command: C with CommandWithResult[R])
      (implicit writer: pack.Writer[C], reader: pack.Reader[R]): Future[R] =
        defaultCursorFetcher(db, pack, command).one

    def apply[R, C <: Command with CommandWithResult[R] with CommandWithPack[P]]
      (db: DB, command: C with CommandWithResult[R] with CommandWithPack[P])
      (implicit writer: pack.Writer[C], reader: pack.Reader[R], ev: C <:< Command with CommandWithResult[R] with CommandWithPack[P]): Future[R] =
        defaultCursorFetcher(db, pack, command).one

    def apply[C <: Command with CommandWithPack[P]]
      (db: DB, command: C with CommandWithPack[P])
      (implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] =
        defaultCursorFetcher(db, pack, command)

    // collection
    def apply[R, C <: CollectionCommand with CommandWithResult[R]]
      (collection: Collection, command: C with CommandWithResult[R])
      (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): Future[R] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command)).one[R]

    // boxed value
    def apply[A <: AnyVal, R <: AnyRef with BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]]
      (collection: Collection, command: C with CommandWithResult[R])
      (implicit ev: R <:< BoxedAnyVal[A], writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): Future[A] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command)).one[R].map(_.value)

    def apply[R, C <: CollectionCommand with CommandWithResult[R] with CommandWithPack[P]]
      (collection: Collection, command: C with CommandWithResult[R] with CommandWithPack[P])
      (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ev: C <:< CollectionCommand with CommandWithResult[R] with CommandWithPack[P]): Future[R] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command)).one[R]

    // boxed value 2
    def apply[A <: AnyVal, R <: AnyRef with BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R] with CommandWithPack[P]]
      (collection: Collection, command: C with CommandWithResult[R with BoxedAnyVal[A]] with CommandWithPack[P])
      (implicit ev1: R <:< BoxedAnyVal[A], writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ev: C <:< CollectionCommand with CommandWithResult[R] with CommandWithPack[P]): Future[A] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command)).one[R].map(_.value)

    def apply[C <: CollectionCommand with CommandWithPack[P]]
      (collection: Collection, command: C with CommandWithPack[P])
      (implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] =
        defaultCursorFetcher(collection.db, pack, ResolvedCollectionCommand(collection.name, command))
  }

  def run[P <: SerializationPack](pack: P): CommandWithPackRunner[P] = CommandWithPackRunner(pack)
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

  type WriteResult = LastError
  val WriteResult = LastError

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