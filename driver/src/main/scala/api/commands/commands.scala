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

trait CursorFetcher[P <: SerializationPack, C[A] <: Cursor[A]] {
  def one[A](implicit reader: P#Reader[A]): Future[A]
  def cursor[A](implicit reader: P#Reader[A]): C[A]
}

object Command {
  // generic commands
  def run[R, C <: Command](db: DB, command: C with CommandWithResult[R])
    (implicit writer: BSONDocumentWriter[C], reader: BSONDocumentReader[R]): Future[R] = ???

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
    (implicit writer: BSONDocumentWriter[ResolvedCollectionCommand[C]], reader: BSONDocumentReader[R]): Future[R] = {
      val comm = ResolvedCollectionCommand(collection.name, command)
      ???
    }

  def run[R, C <: CollectionCommand with CommandWithResult[R] with CommandWithPack[P], P <: SerializationPack]
    (collection: Collection, command: C with CommandWithResult[R] with CommandWithPack[P])
    (implicit writer: P#Writer[ResolvedCollectionCommand[C]], reader: P#Reader[R]): Future[R] = {
      val comm = ResolvedCollectionCommand(collection.name, command)
      ???
    }

  def run[C <: CollectionCommand with CommandWithPack[P], P <: SerializationPack]
    (collection: Collection, command: C with CommandWithPack[P])
    (implicit writer: P#Writer[ResolvedCollectionCommand[C]]): CursorFetcher[P, Cursor] = {
      val comm = ResolvedCollectionCommand(collection.name, command)
      ???
    }
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


  object test {
    import reactivemongo.bson._
    import BSONFindAndModify._
    val db: DB = ???
    val collection: Collection = ???
    val fam = BSONFindAndModify(BSONDocument("hey" -> "hey"), true)
    val mmm = Command.run(collection, fam)
    //mmm.map(_.result[BSONDocument]) : String
    //mmm:String
  }
}