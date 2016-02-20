package reactivemongo.api.commands

import scala.util.{ Failure, Success, Try }

import scala.collection.immutable.ListSet

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.api.{ ReadConcern, SerializationPack }

/** The [[https://docs.mongodb.org/manual/reference/command/distinct/ distinct]] command. */
trait DistinctCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  /**
   * @param keyString the field for which to return distinct values
   * @param query the query that specifies the documents from which to retrieve the distinct values
   * @param readConcern the [[https://docs.mongodb.org/manual/reference/glossary/#term-read-concern read concern]] (default `local`)
   */
  case class Distinct(
    keyString: String,
    query: Option[pack.Document],
    readConcern: ReadConcern = ReadConcern.Local,
    version: MongoWireVersion = MongoWireVersion.V30) extends CollectionCommand
      with CommandWithPack[pack.type] with CommandWithResult[DistinctResult]

  case class DistinctResult(values: ListSet[pack.Value]) {
    @annotation.tailrec
    private def result[T](values: ListSet[pack.Value], reader: pack.WidenValueReader[T], out: ListSet[T]): Try[ListSet[T]] = values.headOption match {
      case Some(t) => pack.readValue(t, reader) match {
        case Failure(e) => Failure(e)
        case Success(v) => result(values.tail, reader, out + v)
      }
      case _ => Success(out)
    }

    def result[T](implicit reader: pack.WidenValueReader[T]): Try[ListSet[T]] =
      result(values, reader, ListSet.empty)
  }
}
