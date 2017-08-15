package reactivemongo.api.commands

import scala.language.higherKinds

import scala.util.{ Failure, Success, Try }

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

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

  /**
   * @param values the raw values (should not contain duplicate)
   */
  case class DistinctResult(values: Traversable[pack.Value]) {
    @annotation.tailrec
    private def result[T, M[_]](values: Traversable[pack.Value], reader: pack.WidenValueReader[T], out: Builder[T, M[T]]): Try[M[T]] =
      values.headOption match {
        case Some(t) => pack.readValue(t, reader) match {
          case Failure(e) => Failure(e)
          case Success(v) => result(values.tail, reader, out += v)
        }
        case _ => Success(out.result())
      }

    def result[T, M[_] <: Iterable[_]](implicit reader: pack.WidenValueReader[T], cbf: CanBuildFrom[M[_], T, M[T]]): Try[M[T]] = result(values, reader, cbf())
  }
}
