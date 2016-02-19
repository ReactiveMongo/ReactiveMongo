package reactivemongo.api.commands

import scala.util.{ Failure, Success, Try }

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
    readConcern: ReadConcern = ReadConcern.Local) extends CollectionCommand
      with CommandWithPack[pack.type] with CommandWithResult[DistinctResult]

  case class DistinctResult(values: List[pack.Value]) {
    @annotation.tailrec
    private def result[T](list: List[Try[T]], out: List[T]): Try[List[T]] =
      list match {
        case Failure(e) :: _  => Failure(e)
        case Success(v) :: vs => result(vs, v :: out)
        case _                => Success(out.reverse)
      }

    def result[T](implicit reader: pack.WidenValueReader[T]): Try[List[T]] =
      result(values.map(v => pack.readValue(v, reader)), Nil)
  }
}
