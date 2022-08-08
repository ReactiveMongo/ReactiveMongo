package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{ Collation, SerializationPack, WriteConcern }
import reactivemongo.api.commands.FindAndModifyCommand

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 */
trait FindAndModifyOps[P <: SerializationPack] extends FindAndModifyCommand[P] {
  collection: GenericCollection[P] =>

  /**
   * @param writeConcern writeConcernParam
   */
  private[reactivemongo] final def prepareFindAndModify[S](
      selector: S,
      modifier: FindAndModifyOp,
      sort: Option[pack.Document],
      fields: Option[pack.Document],
      bypassDocumentValidation: Boolean,
      writeConcern: WriteConcern,
      maxTime: Option[FiniteDuration],
      collation: Option[Collation],
      arrayFilters: Seq[pack.Document]
    )(implicit
      swriter: pack.Writer[S]
    ): FindAndModifyBuilder[S] = new FindAndModifyBuilder[S](
    selector,
    modifier,
    sort,
    fields,
    bypassDocumentValidation,
    writeConcern,
    maxTime,
    collation,
    arrayFilters,
    swriter
  )

  /** Builder for findAndModify operations. */
  private[reactivemongo] final class FindAndModifyBuilder[S](
      val selector: S,
      val modifier: FindAndModifyOp,
      val sort: Option[pack.Document],
      val fields: Option[pack.Document],
      val bypassDocumentValidation: Boolean,
      val writeConcern: WriteConcern,
      val maxTime: Option[FiniteDuration],
      val collation: Option[Collation],
      val arrayFilters: Seq[pack.Document],
      swriter: pack.Writer[S]) {

    @inline def apply(
      )(implicit
        ec: ExecutionContext
      ): Future[FindAndModifyResult] = execute()

    // ---

    private def execute(
      )(implicit
        ec: ExecutionContext
      ): Future[FindAndModifyResult] = {
      val cmd = new FindAndModify(
        query = pack.serialize(selector, swriter),
        modifier = modifier,
        sort = sort,
        fields = fields,
        bypassDocumentValidation = bypassDocumentValidation,
        writeConcern = writeConcern,
        maxTimeMS = maxTime.flatMap { t =>
          val ms = t.toMillis

          if (ms < Int.MaxValue) {
            Some(ms.toInt)
          } else {
            Option.empty[Int]
          }
        },
        collation = collation,
        arrayFilters = arrayFilters
      )

      runCommand(cmd, writePreference)
    }
  }
}
