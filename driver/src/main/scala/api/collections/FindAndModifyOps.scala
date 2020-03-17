package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{ Collation, SerializationPack }
import reactivemongo.api.commands.{
  CommandCodecs,
  FindAndModifyCommand => FNM,
  ResolvedCollectionCommand,
  WriteConcern
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 */
private[api] trait FindAndModifyOps[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] =>

  private[reactivemongo] object FindAndModifyCommand
    extends FNM[collection.pack.type] {
    val pack: collection.pack.type = collection.pack
  }

  private type FindAndModifyResult = FNM.Result[pack.type]

  /**
   * @param writeConcern writeConcernParam
   */
  private[reactivemongo] final def prepareFindAndModify[S](
    selector: S,
    modifier: FNM.ModifyOp,
    sort: Option[pack.Document],
    fields: Option[pack.Document],
    bypassDocumentValidation: Boolean,
    writeConcern: WriteConcern,
    maxTime: Option[FiniteDuration],
    collation: Option[Collation],
    arrayFilters: Seq[pack.Document])(implicit swriter: pack.Writer[S]): FindAndModifyBuilder[S] = new FindAndModifyBuilder[S](selector, modifier, sort, fields, bypassDocumentValidation, writeConcern, maxTime, collation, arrayFilters, swriter)

  private type FindAndModifyCmd = ResolvedCollectionCommand[FindAndModifyCommand.FindAndModify]

  implicit private lazy val findAndModifyWriter: pack.Writer[FindAndModifyCmd] = {

    val underlying = FNM.writer[pack.type](pack)(
      wireVer = db.connectionState.metadata.maxWireVersion,
      context = FindAndModifyCommand)(collection.db.session)

    pack.writer[FindAndModifyCmd](underlying)
  }

  /** Builder for findAndModify operations. */
  private[reactivemongo] final class FindAndModifyBuilder[S](
    val selector: S,
    val modifier: FNM.ModifyOp,
    val sort: Option[pack.Document],
    val fields: Option[pack.Document],
    val bypassDocumentValidation: Boolean,
    val writeConcern: WriteConcern,
    val maxTime: Option[FiniteDuration],
    val collation: Option[Collation],
    val arrayFilters: Seq[pack.Document],
    swriter: pack.Writer[S]) {

    @inline final def apply()(implicit ec: ExecutionContext): Future[FindAndModifyResult] = execute()

    // ---

    implicit private val resultReader: pack.Reader[FNM.Result[pack.type]] = {
      val decoder: SerializationPack.Decoder[pack.type] = pack.newDecoder

      CommandCodecs.dealingWithGenericCommandErrorsReader(pack) { result =>
        FNM.Result[pack.type](pack)(
          decoder.child(result, "lastErrorObject").map { doc =>
            FNM.UpdateLastError[pack.type](pack)(
              decoder.booleanLike(
                doc, "updatedExisting").getOrElse(false),
              decoder.get(doc, "upserted"),
              decoder.int(doc, "n").getOrElse(0),
              decoder.string(doc, "err"))
          },
          decoder.child(result, "value"))
      }
    }

    private final def execute()(implicit ec: ExecutionContext): Future[FindAndModifyResult] = {
      import FindAndModifyCommand.{
        FindAndModify,
        ImplicitlyDocumentProducer => DP
      }

      implicit def selectorWriter = swriter

      val cmd = FindAndModify(
        query = implicitly[DP](selector),
        modify = modifier,
        sort = sort.map(implicitly[DP](_)),
        fields = fields.map(implicitly[DP](_)),
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
        arrayFilters = arrayFilters.map(implicitly[DP](_)))

      runCommand(cmd, writePreference)
    }
  }
}
