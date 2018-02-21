package reactivemongo.api.collections

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.core.errors.GenericDriverException

import reactivemongo.core.nodeset.ProtocolMetadata

import reactivemongo.api.SerializationPack
import reactivemongo.api.commands.{
  BulkOps,
  MultiBulkWriteResult,
  ResolvedCollectionCommand,
  UpdateWriteResult,
  WriteConcern,
  WriteResult
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define orderedParam the ordered behaviour
 */
trait UpdateOps[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] =>

  protected val pack: P

  /**
   * @param ordered $orderedParam
   * @param writeConcern writeConcernParam
   */
  private[reactivemongo] final def prepareUpdate(
    ordered: Boolean,
    writeConcern: WriteConcern): UpdateBuilder = {
    if (ordered) new OrderedUpdate(writeConcern)
    else new UnorderedUpdate(writeConcern)
  }

  /** Builder for update operations. */
  sealed trait UpdateBuilder {
    import BatchCommands.UpdateCommand.UpdateElement

    /** $orderedParam */
    def ordered: Boolean

    /** $writeConcernParam */
    def writeConcern: WriteConcern

    protected def bulkRecover: Option[Exception => Future[UpdateWriteResult]]

    /**
     * Performs a [[https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/ single update]] (see [[BatchCommands.UpdateCommand.UpdateElement]]).
     */
    final def one[Q, U](q: Q, u: U, upsert: Boolean, multi: Boolean)(implicit ec: ExecutionContext, qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateWriteResult] = element[Q, U](q, u, upsert, multi).flatMap { upd => execute(Seq(upd)) }

    /** Prepares an [[BatchCommands.UpdateCommand.UpdateElement]] */
    final def element[Q, U](q: Q, u: U, upsert: Boolean, multi: Boolean)(implicit qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateElement] =
      (Try(pack.serialize(q, qw)).map { query =>
        UpdateElement(query, pack.serialize(u, uw), upsert, multi)
      }) match {
        case Success(element) => Future.successful(element)
        case Failure(cause)   => Future.failed[UpdateElement](cause)
      }

    /**
     * [[https://docs.mongodb.com/manual/reference/method/db.collection.updateMany/ Updates many documents]], according the ordered behaviour.
     *
     * {{{
     * import reactivemongo.bson.BSONDocument
     * import reactivemongo.api.collections.BSONCollection
     *
     * def updateMany(coll: BSONCollection, docs: Iterable[BSONDocument]) = {
     *   val update = coll.update(ordered = true)
     *   val elements = docs.map { doc =>
     *     update.element(
     *       q = BSONDocument("update" -> "selector"),
     *       u = BSONDocument("\$set" -> doc),
     *       upsert = true,
     *       multi = false)
     *   }
     *
     *   update.many(elements) // Future[MultiBulkWriteResult]
     * }
     * }}}
     */
    final def many(updates: Iterable[UpdateElement])(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = for {
      meta <- metadata
      maxSz <- maxBsonSize
      res <- {
        val bulkProducer = BulkOps.bulks(
          updates, maxSz, meta.maxBulkSize) { up =>
          elementEnvelopeSize + pack.bsonSize(up.q) + pack.bsonSize(up.u)
        }

        BulkOps.bulkApply[UpdateElement, UpdateWriteResult](
          bulkProducer)({ bulk => execute(bulk.toSeq) }, bulkRecover).
          map(MultiBulkWriteResult(_))
      }
    } yield res

    // ---

    private lazy val metadata: Future[ProtocolMetadata] =
      collection.db.connection.metadata.fold(
        Future.failed[ProtocolMetadata](collection.MissingMetadata()))(
          Future.successful(_))

    /** The max BSON size, including the size of command envelope */
    private def maxBsonSize(implicit ec: ExecutionContext) =
      metadata.map { meta =>
        // Command envelope to compute accurate BSON size limit
        val i = ResolvedCollectionCommand(
          collection.name,
          BatchCommands.UpdateCommand.Update(
            Seq.empty, ordered, writeConcern))

        val doc = pack.serialize(i, BatchCommands.UpdateWriter)

        meta.maxBsonSize - pack.bsonSize(doc)
      }

    private lazy val elementEnvelopeSize = {
      val builder = pack.newBuilder
      val emptyDoc = builder.document(Seq.empty)
      val sfalse = builder.boolean(false)
      val elements = Seq[pack.ElementProducer](
        builder.elementProducer("q", emptyDoc),
        builder.elementProducer("u", emptyDoc),
        builder.elementProducer("upsert", sfalse),
        builder.elementProducer("multi", sfalse))

      pack.bsonSize(builder.document(elements))
    }

    private final def execute(updates: Seq[UpdateElement])(
      implicit
      ec: ExecutionContext): Future[UpdateWriteResult] =
      metadata.flatMap { meta =>
        import BatchCommands.{ UpdateReader, UpdateWriter }

        if (meta.maxWireVersion >= MongoWireVersion.V26) {
          val cmd = BatchCommands.UpdateCommand.Update(
            updates, ordered, writeConcern)

          Future.successful(cmd).flatMap(
            runCommand(_, writePref).flatMap { wr =>
              val flattened = wr.flatten

              if (!flattened.ok) {
                // was ordered, with one doc => fail if has an error
                Future.failed(WriteResult.lastError(flattened).
                  getOrElse[Exception](GenericDriverException(
                    s"fails to update: $updates")))

              } else Future.successful(wr)
            })
        } else { // Mongo < 2.6
          Future.failed[UpdateWriteResult](GenericDriverException(
            s"unsupported MongoDB version: $meta"))
        }
      }
  }

  // ---

  private val orderedRecover =
    Option.empty[Exception => Future[UpdateWriteResult]]

  private final class OrderedUpdate(
    val writeConcern: WriteConcern) extends UpdateBuilder {

    val ordered = true
    val bulkRecover = orderedRecover
  }

  private val unorderedRecover: Option[Exception => Future[UpdateWriteResult]] =
    Some[Exception => Future[UpdateWriteResult]] {
      case lastError: WriteResult =>
        Future.successful(UpdateWriteResult(
          ok = false,
          n = lastError.n,
          nModified = 0,
          upserted = Seq.empty,
          writeErrors = lastError.writeErrors,
          writeConcernError = lastError.writeConcernError,
          code = lastError.code,
          errmsg = Some(lastError.getMessage)))

      case cause =>
        Future.successful(UpdateWriteResult(
          ok = false,
          n = 0,
          nModified = 0,
          upserted = Seq.empty,
          writeErrors = Seq.empty,
          writeConcernError = Option.empty,
          code = Option.empty,
          errmsg = Option(cause.getMessage)))
    }

  private final class UnorderedUpdate(
    val writeConcern: WriteConcern) extends UpdateBuilder {

    val ordered = false
    val bulkRecover = unorderedRecover
  }
}
