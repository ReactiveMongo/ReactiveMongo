package reactivemongo.api.collections

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.{ Collation, SerializationPack, Session, WriteConcern }
import reactivemongo.api.commands.{
  CommandCodecsWithPack,
  DeleteCommand,
  LastError,
  MultiBulkWriteResult,
  ResolvedCollectionCommand,
  WriteResult
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define orderedParam the ordered behaviour
 */
trait DeleteOps[P <: SerializationPack]
  extends DeleteCommand[P] with CommandCodecsWithPack[P] {
  collection: GenericCollection[P] =>

  /**
   * @param ordered $orderedParam
   * @param writeConcern writeConcernParam
   */
  private[reactivemongo] final def prepareDelete(
    ordered: Boolean,
    writeConcern: WriteConcern): DeleteBuilder = {
    if (ordered) new OrderedDelete(writeConcern)
    else new UnorderedDelete(writeConcern)
  }

  private type DeleteCmd = ResolvedCollectionCommand[DeleteCommand.Delete]

  /**
   * Builder for delete operations.
   *
   * @define queryParam the query/selector
   * @define limitParam the maximum number of documents
   * @define collationParam the collation
   */
  sealed trait DeleteBuilder {
    /** $orderedParam */
    def ordered: Boolean

    /** $writeConcernParam */
    def writeConcern: WriteConcern

    protected def bulkRecover: Option[Exception => Future[WriteResult]]

    /**
     * Performs a delete with a one single selector (see [[DeleteElement]]).
     * This will delete all the documents matched by the `q` selector.
     *
     * @param q $queryParam
     * @param limit $limitParam
     * @param collation $collationParam
     */
    final def one[Q, U](q: Q, limit: Option[Int] = None, collation: Option[Collation] = None)(implicit ec: ExecutionContext, qw: pack.Writer[Q]): Future[WriteResult] = element[Q, U](q, limit, collation).flatMap { upd => execute(Seq(upd)) }

    /**
     * Prepares an [[DeleteElement]].
     *
     * @param q $queryParam
     * @param limit $limitParam
     * @param collation $collationParam
     *
     * @see [[many]]
     */
    final def element[Q, U](q: Q, limit: Option[Int] = None, collation: Option[Collation] = None)(implicit qw: pack.Writer[Q]): Future[DeleteElement] =
      (Try(pack.serialize(q, qw)).map { query =>
        new DeleteElement(query, limit.getOrElse(0), collation)
      }) match {
        case Success(element) => Future.successful(element)
        case Failure(cause)   => Future.failed[DeleteElement](cause)
      }

    /**
     * Performs a bulk operation using many deletes, each can delete multiple documents.
     *
     * {{{
     * import scala.concurrent.{ ExecutionContext, Future }
     *
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def bulkDelete(
     *   coll: BSONCollection,
     *   docs: Iterable[BSONDocument])(implicit ec: ExecutionContext) = {
     *   val delete = coll.delete(ordered = true)
     *   val elements = Future.sequence(docs.map { doc =>
     *     delete.element(
     *       q = doc,
     *       limit = Some(1)) // only first match
     *   })
     *
     *   elements.flatMap { ops =>
     *     delete.many(ops) // Future[MultiBulkWriteResult]
     *   }
     * }
     * }}}
     */
    final def many(deletes: Iterable[DeleteElement])(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = {
      val bulkProducer = BulkOps.bulks(
        deletes, maxBsonSize, metadata.maxBulkSize) { d =>
        elementEnvelopeSize + pack.bsonSize(d.q)
      }

      BulkOps.bulkApply[DeleteElement, WriteResult](
        bulkProducer)({ bulk => execute(bulk.toSeq) }, bulkRecover).
        map(MultiBulkWriteResult(_))
    }

    // ---

    @inline private def metadata = collection.db.connectionState.metadata

    /** The max BSON size, including the size of command envelope */
    private def maxBsonSize = {
      // Command envelope to compute accurate BSON size limit
      val emptyCmd = new ResolvedCollectionCommand(
        collection.name, Delete(Seq.empty, ordered, writeConcern))

      val doc = pack.serialize(emptyCmd, deleteWriter(None))

      metadata.maxBsonSize - pack.bsonSize(doc)
    }

    private lazy val elementEnvelopeSize = {
      val builder = pack.newBuilder
      val emptyDoc = builder.document(Seq.empty)
      val elements = Seq[pack.ElementProducer](
        builder.elementProducer("q", emptyDoc),
        builder.elementProducer("limit", builder.int(0)),
        builder.elementProducer("collation", emptyDoc))

      pack.bsonSize(builder.document(elements))
    }

    implicit private val resultReader: pack.Reader[DeleteCommand.DeleteResult] =
      CommandCodecs.defaultWriteResultReader(pack)

    implicit private lazy val writer: pack.Writer[DeleteCmd] =
      deleteWriter(collection.db.session)

    private final def execute(deletes: Seq[DeleteElement])(
      implicit
      ec: ExecutionContext): Future[WriteResult] = {
      val cmd = new Delete(deletes, ordered, writeConcern)

      Future.successful(cmd).flatMap(
        runCommand(_, writePreference).flatMap { wr =>
          val flattened = wr.flatten

          if (!flattened.ok) {
            // was ordered, with one doc => fail if has an error
            Future.failed(WriteResult.lastError(flattened).
              getOrElse[Exception](new GenericDriverException(
                s"fails to delete: $deletes")))

          } else Future.successful(wr)
        })
    }
  }

  // ---

  private val orderedRecover =
    Option.empty[Exception => Future[WriteResult]]

  private final class OrderedDelete(
    val writeConcern: WriteConcern) extends DeleteBuilder {

    val ordered = true
    val bulkRecover = orderedRecover
  }

  private val unorderedRecover: Option[Exception => Future[WriteResult]] =
    Some[Exception => Future[WriteResult]] {
      case lastError: WriteResult =>
        Future.successful(lastError)

      case cause => Future.successful(new LastError(
        ok = false,
        errmsg = Option(cause.getMessage),
        code = Option.empty,
        lastOp = Some(2002), // InsertOp
        n = 0,
        singleShard = Option.empty[String],
        updatedExisting = false,
        upserted = Option.empty,
        wnote = Option.empty[WriteConcern.W],
        wtimeout = false,
        waited = Option.empty[Int],
        wtime = Option.empty[Int],
        writeErrors = Seq.empty,
        writeConcernError = Option.empty))
    }

  private final class UnorderedDelete(
    val writeConcern: WriteConcern) extends DeleteBuilder {

    val ordered = false
    val bulkRecover = unorderedRecover
  }
}
