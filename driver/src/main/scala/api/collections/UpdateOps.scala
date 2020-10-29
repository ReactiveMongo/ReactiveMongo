package reactivemongo.api.collections

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.{ Collation, SerializationPack, WriteConcern }
import reactivemongo.api.commands.{
  LastErrorFactory,
  MultiBulkWriteResultFactory,
  ResolvedCollectionCommand,
  UpdateCommand,
  UpdateWriteResultFactory,
  UpsertedFactory,
  WriteResult
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define orderedParam the [[https://docs.mongodb.com/manual/reference/method/db.collection.update/#perform-an-unordered-update ordered]] behaviour
 * @define bypassDocumentValidationParam the flag to bypass document validation during the operation
 * @define maxBulkSizeParam the maximum number of document(s) per bulk
 */
trait UpdateOps[P <: SerializationPack] extends UpdateCommand[P]
  with UpdateWriteResultFactory[P] with MultiBulkWriteResultFactory[P]
  with UpsertedFactory[P] with LastErrorFactory[P] {
  collection: GenericCollection[P] =>

  protected lazy val maxWireVersion =
    collection.db.connectionState.metadata.maxWireVersion

  /**
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam
   * @param maxBulkSize $maxBulkSize
   */
  private[reactivemongo] final def prepareUpdate(
    ordered: Boolean,
    writeConcern: WriteConcern,
    bypassDocumentValidation: Boolean,
    maxBulkSize: Int): UpdateBuilder = {
    if (ordered) {
      new OrderedUpdate(writeConcern, bypassDocumentValidation, maxBulkSize)
    } else {
      new UnorderedUpdate(writeConcern, bypassDocumentValidation, maxBulkSize)
    }
  }

  /** Builder for update operations. */
  sealed trait UpdateBuilder {
    /** $orderedParam */
    def ordered: Boolean

    /** $writeConcernParam */
    def writeConcern: WriteConcern

    /** $bypassDocumentValidationParam */
    def bypassDocumentValidation: Boolean

    /** $maxBulkSizeParam */
    def maxBulkSize: Int

    /** Returns an update builder with the given [[maxBulkSize]]. */
    def maxBulkSize(max: Int): UpdateBuilder

    protected def bulkRecover: Option[Exception => Future[UpdateWriteResult]]

    /**
     * Performs a [[https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/ single update]] (see [[UpdateElement]]).
     *
     * {{{
     * import scala.concurrent.ExecutionContext.Implicits.global
     *
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def updateOne(coll: BSONCollection, q: BSONDocument, u: BSONDocument) =
     *   coll.update.one(q, u, upsert = true)
     * }}}
     */
    final def one[Q, U](q: Q, u: U, upsert: Boolean = false, multi: Boolean = false)(implicit ec: ExecutionContext, qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateWriteResult] = element[Q, U](q, u, upsert, multi, None, Seq.empty).flatMap { upd => execute(upd) }

    /**
     * Performs a [[https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/ single update]] (see [[UpdateElement]]).
     */
    final def one[Q, U](q: Q, u: U, upsert: Boolean, multi: Boolean, collation: Option[Collation])(implicit ec: ExecutionContext, qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateWriteResult] = element[Q, U](q, u, upsert, multi, collation, Seq.empty).flatMap { upd => execute(upd) }

    /**
     * Performs a [[https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/ single update]] (see [[UpdateElement]]).
     */
    final def one[Q, U](q: Q, u: U, upsert: Boolean, multi: Boolean, collation: Option[Collation], arrayFilters: Seq[pack.Document])(implicit ec: ExecutionContext, qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateWriteResult] = element[Q, U](q, u, upsert, multi, collation, arrayFilters).flatMap { upd => execute(upd) }

    /** Prepares an [[UpdateElement]] */
    final def element[Q, U](q: Q, u: U, upsert: Boolean = false, multi: Boolean = false)(implicit qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateElement] = element(q, u, upsert, multi, None, Seq.empty)

    /** Prepares an [[UpdateElement]] */
    final def element[Q, U](q: Q, u: U, upsert: Boolean, multi: Boolean, collation: Option[Collation])(implicit qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateElement] = element(q, u, upsert, multi, collation, Seq.empty)

    /** Prepares an [[UpdateElement]] */
    final def element[Q, U](q: Q, u: U, upsert: Boolean, multi: Boolean, collation: Option[Collation], arrayFilters: Seq[pack.Document])(implicit qw: pack.Writer[Q], uw: pack.Writer[U]): Future[UpdateElement] = {
      (Try(pack.serialize(q, qw)).map { query =>
        new UpdateElement(query, pack.serialize(u, uw), upsert, multi, collation, arrayFilters)
      }) match {
        case Success(element) => Future.successful(element)
        case Failure(cause)   => Future.failed[UpdateElement](cause)
      }
    }

    /**
     * [[https://docs.mongodb.com/manual/reference/method/db.collection.updateMany/ Updates many documents]], according the ordered behaviour.
     *
     * {{{
     * import scala.concurrent.{ ExecutionContext, Future }
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def updateMany(
     *   coll: BSONCollection,
     *   first: BSONDocument,
     *   docs: Iterable[BSONDocument])(implicit ec: ExecutionContext) = {
     *   val update = coll.update(ordered = true)
     *   val elements = Future.sequence(docs.map { doc =>
     *     update.element(
     *       q = BSONDocument("update" -> "selector"),
     *       u = BSONDocument(f"$$set" -> doc),
     *       upsert = true,
     *       multi = false)
     *   })
     *
     *   for {
     *     _ <- update.element(
     *       q = BSONDocument("update" -> "selector"),
     *       u = BSONDocument(f"$$set" -> first),
     *       upsert = true,
     *       multi = false)
     *     ups <- elements
     *     res <- update.many(ups) // Future[MultiBulkWriteResult]
     *   } yield res
     * }
     * }}}
     */
    final def many(firstUpdate: UpdateElement, updates: Iterable[UpdateElement])(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = {
      val bulkProducer = BulkOps.bulks(
        Seq(firstUpdate) ++ updates, maxBsonSize, maxBulkSize) { up =>
          elementEnvelopeSize + pack.bsonSize(up.q) + pack.bsonSize(up.u)
        }

      BulkOps.bulkApply[UpdateElement, UpdateWriteResult](
        bulkProducer)(
        { bulk => execute(firstUpdate, bulk.toSeq) },
        bulkRecover).
        map(MultiBulkWriteResult(_))
    }

    /**
     * [[https://docs.mongodb.com/manual/reference/method/db.collection.updateMany/ Updates many documents]], according the ordered behaviour.
     *
     * {{{
     * import scala.concurrent.{ ExecutionContext, Future }
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def updateMany(
     *   coll: BSONCollection,
     *   docs: Iterable[BSONDocument])(implicit ec: ExecutionContext) = {
     *   val update = coll.update(ordered = true)
     *   val elements = Future.sequence(docs.map { doc =>
     *     update.element(
     *       q = BSONDocument("update" -> "selector"),
     *       u = BSONDocument(f"$$set" -> doc),
     *       upsert = true,
     *       multi = false)
     *   })
     *
     *   elements.flatMap { ups =>
     *     update.many(ups) // Future[MultiBulkWriteResult]
     *   }
     * }
     * }}}
     */
    final def many(updates: Iterable[UpdateElement])(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = updates.headOption match {
      case Some(first) => {
        println(s"maxBulkSize = $maxBulkSize")

        val bulkProducer = BulkOps.bulks(
          updates, maxBsonSize, maxBulkSize) { up =>
          elementEnvelopeSize + pack.bsonSize(up.q) + pack.bsonSize(up.u)
        }

        BulkOps.bulkApply[UpdateElement, UpdateWriteResult](
          bulkProducer)({ bulk =>
          println(s"bulk = $bulk")
          execute(first, bulk.drop(1).toSeq)
        }, bulkRecover).map(MultiBulkWriteResult(_))
      }

      case _ =>
        Future.failed[MultiBulkWriteResult](
          new GenericDriverException("No update to be performed"))
    }

    // ---

    @inline private def metadata = db.connectionState.metadata

    /** The max BSON size, including the size of command envelope */
    private def maxBsonSize = {
      val builder = pack.newBuilder
      val emptyElm = new UpdateElement(
        q = builder.document(Seq.empty),
        u = builder.document(Seq.empty),
        upsert = false,
        multi = false,
        collation = None,
        arrayFilters = Seq.empty)

      // Command envelope to compute accurate BSON size limit
      val emptyCmd = new ResolvedCollectionCommand(
        collection.name,
        new Update(
          emptyElm, Seq.empty, ordered, writeConcern, false))

      val doc = pack.serialize(emptyCmd, updateWriter(None))

      metadata.maxBsonSize - pack.bsonSize(doc)
    }

    private lazy val elementEnvelopeSize = {
      val builder = pack.newBuilder
      val emptyDoc = builder.document(Seq.empty)
      val sfalse = builder.boolean(false)

      import builder.{ elementProducer => elmt }

      val elements = Seq.newBuilder[pack.ElementProducer] ++= Seq(
        elmt("q", emptyDoc), elmt("u", emptyDoc),
        elmt("upsert", sfalse), elmt("multi", sfalse))

      if (metadata.maxWireVersion >= MongoWireVersion.V34) {
        elements += elmt("collation", emptyDoc)
      }

      if (metadata.maxWireVersion >= MongoWireVersion.V36) {
        elements += elmt("arrayFilters", emptyDoc)
      }

      pack.bsonSize(builder.document(elements.result()))
    }

    private final def execute(
      firstUpdate: UpdateElement,
      updates: Seq[UpdateElement] = Seq.empty)(
      implicit
      ec: ExecutionContext): Future[UpdateWriteResult] = {

      val cmd = new Update(
        firstUpdate, updates, ordered, writeConcern, bypassDocumentValidation)

      runCommand(cmd, writePreference).flatMap { wr =>
        val flattened = wr.flatten

        if (!flattened.ok) {
          // was ordered, with one doc => fail if has an error
          Future.failed(lastError(flattened).
            getOrElse[Exception](new GenericDriverException(
              s"fails to update: $updates")))

        } else Future.successful(wr)
      }
    }
  }

  // ---

  private val orderedRecover =
    Option.empty[Exception => Future[UpdateWriteResult]]

  private final class OrderedUpdate(
    val writeConcern: WriteConcern,
    val bypassDocumentValidation: Boolean,
    val maxBulkSize: Int) extends UpdateBuilder {

    val ordered = true
    val bulkRecover = orderedRecover

    def maxBulkSize(max: Int): UpdateBuilder =
      new OrderedUpdate(writeConcern, bypassDocumentValidation, max)
  }

  private val unorderedRecover: Option[Exception => Future[UpdateWriteResult]] =
    Some[Exception => Future[UpdateWriteResult]] {
      case lastError: WriteResult =>
        Future.successful(new UpdateWriteResult(
          ok = false,
          n = lastError.n,
          nModified = 0,
          upserted = Seq.empty,
          writeErrors = lastError.writeErrors,
          writeConcernError = lastError.writeConcernError,
          code = lastError.code,
          errmsg = Some(lastError.getMessage)))

      case cause =>
        Future.successful(new UpdateWriteResult(
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
    val writeConcern: WriteConcern,
    val bypassDocumentValidation: Boolean,
    val maxBulkSize: Int) extends UpdateBuilder {

    val ordered = false
    val bulkRecover = unorderedRecover

    def maxBulkSize(max: Int): UpdateBuilder =
      new UnorderedUpdate(writeConcern, bypassDocumentValidation, max)
  }
}
