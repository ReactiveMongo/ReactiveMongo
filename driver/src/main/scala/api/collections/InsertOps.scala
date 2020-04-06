package reactivemongo.api.collections

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.{ SerializationPack, WriteConcern }

import reactivemongo.api.commands.{
  CommandCodecsWithPack,
  InsertCommand,
  LastErrorFactory,
  MultiBulkWriteResultFactory,
  ResolvedCollectionCommand,
  UpsertedFactory,
  WriteResult
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define orderedParam the [[https://docs.mongodb.com/manual/reference/method/db.collection.insert/#perform-an-unordered-insert ordered]] behaviour
 * @define bypassDocumentValidationParam the flag to bypass document validation during the operation
 */
trait InsertOps[P <: SerializationPack]
  extends InsertCommand[P] with CommandCodecsWithPack[P]
  with MultiBulkWriteResultFactory[P] with UpsertedFactory[P]
  with LastErrorFactory[P] { collection: GenericCollection[P] =>

  /**
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam
   */
  private[reactivemongo] final def prepareInsert(
    ordered: Boolean,
    writeConcern: WriteConcern,
    bypassDocumentValidation: Boolean): InsertBuilder = {
    if (ordered) {
      new OrderedInsert(writeConcern, bypassDocumentValidation)
    } else {
      new UnorderedInsert(writeConcern, bypassDocumentValidation)
    }
  }

  /** Builder for insert operations. */
  sealed trait InsertBuilder {
    //implicit protected def writer: pack.Writer[T]

    @inline private def metadata = db.connectionState.metadata

    /** The max BSON size, including the size of command envelope */
    private lazy val maxBsonSize: Int = {
      // Command envelope to compute accurate BSON size limit
      val emptyDoc: pack.Document = pack.newBuilder.document(Seq.empty)

      val emptyCmd = new ResolvedCollectionCommand(
        collection.name,
        new Insert(
          emptyDoc, Seq.empty[pack.Document], ordered, writeConcern, false))

      val doc = pack.serialize(emptyCmd, insertWriter(None))

      metadata.maxBsonSize - pack.bsonSize(doc) + pack.bsonSize(emptyDoc)
    }

    /** $orderedParam */
    def ordered: Boolean

    /** $writeConcernParam */
    def writeConcern: WriteConcern

    /** $bypassDocumentValidationParam (default: `false`) */
    def bypassDocumentValidation: Boolean

    protected def bulkRecover: Option[Exception => Future[WriteResult]]

    /**
     * Inserts a single document.
     *
     * {{{
     * import scala.concurrent.ExecutionContext.Implicits.global
     *
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def insertOne(coll: BSONCollection, doc: BSONDocument) = {
     *   val insert = coll.insert(ordered = true)
     *
     *   insert.one(doc)
     * }
     * }}}
     */
    final def one[T](document: T)(implicit ec: ExecutionContext, writer: pack.Writer[T]): Future[WriteResult] = Future(pack.serialize(document, writer)).flatMap { single =>
      execute(Seq(single))
    }

    /** Inserts many documents, according the ordered behaviour. */
    /**
     * [[https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/ Inserts many documents]], according the ordered behaviour.
     *
     * {{{
     * import scala.concurrent.ExecutionContext.Implicits.global
     *
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def insertMany(coll: BSONCollection, docs: Iterable[BSONDocument]) = {
     *   val insert = coll.insert(ordered = true)
     *
     *   insert.many(docs) // Future[MultiBulkWriteResult]
     * }
     * }}}
     */
    final def many[T](documents: Iterable[T])(implicit ec: ExecutionContext, writer: pack.Writer[T]): Future[MultiBulkWriteResult] = {
      val bulkSz = metadata.maxBulkSize
      val maxSz = maxBsonSize

      for {
        docs <- serialize(documents)
        res <- {
          val bulkProducer = BulkOps.bulks(
            docs, maxSz, bulkSz) { pack.bsonSize(_) }

          BulkOps.bulkApply[pack.Document, WriteResult](bulkProducer)({ bulk =>
            execute(bulk.toSeq)
          }, bulkRecover)
        }
      } yield MultiBulkWriteResult(res)
    }

    // ---

    private def serialize[T](input: Iterable[T])(implicit ec: ExecutionContext, writer: pack.Writer[T]): Future[Iterable[pack.Document]] =
      Future.sequence(input.map { v =>
        Try(pack.serialize(v, writer)) match {
          case Success(v) => Future.successful(v)
          case Failure(e) => Future.failed[pack.Document](e)
        }
      })

    implicit private val resultReader: pack.Reader[InsertResult] =
      CommandCodecs.defaultWriteResultReader(pack)

    implicit private lazy val writer: pack.Writer[InsertCmd] =
      insertWriter(collection.db.session)

    private final def execute(documents: Seq[pack.Document])(implicit ec: ExecutionContext): Future[WriteResult] = documents.headOption match {
      case Some(head) => {
        val cmd = new Insert(
          head, documents.tail, ordered, writeConcern,
          bypassDocumentValidation)

        runCommand(cmd, writePreference).flatMap { wr =>
          val flattened = wr.flatten

          if (!flattened.ok) {
            // was ordered, with one doc => fail if has an error
            Future.failed(lastError(flattened).
              getOrElse[Exception](new GenericDriverException(
                s"fails to insert: $documents")))

          } else Future.successful(wr)
        }
      }

      case _ => Future.successful(WriteResult.empty) // No doc to insert
    }
  }

  // ---

  private val orderedRecover = Option.empty[Exception => Future[WriteResult]]

  private final class OrderedInsert(
    val writeConcern: WriteConcern,
    val bypassDocumentValidation: Boolean) extends InsertBuilder {

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

  private final class UnorderedInsert(
    val writeConcern: WriteConcern,
    val bypassDocumentValidation: Boolean) extends InsertBuilder {

    val ordered = false
    val bulkRecover = unorderedRecover
  }
}
