package reactivemongo.api.collections

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.{ Collation, SerializationPack, Session }
import reactivemongo.api.commands.{
  CommandCodecs,
  LastError,
  MultiBulkWriteResult,
  ResolvedCollectionCommand,
  WriteConcern,
  WriteResult
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define orderedParam the ordered behaviour
 */
trait DeleteOps[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] =>

  private[reactivemongo] object DeleteCommand
    extends reactivemongo.api.commands.DeleteCommand[collection.pack.type] {
    val pack: collection.pack.type = collection.pack
  }
  import DeleteCommand.{ Delete, DeleteElement }

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

  private lazy val deleteWriter: Option[Session] => pack.Writer[DeleteCmd] = {
    val builder = pack.newBuilder
    import builder.{ elementProducer => element }

    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)
    val writeSession = CommandCodecs.writeSession(builder)
    val writeCollation = Collation.serializeWith(pack, _: Collation)(builder)

    def writeElement(e: DeleteElement): pack.Document = {
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("q", e.q),
        element("limit", builder.int(e.limit)))

      e.collation.foreach { c =>
        elements += element("collation", writeCollation(c))
      }

      builder.document(elements.result())
    }

    { session =>
      pack.writer[DeleteCmd] { delete =>
        val elements = Seq.newBuilder[pack.ElementProducer]

        elements ++= Seq(
          element("delete", builder.string(delete.collection)),
          element("ordered", builder.boolean(delete.command.ordered)),
          element(
            "writeConcern",
            writeWriteConcern(delete.command.writeConcern)))

        session.foreach { s =>
          elements ++= writeSession(s)
        }

        delete.command.deletes.headOption.foreach { first =>
          elements += element("deletes", builder.array(
            writeElement(first), delete.command.deletes.map(writeElement)))
        }

        builder.document(elements.result())
      }
    }
  }

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
     * Performs a delete with a one single selector (see [[DeleteCommand.DeleteElement]]).
     * This will delete all the documents matched by the `q` selector.
     *
     * @param q $queryParam
     * @param limit $limitParam
     * @param collation $collationParam
     */
    final def one[Q, U](q: Q, limit: Option[Int] = None, collation: Option[Collation] = None)(implicit ec: ExecutionContext, qw: pack.Writer[Q]): Future[WriteResult] = element[Q, U](q, limit, collation).flatMap { upd => execute(Seq(upd)) }

    /**
     * Prepares an [[DeleteCommand.DeleteElement]].
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
      val emptyCmd = ResolvedCollectionCommand(
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
      if (metadata.maxWireVersion >= MongoWireVersion.V26) {
        val cmd = Delete(deletes, ordered, writeConcern)

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
      } else { // Mongo < 2.6
        Future.failed[WriteResult](unsupportedVersion(metadata))
      }
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
