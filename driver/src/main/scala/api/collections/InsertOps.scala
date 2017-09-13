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
  LastError,
  ResolvedCollectionCommand,
  WriteConcern,
  WriteResult
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define orderedParam the [[https://docs.mongodb.com/manual/reference/method/db.collection.insert/#perform-an-unordered-insert ordered]] behaviour
 */
private[reactivemongo] trait InsertOps[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] with BulkOps[P] =>

  val pack: P

  /**
   * @param ordered $orderedParam
   * @param writeConcern writeConcernParam
   */
  private[reactivemongo] final def prepareInsert[T: pack.Writer](
    ordered: Boolean,
    writeConcern: WriteConcern): InsertBuilder[T] = {
    if (ordered) {
      new Ordered[T](writeConcern, implicitly[pack.Writer[T]])
    } else {
      new Unordered[T](writeConcern, implicitly[pack.Writer[T]])
    }
  }

  /** Builder for insert operations. */
  sealed trait InsertBuilder[T] {
    protected implicit def writer: pack.Writer[T]

    final protected lazy val metadata: Future[ProtocolMetadata] =
      collection.db.connection.metadata.fold(
        Future.failed[ProtocolMetadata](collection.MissingMetadata()))(
          Future.successful(_))

    /** The max BSON size, including the size of command envelope */
    final protected def maxBsonSize(implicit ec: ExecutionContext) =
      metadata.map { meta =>
        // Command envelope to compute accurate BSON size limit
        val i = ResolvedCollectionCommand(
          collection.name,
          BatchCommands.InsertCommand.Insert(
            Seq.empty, ordered, writeConcern))

        val doc = pack.serialize(i, BatchCommands.InsertWriter)

        meta.maxBsonSize - pack.bsonSize(doc)
      }

    /** $orderedParam */
    def ordered: Boolean

    /** $writeConcernParam */
    def writeConcern: WriteConcern

    protected def bulkRecover: Option[Exception => Future[WriteResult]]

    /** Inserts a single document */
    final def one(document: T)(implicit ec: ExecutionContext): Future[WriteResult] = Future(pack.serialize(document, writer)).flatMap { single =>
      execute(Seq(single))
    }

    /** Inserts many documents, according the ordered behaviour. */
    final def many(documents: Iterable[T])(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = for {
      meta <- metadata
      maxSz <- maxBsonSize
      docs <- serialize(documents)
      res <- {
        val bulkProducer = collection.bulks(
          docs, meta.maxBulkSize, maxSz)

        collection.bulkApply[WriteResult](bulkProducer)({ bulk =>
          execute(bulk.toSeq)
        }, bulkRecover).map(MultiBulkWriteResult(_))
      }
    } yield res

    final protected def serialize(input: Iterable[T])(implicit ec: ExecutionContext): Future[Iterable[pack.Document]] = Future.sequence(input.map { v =>
      Try(pack.serialize(v, writer)) match {
        case Success(v) => Future.successful(v)
        case Failure(e) => Future.failed[pack.Document](e)
      }
    })

    protected final def execute(documents: Seq[pack.Document])(
      implicit
      ec: ExecutionContext): Future[WriteResult] = metadata.flatMap { meta =>
      import BatchCommands.{ DefaultWriteResultReader, InsertWriter }

      if (meta.maxWireVersion >= MongoWireVersion.V26) {
        val cmd = BatchCommands.InsertCommand.Insert(
          documents, ordered, writeConcern)

        Future.successful(cmd).flatMap(runCommand(_, writePref).flatMap { wr =>
          val flattened = wr.flatten

          if (!flattened.ok) {
            // was ordered, with one doc => fail if has an error
            Future.failed(WriteResult.lastError(flattened).
              getOrElse[Exception](GenericDriverException(
                s"fails to insert: $documents")))

          } else Future.successful(wr)
        })
      } else { // Mongo < 2.6
        Future.failed[WriteResult](GenericDriverException(
          s"unsupported MongoDB version: $meta"))
      }
    }
  }

  // ---

  private val orderedRecover = Option.empty[Exception => Future[WriteResult]]

  private final class Ordered[T](
    val writeConcern: WriteConcern,
    val writer: pack.Writer[T]) extends InsertBuilder[T] {

    val ordered = true
    val bulkRecover = orderedRecover
  }

  private val unorderedRecover: Option[Exception => Future[WriteResult]] =
    Some[Exception => Future[WriteResult]] {
      case lastError: WriteResult =>
        Future.successful(lastError)

      case cause => Future.successful(LastError(
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
        wtime = Option.empty[Int]))
    }

  private final class Unordered[T](
    val writeConcern: WriteConcern,
    val writer: pack.Writer[T]) extends InsertBuilder[T] {

    val ordered = false
    val bulkRecover = unorderedRecover
  }
}
