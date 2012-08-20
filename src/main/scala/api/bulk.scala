package reactivemongo.api

import java.nio.ByteOrder._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.iteratee._
import reactivemongo.bson.handlers.BSONWriter
import reactivemongo.utils.LazyLogger
import scala.concurrent.{ExecutionContext, Future}

/**
 * Bulk insertion.
 */
object bulk {
  private val logger = LazyLogger(LoggerFactory.getLogger("Bulk"))
  /** Default maximum size for a bulk (1MB). */
  val MaxBulkSize = 1024 * 1024
  /** Default maximum documents number for a bulk (100). */
  val MaxDocs = 100

  /**
   * Returns an iteratee that will consume chunks (chunk == document) and insert them into the given collection.
   *
   * This iteratee eventually gives the number of documents that have been inserted into the given collection.
   *
   * @param coll The collection where the documents will be stored.
   * @param bulkSize The number of documents per bulk.
   * @param bulkByteSize The maximum size for a bulk, in bytes.
   */
  def iteratee(coll: Collection, bulkSize: Int = MaxDocs, bulkByteSize: Int = MaxBulkSize)(implicit context: ExecutionContext) :Iteratee[ChannelBuffer, Int] =
    iteratee(coll, (docs, bulk) => docs > bulkSize || bulk > bulkByteSize)

  /**
   * Returns an iteratee that will consume chunks (chunk == document) and insert them into the given collection.
   *
   * This iteratee eventually gives the number of documents that have been inserted into the given collection.
   *
   * @param coll The collection where the documents will be stored.
   * @param reachedUpperBound A function that returns true if the staging bulk can be sent to be written. This function has two parameters: numberOfDocuments and numberOfBytes.
   */
  def iteratee(coll: Collection, reachedUpperBound: (Int, Int) => Boolean)(implicit context: ExecutionContext) :Iteratee[ChannelBuffer, Int] = {
    Iteratee.foldM(Bulk()) { (bulk, doc :ChannelBuffer) =>
      logger.debug("bulk= " + bulk)
      if(reachedUpperBound(bulk.currentBulkDocsNumber + 1, bulk.docs.writerIndex + doc.writerIndex)) {
        logger.debug("inserting, at " + bulk.collectedDocs)
        coll.insert(bulk)(BulkWriter).map { _ =>
          val nextBulk = Bulk(collectedDocs = bulk.collectedDocs) +> doc
          logger.debug("redeemed, will give " + nextBulk)
          nextBulk
        }
      } else Future(bulk +> doc)
    }.flatMap { bulk =>
      logger.debug("inserting (last), at " + bulk.collectedDocs)
      Iteratee.flatten(coll.insert(bulk)(BulkWriter).map(_ => Done[ChannelBuffer, Int](bulk.collectedDocs, Input.EOF)))
    }
  }

  private[bulk] case class Bulk(
    currentBulkDocsNumber: Int = 0,
    docs: ChannelBuffer = ChannelBuffers.dynamicBuffer(LITTLE_ENDIAN, 32),
    collectedDocs: Int = 0) {

    def +> (doc: ChannelBuffer) = {
      val buf = docs.copy
      buf.writeBytes(doc)
      Bulk(currentBulkDocsNumber + 1, buf, collectedDocs + 1)
    }
  }

  private[bulk] object BulkWriter extends BSONWriter[Bulk] {
    override def write(bulk: Bulk) = bulk.docs
  }
}