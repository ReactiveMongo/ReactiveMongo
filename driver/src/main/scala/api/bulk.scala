/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.api

import java.nio.ByteOrder._
import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers }
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.iteratee._
import reactivemongo.utils.LazyLogger
import scala.concurrent.{ ExecutionContext, Future }
import reactivemongo.core.netty.ChannelBufferWritableBuffer

/**
 * Bulk insertion.
 */
object bulk {
  import reactivemongo.api.collections.buffer._
  private val logger = LazyLogger(LoggerFactory.getLogger("reactivemongo.api.Bulk"))
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
  def iteratee(coll: Collection, bulkSize: Int = MaxDocs, bulkByteSize: Int = MaxBulkSize)(implicit context: ExecutionContext): Iteratee[ChannelBuffer, Int] =
    iteratee(coll, (docs, bulk) => docs > bulkSize || bulk > bulkByteSize)

  /**
   * Returns an iteratee that will consume chunks (chunk == document) and insert them into the given collection.
   *
   * This iteratee eventually gives the number of documents that have been inserted into the given collection.
   *
   * @param coll The collection where the documents will be stored.
   * @param reachedUpperBound A function that returns true if the staging bulk can be sent to be written. This function has two parameters: numberOfDocuments and numberOfBytes.
   */
  def iteratee(coll: Collection, reachedUpperBound: (Int, Int) => Boolean)(implicit context: ExecutionContext): Iteratee[ChannelBuffer, Int] = {
    val channelColl = coll.as(coll.failoverStrategy)
    Iteratee.foldM(Bulk()) { (bulk, doc: ChannelBuffer) =>
      logger.debug("bulk= " + bulk)
      if (reachedUpperBound(bulk.currentBulkDocsNumber + 1, bulk.docs.writerIndex + doc.writerIndex)) {
        logger.debug("inserting, at " + bulk.collectedDocs)
        channelColl.insert(bulk)(BulkWriter, context).map { _ =>
          val nextBulk = Bulk(collectedDocs = bulk.collectedDocs) +> doc
          logger.debug("redeemed, will give " + nextBulk)
          nextBulk
        }
      } else Future(bulk +> doc)
    }.flatMap { bulk =>
      logger.debug("inserting (last), at " + bulk.collectedDocs)
      Iteratee.flatten(channelColl.insert(bulk)(BulkWriter, context).map(_ => Done[ChannelBuffer, Int](bulk.collectedDocs, Input.EOF)))
    }
  }

  private[bulk] case class Bulk(
      currentBulkDocsNumber: Int = 0,
      docs: ChannelBuffer = ChannelBuffers.dynamicBuffer(LITTLE_ENDIAN, 32),
      collectedDocs: Int = 0) {

    def +>(doc: ChannelBuffer) = {
      val buf = docs.copy
      buf.writeBytes(doc)
      Bulk(currentBulkDocsNumber + 1, buf, collectedDocs + 1)
    }
  }

  private[bulk] object BulkWriter extends RawBSONDocumentSerializer[Bulk] {
    override def serialize(bulk: Bulk) = new ChannelBufferWritableBuffer(bulk.docs)
  }
}