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
package reactivemongo.core.commands

import reactivemongo.bson._
import DefaultBSONHandlers._

case class CappedOptions(
  size: Long,
  maxDocuments: Option[Int] = None) {
  def toDocument = BSONDocument(
    "capped" -> BSONBoolean(true),
    "size" -> BSONLong(size),
    "max" -> maxDocuments.map(max => BSONLong(max.toLong)))
}

/**
 * Various information about a collection.
 *
 * @param ns The fully qualified collection name.
 * @param count The number of documents in this collection.
 * @param size The size in bytes (or in bytes / scale, if any).
 * @param averageObjectSize The average object size in bytes (or in bytes / scale, if any).
 * @param storageSize Preallocated space for the collection.
 * @param numExtents Number of extents (contiguously allocated chunks of datafile space).
 * @param nindexes Number of indexes.
 * @param lastExtentSize Size of the most recently created extent.
 * @param paddingFactor Padding can speed up updates if documents grow.
 * @param systemFlags System flags.
 * @param userFlags User flags.
 * @param indexSizes Size of specific indexes in bytes.
 * @param capped States if this collection is capped.
 * @param max The maximum number of documents of this collection, if capped.
 */
case class CollStatsResult(
  ns: String,
  count: Int,
  size: Double,
  averageObjectSize: Option[Double],
  storageSize: Double,
  numExtents: Int,
  nindexes: Int,
  lastExtentSize: Option[Int],
  paddingFactor: Option[Double],
  systemFlags: Option[Int],
  userFlags: Option[Int],
  totalIndexSize: Int,
  indexSizes: Array[(String, Int)],
  capped: Boolean,
  max: Option[Long])

object CollStatsResult extends BSONCommandResultMaker[CollStatsResult] {
  def apply(doc: BSONDocument): Either[CommandError, CollStatsResult] = {
    CommandError.checkOk(doc, Some("collStats")).toLeft {
      CollStatsResult(
        doc.getAs[BSONString]("ns").get.value,
        doc.getAs[BSONInteger]("count").get.value,
        doc.getAs[BSONNumberLike]("size").get.toDouble,
        doc.getAs[BSONDouble]("avgObjSize").map(_.value),
        doc.getAs[BSONNumberLike]("storageSize").get.toDouble,
        doc.getAs[BSONInteger]("numExtents").get.value,
        doc.getAs[BSONInteger]("nindexes").get.value,
        doc.getAs[BSONInteger]("lastExtentSize").map(_.value),
        doc.getAs[BSONDouble]("paddingFactor").map(_.value),
        doc.getAs[BSONInteger]("systemFlags").map(_.value),
        doc.getAs[BSONInteger]("userFlags").map(_.value),
        doc.getAs[BSONInteger]("totalIndexSize").get.value,
        {
          val indexSizes = doc.getAs[BSONDocument]("indexSizes").get
          (for (kv <- indexSizes.elements) yield kv.name -> kv.value.asInstanceOf[BSONInteger].value).toArray
        },
        doc.getAs[BSONBooleanLike]("capped").map(_.toBoolean).getOrElse(false),
        doc.getAs[BSONDouble]("max").map(_.value.toLong))
    }
  }
}
