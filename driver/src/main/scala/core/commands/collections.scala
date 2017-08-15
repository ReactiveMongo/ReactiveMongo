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
 * Creates a collection on the database.
 *
 * Unless you want to give some extra parameters (like capped collections, or autoIndexId), it is not necessary as MongoDB creates regular collections on the fly.
 *
 * @param name The collection name.
 * @param capped The collection to be created will be a capped collection if some CappedOptions instance is supplied.
 * @param autoIndexId States if should automatically add an index on the _id field. By default, regular collections will have an indexed _id field, in contrast to capped collections.
 */
@deprecated("consider using reactivemongo.api.commands.Create instead", "0.11.0")
class CreateCollection(
  name: String,
  capped: Option[CappedOptions] = None,
  autoIndexId: Option[Boolean] = None) extends Command[Boolean] {
  def makeDocuments = {
    val doc = BSONDocument(
      "create" -> BSONString(name),
      "autoIndexId" -> autoIndexId.map(BSONBoolean(_)))
    if (capped.isDefined)
      doc ++ capped.get.toDocument
    else doc
  }

  object ResultMaker extends BSONCommandResultMaker[Boolean] {
    def apply(doc: BSONDocument) = {
      CommandError.checkOk(doc, Some("createCollection")).toLeft(true)
    }
  }
}

/**
 * Converts a regular collection into a capped one.
 *
 * @param name The collection name.
 * @param capped The capped collection options.
 */
@deprecated("consider using reactivemongo.api.commands.ConvertToCapped instead", "0.11.0")
class ConvertToCapped(
  name: String,
  capped: CappedOptions) extends Command[Boolean] {
  def makeDocuments = {
    BSONDocument("convertToCapped" -> BSONString(name)) ++ capped.toDocument
  }

  object ResultMaker extends BSONCommandResultMaker[Boolean] {
    def apply(doc: BSONDocument) = {
      CommandError.checkOk(doc, Some("convertToCapped")).toLeft(true)
    }
  }
}

/**
 * Returns a document containing various information about a collection.
 *
 * @param name The collection name.
 * @param scale A number to divide the numbers representing size (useful for getting sizes in kB (1024) or MB (1024 * 1024)).
 */
@deprecated("consider using reactivemongo.api.commands.CollStats instead", "0.11.0")
class CollStats(
  name: String,
  scale: Option[Int] = None) extends Command[CollStatsResult] {
  def makeDocuments = {
    BSONDocument(
      "collStats" -> BSONString(name),
      "scale" -> scale.map(BSONInteger(_)))
  }

  val ResultMaker = CollStatsResult
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
    // TODO support sharding info
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

/**
 * Drop a collection.
 *
 * @param name The collection name.
 */
@deprecated("consider using reactivemongo.api.commands.Drop instead", "0.11.0")
class Drop(
  name: String) extends Command[Boolean] {
  def makeDocuments =
    BSONDocument("drop" -> BSONString(name))

  object ResultMaker extends BSONCommandResultMaker[Boolean] {
    def apply(doc: BSONDocument) = {
      CommandError.checkOk(doc, Some("drop")).toLeft(true)
    }
  }
}

/**
 * Empty a capped collection.
 *
 * @param name The collection name.
 */
@deprecated("consider using reactivemongo.api.commands.EmptyCapped instead", "0.11.0")
class EmptyCapped(
  name: String) extends Command[Boolean] {
  def makeDocuments =
    BSONDocument("emptycapped" -> BSONString(name))

  object ResultMaker extends BSONCommandResultMaker[Boolean] {
    def apply(doc: BSONDocument) = {
      CommandError.checkOk(doc, Some("emptycapped")).toLeft(true)
    }
  }
}

/**
 * Rename a collection.
 *
 * @param name The collection name.
 * @param target The new name of the collection.
 * @param dropTarget If a collection of name `target` already exists, drop it before renaming this collection.
 */
@deprecated("consider using reactivemongo.api.commands.RenameCollection instead", "0.11.0")
class RenameCollection(
  name: String,
  target: String,
  dropTarget: Boolean = false) extends AdminCommand[Boolean] {
  def makeDocuments =
    BSONDocument(
      "renameCollection" -> BSONString(name),
      "to" -> BSONString(target),
      "dropTarget" -> BSONBoolean(dropTarget))

  object ResultMaker extends BSONCommandResultMaker[Boolean] {
    def apply(doc: BSONDocument) = {
      CommandError.checkOk(doc, Some("renameCollection")).toLeft(true)
    }
  }
}
