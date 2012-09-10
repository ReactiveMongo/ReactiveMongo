package reactivemongo.core.commands

import reactivemongo.bson._
import reactivemongo.core.protocol.Response

case class CappedOptions(
  size: Long,
  maxDocuments: Option[Int] = None
) {
  def write(doc: AppendableBSONDocument) :AppendableBSONDocument = {
    doc +=  ("capped" -> BSONBoolean(true), "size" -> BSONLong(size))
    if(maxDocuments.isDefined)
      doc += "max" -> BSONLong(maxDocuments.get)
    else doc
  }
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
class CreateCollection(
  name: String,
  capped: Option[CappedOptions] = None,
  autoIndexId: Option[Boolean] = None
) extends Command {
  def makeDocuments = {
    val doc = BSONDocument("create" -> BSONString(name))
    if(capped.isDefined)
      capped.get.write(doc)
    if(autoIndexId.isDefined)
      doc += "autoIndexId" -> BSONBoolean(autoIndexId.get)
    doc
  }

  type Result = Boolean

  object ResultMaker extends BSONCommandResultMaker[Boolean] {
    def apply(doc: TraversableBSONDocument)  = {
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
class ConvertToCapped(
  name: String,
  capped: CappedOptions
) extends Command {
  def makeDocuments = {
    val doc = BSONDocument("convertToCapped" -> BSONString(name))
    capped.write(doc)
  }

  type Result = Boolean

  object ResultMaker extends BSONCommandResultMaker[Boolean] {
    def apply(doc: TraversableBSONDocument) = {
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
class CollStats(
  name: String,
  scale :Option[Int] = None
) extends Command {
  def makeDocuments = {
    val doc = BSONDocument("collStats" -> BSONString(name))
    if(scale.isDefined)
      doc += "scale" -> BSONInteger(scale.get)
    else doc
  }

  type Result = CollStatsResult

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
  size: Int,
  averageObjectSize: Option[Int],
  storageSize: Int,
  numExtents: Int,
  nindexes: Int,
  lastExtentSize: Int,
  paddingFactor: Double,
  systemFlags: Int,
  userFlags: Int,
  totalIndexSize: Int,
  indexSizes: Array[(String, Int)],
  capped: Boolean,
  max: Option[Long]
)

object CollStatsResult extends BSONCommandResultMaker[CollStatsResult] {
  def apply(doc: TraversableBSONDocument) = {
    CommandError.checkOk(doc, Some("collStats")).toLeft(
      CollStatsResult(
        doc.getAs[BSONString]("ns").get.value,
        doc.getAs[BSONInteger]("count").get.value,
        doc.getAs[BSONInteger]("size").get.value,
        doc.getAs[BSONInteger]("avgObjSize").map(_.value),
        doc.getAs[BSONInteger]("storageSize").get.value,
        doc.getAs[BSONInteger]("numExtents").get.value,
        doc.getAs[BSONInteger]("nindexes").get.value,
        doc.getAs[BSONInteger]("lastExtentSize").get.value,
        doc.getAs[BSONDouble]("paddingFactor").get.value,
        doc.getAs[BSONInteger]("systemFlags").get.value,
        doc.getAs[BSONInteger]("userFlags").get.value,
        doc.getAs[BSONInteger]("totalIndexSize").get.value,
        {
          val indexSizes = doc.getAs[TraversableBSONDocument]("indexSizes").get
          (for(kv <- indexSizes.mapped) yield kv._1 -> kv._2.asInstanceOf[BSONInteger].value).toArray
        },
        doc.getAs[BSONBoolean]("capped").map(_.value).getOrElse(false),
        doc.getAs[BSONDouble]("max").map(_.value.toLong)
      )
    )
  }
}