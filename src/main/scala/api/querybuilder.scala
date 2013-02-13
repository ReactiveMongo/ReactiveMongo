package reactivemongo.api

import org.jboss.netty.buffer.ChannelBuffer
import reactivemongo.bson._
import DefaultBSONHandlers._
import reactivemongo.bson.handlers._
import reactivemongo.core.protocol.QueryFlags
import reactivemongo.utils._

sealed trait SortOrder
object SortOrder {
  case object Ascending extends SortOrder
  case object Descending extends SortOrder
}

/**
 * A helper to make query documents.
 *
 * You may use the methods to set the fields of this class, or set them directly.
 * Reading the [[http://www.mongodb.org/display/DOCS/Advanced+Queries documentation about advanced queries]] may be very useful.
 *
 * @param queryDoc The query itself (actually the selector)
 * @param sortDoc The sorting document.
 * @param hintDoc The index hint document.
 * @param explainFlag The explain mode setter.
 * @param snapshotFlag The snapshot mode setter.
 * @param commentString A comment about this query.
 */
case class QueryBuilder(
  queryDoc: Option[BSONDocument] = None,
  sortDoc: Option[BSONDocument] = None,
  hintDoc: Option[BSONDocument] = None,
  projectionDoc: Option[BSONDocument] = None,
  explainFlag: Boolean = false,
  snapshotFlag: Boolean = false,
  commentString: Option[String] = None
) {
  /** Builds the query document by merging all the params. */
  def makeQueryDocument :BSONDocument = {
    if(!sortDoc.isDefined && !hintDoc.isDefined && !explainFlag && !snapshotFlag && !commentString.isDefined)
      queryDoc.getOrElse(BSONDocument())
    else {
      BSONDocument(
        "$query"    -> queryDoc.getOrElse(BSONDocument()),
        "$orderby"  -> sortDoc,
        "$hint"     -> hintDoc,
        "$comment"  -> commentString.map(BSONString(_)),
        "$explain"  -> option(explainFlag,  BSONBoolean(true)),
        "$snapshot" -> option(snapshotFlag, BSONBoolean(true))
      )
    }
  }

  def makeMergedBuffer :ChannelBuffer = {
    val buffer = makeQueryDocument.makeBuffer
    if(projectionDoc.isDefined)
      buffer.writeBytes(projectionDoc.get.makeBuffer)
    buffer
  }

  /**
   * Sets the query (the selector document).
   *
   * @tparam Qry The type of the query. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][Qry] typeclass for handling it has to be in the scope.
   */
  def query[Qry](selector: Qry)(implicit writer: RawBSONDocumentWriter[Qry]) :QueryBuilder = copy(queryDoc=Some(
    writer.write(selector).makeDocument
  ))

  /** Sets the query (the selector document). */
  def query(selector: BSONDocument) :QueryBuilder = copy(queryDoc=Some(selector))

  /** Sets the sorting document. */
  def sort(document: BSONDocument) :QueryBuilder = copy(sortDoc=Some(document))

  /** Sets the sorting document. */
  def sort( sorters: (String, SortOrder)* ) :QueryBuilder = copy(sortDoc = {
    if(sorters.size == 0)
      None
    else {
      val bson = BSONDocument(
        (for(sorter <- sorters) yield sorter._1 -> BSONInteger(
          sorter._2 match {
            case SortOrder.Ascending => 1
            case SortOrder.Descending => -1
          }
        )).toStream
      )
      Some(bson)
    }
  })

  /**
   * Sets the projection document (for [[http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields retrieving only a subset of fields]]).
   *
   * @tparam Pjn The type of the projection. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][Pjn] typeclass for handling it has to be in the scope.
   */
  def projection[Pjn](p: Pjn)(implicit writer: RawBSONDocumentWriter[Pjn]) :QueryBuilder = copy(projectionDoc=Some(
    writer.write(p).makeDocument
  ))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  def hint(document: BSONDocument) :QueryBuilder = copy(hintDoc=Some(document))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  def hint(indexName: String) :QueryBuilder = copy(hintDoc=Some(BSONDocument(indexName -> BSONInteger(1))))

  //TODO def explain(flag: Boolean = true) :QueryBuilder = copy(explainFlag=flag)

  /** Toggles [[http://www.mongodb.org/display/DOCS/How+to+do+Snapshotted+Queries+in+the+Mongo+Database snapshot mode]]. */
  def snapshot(flag: Boolean = true) :QueryBuilder = copy(snapshotFlag=flag)

  /** Adds a comment to this query, that may appear in the MongoDB logs. */
  def comment(message: String) :QueryBuilder = copy(commentString=Some(message))
}

/**
 * A helper to make the query options.
 *
 * You may use the methods to set the fields of this class, or set them directly.
 *
 * @param skipN The number of documents to skip.
 * @param batchSizeN The upper limit on the number of documents to retrieve per batch.
 * @param flagsN The query flags.
 */
case class QueryOpts(
  skipN: Int = 0,
  batchSizeN: Int = 0,
  flagsN: Int = 0
) {
  /** Sets the number of documents to skip. */
  def skip(n: Int) = copy(skipN = n)
  /** Sets an upper limit on the number of documents to retrieve per batch. Defaults to 0 (meaning no upper limit - MongoDB decides). */
  def batchSize(n: Int) = copy(batchSizeN = n)
  /** Sets the query flags. */
  def flags(n: Int) = copy(flagsN = n)

  /** Toggles TailableCursor: Makes the cursor not to close after all the data is consumed. */
  def tailable        = copy(flagsN = flagsN ^ QueryFlags.TailableCursor)
  /** Toggles SlaveOk: The query is might be run on a secondary. */
  def slaveOk         = copy(flagsN = flagsN ^ QueryFlags.SlaveOk)
  /** Toggles OplogReplay */
  def oplogReplay     = copy(flagsN = flagsN ^ QueryFlags.OplogReplay)
  /** Toggles NoCursorTimeout: The cursor will not expire automatically */
  def noCursorTimeout = copy(flagsN = flagsN ^ QueryFlags.NoCursorTimeout)
  /**
   * Toggles AwaitData: Block a little while waiting for more data instead of returning immediately if no data.
   * Use along with TailableCursor.
   */
  def awaitData       = copy(flagsN = flagsN ^ QueryFlags.AwaitData)
  /** Toggles Exhaust */
  def exhaust         = copy(flagsN = flagsN ^ QueryFlags.Exhaust)
  /** Toggles Partial: The response can be partial - if a shard is down, no error will be thrown. */
  def partial         = copy(flagsN = flagsN ^ QueryFlags.Partial)
}