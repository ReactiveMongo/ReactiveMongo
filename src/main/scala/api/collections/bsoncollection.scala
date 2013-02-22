package reactivemongo.api.collections.default

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.buffer._
import reactivemongo.core.netty._
import org.jboss.netty.buffer.ChannelBuffer

/**
 * The default [[Collection]] implementation.
 */
object `package` {
  implicit object BSONCollectionProducer extends GenericCollectionProducer[BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONCollection] {
    def apply(db: DB, name: String, failoverStrategy: FailoverStrategy) = new BSONCollection(db, name, failoverStrategy)
  }
}

case class BSONDocumentReaderAsBufferReader[T](reader: BSONDocumentReader[T]) extends BufferReader[T] {
  def read(buffer: ReadableBuffer) = reader.read(BSONDocument.read(buffer))
}

protected sealed trait BSONConverters {
  protected def writeStructureIntoBuffer[B <: reactivemongo.bson.buffer.WritableBuffer](document: reactivemongo.bson.BSONDocument, buffer: B): B = {
    BSONDocument.write(document, buffer)
    buffer
  }

  protected def toStructure[T](writer: BSONDocumentWriter[T], subject: T) = writer.write(subject)

  def convert[T](reader: BSONDocumentReader[T]): BufferReader[T] = BSONDocumentReaderAsBufferReader(reader)
}

trait BSONGenericHandlers extends GenericHandlers[BSONDocument, BSONDocumentReader, BSONDocumentWriter] {
  object StructureBufferReader extends BufferReader[BSONDocument] {
    def read(buffer: ReadableBuffer) = BSONDocument.read(buffer)
  }
  object StructureBufferWriter extends BufferWriter[BSONDocument] {
    def write[B <: reactivemongo.bson.buffer.WritableBuffer](document: reactivemongo.bson.BSONDocument, buffer: B): B = {
      BSONDocument.write(document, buffer)
      buffer
    }
  }
  case class BSONStructureReader[T](reader: BSONDocumentReader[T]) extends GenericReader[BSONDocument, T] {
    def read(doc: BSONDocument) = reader.read(doc)
  }
  case class BSONStructureWriter[T](writer: BSONDocumentWriter[T]) extends GenericWriter[T, BSONDocument] {
    def write(t: T) = writer.write(t)
  }
  def StructureReader[T](reader: BSONDocumentReader[T]) = BSONStructureReader(reader)
  def StructureWriter[T](writer: BSONDocumentWriter[T]): GenericWriter[T, BSONDocument] = BSONStructureWriter(writer)
}

object BSONGenericHandlers extends BSONGenericHandlers

/**
 * The default implementation of [[Collection]].
 * 
 * {{{
 * import play.api.libs.iteratee.Iteratee
 * import reactivemongo.api.MongoConnection
 * import reactivemongo.bson._
 *
 * object Samples {
 * val connection = MongoConnection(List("localhost"))
 *
 * // Gets a reference to the database "plugin"
 * val db = connection("plugin")
 *
 * // Gets a reference to the collection "acoll"
 * // By default, you get a BSONCollection.
 * val collection = db("acoll")
 *
 * def listDocs() = {
 * // Select only the documents which field 'firstName' equals 'Jack'
 * val query = BSONDocument(
 * "firstName" -> "Jack")
 *
 * // Select only the field 'lastName'
 * val filter = BSONDocument(
 * "lastName" -> 1,
 * "_id" -> 0)
 *
 * // Get a cursor of BSONDocuments
 * val cursor = collection.find(query, filter).cursor
 * // Let's enumerate this cursor and print a readable representation of each document in the response
 * cursor.enumerate.apply(Iteratee.foreach { doc =>
 * println("found document: " + BSONDocument.pretty(doc))
 * })
 *
 * // Or, the same with getting a list
 * val cursor2 = collection.find(query, filter).cursor
 * val futureList = cursor.toList
 * futureList.map { list =>
 * list.foreach { doc =>
 * println("found document: " + BSONDocument.pretty(doc))
 * }
 * }
 * }
 * }
 * }}}
 */
case class BSONCollection(
  db: DB,
  name: String,
  failoverStrategy: FailoverStrategy) extends GenericCollection[BSONDocument, BSONDocumentReader, BSONDocumentWriter] with BSONGenericHandlers with CollectionMetaCommands {
  def genericQueryBuilder: GenericQueryBuilder[BSONDocument, BSONDocumentReader, BSONDocumentWriter] =
    BSONQueryBuilder(this, failoverStrategy)
}

case class BSONQueryBuilder(
  collection: Collection,
  failover: FailoverStrategy,
  queryOption: Option[BSONDocument] = None,
  sortOption: Option[BSONDocument] = None,
  projectionOption: Option[BSONDocument] = None,
  hintOption: Option[BSONDocument] = None,
  explainFlag: Boolean = false,
  snapshotFlag: Boolean = false,
  commentString: Option[String] = None,
  options: QueryOpts = QueryOpts()) extends GenericQueryBuilder[BSONDocument, BSONDocumentReader, BSONDocumentWriter] with BSONConverters with BSONGenericHandlers {
  import reactivemongo.bson._
  import reactivemongo.bson.DefaultBSONHandlers._
  import reactivemongo.utils.option

  type Self = BSONQueryBuilder

  def structureReader: BSONDocumentReader[BSONDocument] = DefaultBSONHandlers.BSONDocumentIdentity

  def copy(queryOption: Option[BSONDocument], sortOption: Option[BSONDocument], projectionOption: Option[BSONDocument], hintOption: Option[BSONDocument], explainFlag: Boolean, snapshotFlag: Boolean, commentString: Option[String], options: QueryOpts, failover: FailoverStrategy): BSONQueryBuilder = {
    BSONQueryBuilder(
      collection,
      failover,
      queryOption,
      sortOption,
      projectionOption,
      hintOption,
      explainFlag,
      snapshotFlag,
      commentString,
      options)
  }

  def merge: BSONDocument = {
    if (!sortOption.isDefined && !hintOption.isDefined && !explainFlag && !snapshotFlag && !commentString.isDefined)
      queryOption.getOrElse(BSONDocument())
    else
      BSONDocument(
        "$query" -> queryOption.getOrElse(BSONDocument()),
        "$orderby" -> sortOption,
        "$hint" -> hintOption,
        "$comment" -> commentString.map(BSONString(_)),
        "$explain" -> option(explainFlag, BSONBoolean(true)),
        "$snapshot" -> option(snapshotFlag, BSONBoolean(true)))
  }
}
