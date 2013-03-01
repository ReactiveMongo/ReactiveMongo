package reactivemongo.api.collections.buffer

import reactivemongo.bson.buffer.{BSONBuffer, ReadableBuffer, WritableBuffer}
import reactivemongo.api._
import reactivemongo.core.netty._

/**
 * A typeclass that creates a raw BSON document contained in a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] from a `DocumentType` instance.
 *
 * @tparam DocumentType The type of the instance that can be turned into a BSON document.
 */
trait RawBSONDocumentSerializer[-DocumentType] {
  def serialize(document: DocumentType) :WritableBuffer
}

/**
 * A typeclass that creates a `DocumentType` instance from a raw BSON document contained in a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam DocumentType The type of the instance to create.
 */
trait RawBSONDocumentDeserializer[+DocumentType] {
  def deserialize(buffer: ReadableBuffer) :DocumentType
}

object `package` {
  implicit object ChannelCollectionProducer extends GenericCollectionProducer[WritableBuffer, RawBSONDocumentDeserializer, RawBSONDocumentSerializer, ChannelCollection] {
    def apply(db: DB, name: String, failoverStrategy: FailoverStrategy) = new ChannelCollection(db, name, failoverStrategy)
  }
}

trait BufferGenericHandlers extends GenericHandlers[WritableBuffer, RawBSONDocumentDeserializer, RawBSONDocumentSerializer] {
  /** As seen from class P, the missing signatures are as follows. * For convenience, these are usable as stub implementations. */
  object StructureBufferReader extends BufferReader[WritableBuffer] {
    def read(buffer: ReadableBuffer) = buffer.toWritableBuffer
  }
  object StructureBufferWriter extends BufferWriter[WritableBuffer] {
    def write[B <: reactivemongo.bson.buffer.WritableBuffer](document: WritableBuffer, buffer: B): B = {
      buffer.writeBytes(document.toReadableBuffer)
      buffer
    }
  }
  case class BufferStructureReader[T](reader: RawBSONDocumentDeserializer[T]) extends GenericReader[WritableBuffer, T] {
    def read(doc: WritableBuffer) = reader.deserialize(doc.toReadableBuffer)
  }
  case class BufferStructureWriter[T](writer: RawBSONDocumentSerializer[T]) extends GenericWriter[T, WritableBuffer] {
    def write(t: T) = writer.serialize(t)
  }
  def StructureReader[T](reader: RawBSONDocumentDeserializer[T]) = BufferStructureReader(reader)
  def StructureWriter[T](writer: RawBSONDocumentSerializer[T]): GenericWriter[T, WritableBuffer] = BufferStructureWriter(writer)
}

object BufferGenericHandlers extends BufferGenericHandlers

case class ChannelCollection(
  db: DB,
  name: String,
  failoverStrategy: FailoverStrategy) extends GenericCollection[WritableBuffer, RawBSONDocumentDeserializer, RawBSONDocumentSerializer] with BufferGenericHandlers {
  def genericQueryBuilder: GenericQueryBuilder[WritableBuffer, RawBSONDocumentDeserializer, RawBSONDocumentSerializer] =
    ChannelQueryBuilder(this, failoverStrategy)
}

case class ChannelQueryBuilder(
  collection: Collection,
  failover: FailoverStrategy,
  queryOption: Option[WritableBuffer] = None,
  sortOption: Option[WritableBuffer] = None,
  projectionOption: Option[WritableBuffer] = None,
  hintOption: Option[WritableBuffer] = None,
  explainFlag: Boolean = false,
  snapshotFlag: Boolean = false,
  commentString: Option[String] = None,
  options: QueryOpts = QueryOpts()) extends GenericQueryBuilder[WritableBuffer, RawBSONDocumentDeserializer, RawBSONDocumentSerializer] with BufferGenericHandlers {

  type Self = ChannelQueryBuilder

  object structureReader extends RawBSONDocumentDeserializer[WritableBuffer] {
    def deserialize(buffer: ReadableBuffer) = buffer.toWritableBuffer
  }
  def merge() = {
    import reactivemongo.bson.BSONDocument
      def emptyDoc = {
        val buf = ChannelBufferWritableBuffer()
        reactivemongo.bson.buffer.DefaultBufferHandler.BSONDocumentBufferHandler.write(BSONDocument(), buf)
        buf
      }
      def writeDocChannel(buffer: WritableBuffer, name: String, doc: WritableBuffer) = {
        buffer.writeByte(0x03)
        buffer.writeCString(name)
        buffer.writeBytes(doc.toReadableBuffer)
      }
    val buffer = ChannelBufferWritableBuffer()
    val now = buffer.index
    buffer.writeInt(0)
    writeDocChannel(buffer, "$query", queryOption.getOrElse(emptyDoc))
    writeDocChannel(buffer, "$orderby", sortOption.getOrElse(emptyDoc))
    writeDocChannel(buffer, "$hint", hintOption.getOrElse(emptyDoc))
    // other not supported
    buffer.setInt(now, (buffer.index - now + 1))
    buffer.writeByte(0)
    buffer
  }
  def copy(queryOption: Option[WritableBuffer], sortOption: Option[WritableBuffer], projectionOption: Option[WritableBuffer], hintOption: Option[WritableBuffer], explainFlag: Boolean, snapshotFlag: Boolean, commentString: Option[String], options: QueryOpts, failover: FailoverStrategy): ChannelQueryBuilder = {
    ChannelQueryBuilder(
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
}