package org.asyncmongo.protocol

import java.nio.ByteOrder
import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.oneone._

import org.codehaus.jackson.map.ObjectMapper

import org.asyncmongo.MongoSystem

 // implicits
object `package` {
  case class ExtendedBuffer(buffer: ChannelBuffer) {
    def writeUTF8(s: String) {
      buffer writeBytes (s.getBytes("UTF-8"))
      buffer writeByte 0
    }
    def write(writable: ChannelBufferWritable) {
      writable writeTo buffer
    }
  }
  implicit def channelBufferToExtendedBuffer(buffer: ChannelBuffer) = ExtendedBuffer(buffer)
}

 // traits
trait ChannelBufferWritable extends SizeMeasurable {
  def writeTo(buffer: ChannelBuffer) :Unit
}

trait ChannelBufferReadable[T] {
  def readFrom(buffer: ChannelBuffer) :T
  def apply(buffer: ChannelBuffer) :T = readFrom(buffer)
}

trait SizeMeasurable {
  def size :Int
}

sealed trait Op {
  val code :Int
}

sealed trait WritableOp extends Op with ChannelBufferWritable

sealed trait AwaitingResponse extends WritableOp

trait BSONReader[DocumentType] {
  val count: Int
  def next: Option[DocumentType]
}
trait BSONReaderHandler[DocumentType] {
  def handle(reply: Reply, buffer: ChannelBuffer) :BSONReader[DocumentType]
}
 // concrete classes
case class MessageHeader(
  messageLength: Int,
  requestID: Int,
  responseTo: Int,
  opCode: Int
) extends ChannelBufferWritable {
  def writeTo(buffer: ChannelBuffer) {
    buffer writeInt messageLength
    buffer writeInt requestID
    buffer writeInt responseTo
    buffer writeInt opCode
  }
  override def size = 4 + 4 + 4 + 4
}

object MessageHeader extends ChannelBufferReadable[MessageHeader] {
  override def readFrom(buffer: ChannelBuffer) = MessageHeader(
    buffer.readInt,
    buffer.readInt,
    buffer.readInt,
    buffer.readInt
  )
}

case class Update(
  fullCollectionName: String,
  flags: Int
) extends WritableOp {
  override val code = 2001
  override def writeTo(buffer: ChannelBuffer) {
    buffer writeInt 0
    buffer writeUTF8 fullCollectionName
    buffer writeInt flags
  }
  override def size = 4 /* int32 = ZERO */ + 4 + fullCollectionName.length + 1
}

case class Insert(
  flags: Int,
  fullCollectionName: String
) extends WritableOp {
  override val code = 2002
  override def writeTo(buffer: ChannelBuffer) {
    buffer writeInt flags
    buffer writeUTF8 fullCollectionName
  }
  override def size = 4 + fullCollectionName.length + 1
}

case class Query(
  flags: Int,
  fullCollectionName: String,
  numberToSkip: Int,
  numberToReturn: Int
) extends AwaitingResponse {
  override val code = 2004
  override def writeTo(buffer: ChannelBuffer) {
    buffer writeInt flags
    buffer writeUTF8 fullCollectionName
    buffer writeInt numberToSkip
    buffer writeInt numberToReturn
  }
  override def size = 4 + fullCollectionName.length + 1 + 4 + 4
}

case class GetMore(
  fullCollectionName: String,
  numberToReturn: Int,
  cursorID: Long
) extends AwaitingResponse {
  override val code = 2005
  override def writeTo(buffer: ChannelBuffer) {
    buffer writeInt 0
    buffer writeUTF8 fullCollectionName
    buffer writeInt numberToReturn
    buffer writeLong cursorID
  }
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4 + 8
}

case class Delete(
  fullCollectionName: String,
  flags: Int
) extends WritableOp {
  override val code = 2006
  override def writeTo(buffer: ChannelBuffer) {
    buffer writeInt 0
    buffer writeUTF8 fullCollectionName
    buffer writeInt flags
  }
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4
}

case class KillCursors(
  cursorIDs: Set[Long]
) extends WritableOp {
  override val code = 2007
  override def writeTo(buffer: ChannelBuffer) {
    buffer writeInt cursorIDs.size
    for(cursorID <- cursorIDs) {
      buffer writeLong cursorID
    }
  }
  override def size = 4 /* int32 ZERO */ + 4 + cursorIDs.size * 8
}

case class Reply(
  flags: Int,
  cursorID: Long,
  startingFrom: Int,
  numberReturned: Int
) extends Op {
  override val code = 1
}

object Reply extends ChannelBufferReadable[Reply] {
  def readFrom(buffer: ChannelBuffer) = Reply(
    buffer.readInt,
    buffer.readLong,
    buffer.readInt,
    buffer.readInt
  )
}

case class WritableMessage[+T <: WritableOp] (
  val requestID: Int,
  val responseTo: Int,
  val op: T,
  val documents: Array[Byte]
) extends ChannelBufferWritable {
  override def writeTo(buffer: ChannelBuffer) {
    println("write into buffer, header=" + header + ", op=" + op)
    buffer write header
    buffer write op
    buffer writeBytes documents
  }
  override def size = 16 + op.size + documents.size
  lazy val header = MessageHeader(size, requestID, responseTo, op.code)
}

/* object WritableMessage {
  def apply(requestID: Int, responseTo: Int, op: WritableOp, documents: Array[Byte]) = new WritableMessage(requestID, responseTo, op, documents)
  def withResponse(requestID: Int, responseTo: Int, op: WritableOp, documents: Array[Byte]) = new WritableMessage(requestID, responseTo, op, documents) with AwaitingResponse
} */

case class ReadReply(
  header: MessageHeader,
  reply: Reply,
  documents: Array[Byte]
)

class WritableMessageEncoder extends OneToOneEncoder {
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    obj match {
      case message: WritableMessage[WritableOp] => {
        println("WritableMessageEncoder: " + message)
        val buffer :ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
        message writeTo buffer
        buffer
      }
      case _ => {
         println("WritableMessageEncoder: weird... " + obj)
         obj
      }
    }
  }
}

class ReplyDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    println("ReplyDecoder: " + obj.asInstanceOf[ChannelBuffer].factory().getDefaultOrder)
    val buffer = obj.asInstanceOf[ChannelBuffer]
    val header = MessageHeader(buffer)
    val reply = Reply(buffer)
    val json = MapReaderHandler.handle(reply, buffer).next
    println(header)
    println(reply)
    println(json)
    ReadReply(header, reply, Array())
  }
}

class MongoHandler extends SimpleChannelHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    println("handler: message received " + e.getMessage)
    MongoSystem.actor ! e.getMessage.asInstanceOf[ReadReply]
  }
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    println("connected")
  }
}

// json stuff
import org.codehaus.jackson.JsonNode

object JacksonNodeReaderHandler extends BSONReaderHandler[JsonNode] {
  override def handle(reply: Reply, buffer: ChannelBuffer) :BSONReader[JsonNode] = JacksonNodeReader(reply.numberReturned, buffer)
}

case class JacksonNodeReader(count: Int, buffer: ChannelBuffer) extends BSONReader[JsonNode] {
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._
  import de.undercouch.bson4jackson.uuid._
   private val mapper = {
    val fac = new BsonFactory()
    fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
    val om = new ObjectMapper(fac)
    om.registerModule(new BsonUuidModule())
    om
  }
  private val is = new ChannelBufferInputStream(buffer)
   override def next :Option[JsonNode] = {
    if(is.available > 0)
      Some(mapper.readValue(new ChannelBufferInputStream(buffer), classOf[JsonNode]))
    else None
  }
}

object MapReaderHandler extends BSONReaderHandler[java.util.HashMap[Object, Object]] {
  override def handle(reply: Reply, buffer: ChannelBuffer) :BSONReader[java.util.HashMap[Object, Object]] = MapReader(reply.numberReturned, buffer)
}

case class MapReader(count: Int, buffer: ChannelBuffer) extends BSONReader[java.util.HashMap[Object, Object]] {
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._
  import de.undercouch.bson4jackson.uuid._
  private val mapper = {
    val fac = new BsonFactory()
    fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
    val om = new ObjectMapper(fac)
    om.registerModule(new BsonUuidModule())
    om
  }
  private val is = new ChannelBufferInputStream(buffer)
  override def next :Option[java.util.HashMap[Object, Object]] = {
    if(is.available > 0)
      Some(mapper.readValue(new ChannelBufferInputStream(buffer), classOf[java.util.HashMap[Object, Object]]))
    else None
  }
}