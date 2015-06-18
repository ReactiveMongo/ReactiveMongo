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
package reactivemongo.core.protocol

import java.nio.ByteOrder

import akka.util.{ByteString, ByteStringBuilder}
import reactivemongo.api.commands.GetLastError
import reactivemongo.api.{ReadPreference, SerializationPack}
import reactivemongo.bson.buffer.ReadableBuffer
import reactivemongo.core.AkkaReadableBuffer
import reactivemongo.core.errors._
import reactivemongo.core.protocol.ByteStringBuilderHelper._

trait ByteStringBuffer {
  def append : ByteStringBuilder => Unit
  def size: Int
  def message = {
    val builder = new ByteStringBuilder
    append(builder)
    builder.result()
  }
}

trait BufferReadable[T] {
  /** Makes an instance of T from the data from the given buffer. */
  def readFrom(buffer: ReadableBuffer): T
  /** @see readFrom */
  def apply(buffer: ReadableBuffer): T = readFrom(buffer)
}

trait ReadableFrom[B, T] {
  def readFrom(buffer: B) : T
  def apply(buffer: B) : T =  readFrom(buffer)
}


// concrete classes
/**
 * Header of a Mongo Wire Protocol message.
 *
 * @param messageLength length of this message.
 * @param requestID id of this request (> 0 for request operations, else 0).
 * @param responseTo id of the request that the message including this a response to (> 0 for reply operation, else 0).
 * @param opCode operation code of this message.
 */
case class MessageHeader(
    messageLength: Int,
    requestID: Int,
    responseTo: Int,
    opCode: Int) extends ByteStringBuffer {
  override def size = 4 + 4 + 4 + 4

  override val append  = { builder: ByteStringBuilder =>
      import ByteStringBuilderHelper._
      write(messageLength, requestID, responseTo, opCode)(builder)
  }
}

/** Header deserializer from a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]]. */
object MessageHeader extends ReadableFrom[ByteString, MessageHeader] {
  val size = 4 + 4 + 4 + 4

  override def readFrom(buffer: ByteString): MessageHeader = {
    val iterator = buffer.iterator
    MessageHeader(
      iterator.getInt,
      iterator.getInt,
      iterator.getInt,
      iterator.getInt)
  }
}

/**
 * Request message.
 *
 * @param requestID id of this request, so that the response may be identifiable. Should be strictly positive.
 * @param op request operation.
 * @param documents body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
case class Request(
    requestID: Int,
    responseTo: Int, // TODO remove, nothing to do here.
    op: RequestOp,
    documents: ByteString,
    readPreference: ReadPreference = ReadPreference.primary,
    channelIdHint: Option[Int] = None) extends ByteStringBuffer {


  override def size = 16 + op.size + documents.size
  /** Header of this request */
  lazy val header = MessageHeader(size, requestID, responseTo, op.code)

  override def append = (builder: ByteStringBuilder) => {
    header append builder
    op append builder
    builder.append(documents)
  }
}

/**
 * A helper to build write request which result needs to be checked (by sending a [[reactivemongo.core.commands.GetLastError]] command after).
 *
 * @param op write operation.
 * @param documents body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param getLastError a [[reactivemongo.core.commands.GetLastError]] command message.
 */
case class CheckedWriteRequest(
    op: WriteRequestOp,
    documents: ByteString,
    getLastError: GetLastError) {
  def apply(): (RequestMaker, RequestMaker) = {
    import reactivemongo.api.BSONSerializationPack
    import reactivemongo.api.commands.Command
    import reactivemongo.api.commands.bson.BSONGetLastErrorImplicits.GetLastErrorWriter
    val gleRequestMaker = Command.requestMaker(BSONSerializationPack).onDatabase(op.db, getLastError, ReadPreference.primary)(GetLastErrorWriter).requestMaker
    RequestMaker(op, documents) -> gleRequestMaker
  }
}

/**
 * A helper to build requests.
 *
 * @param op write operation.
 * @param documents body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
case class RequestMaker(
    op: RequestOp,
    documents: ByteString = ByteString.empty,
    readPreference: ReadPreference = ReadPreference.primary,
    channelIdHint: Option[Int] = None) {
  def apply(id: Int) = Request(id, 0, op, documents, readPreference, channelIdHint)
}

/**
 * @define requestID id of this request, so that the response may be identifiable. Should be strictly positive.
 * @define op request operation.
 * @define documentsA body of this request, an Array containing 0, 1, or many documents.
 * @define documentsC body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 */
object Request {
  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   * @param documents $documentsA
   */
  def apply(requestID: Int, responseTo: Int, op: RequestOp, documents: Array[Byte]): Request = Request(
    requestID,
    responseTo,
    op,
    ByteString(documents))
}

/**
 * A Mongo Wire Protocol Response messages.
 *
 * @param header header of this response.
 * @param reply the reply operation contained in this response.
 * @param documents body of this response, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param channelId chennelId(actualy port number) through which the [[Response]]] was received.
 */
case class Response(
    header: MessageHeader,
    reply: Reply,
    documents: ByteString,
    channelId: Int) {
  /**
   * if this response is in error, explain this error.
   */
  lazy val error: Option[DatabaseException] = {
    if (reply.inError) {
      val bson = Response.parse(this)
      //val bson = ReplyDocumentIterator(reply, documents)
      if (bson.hasNext)
        Some(ReactiveMongoException(bson.next))
      else None
    } else None
  }
}

object Response {
  import reactivemongo.api.BSONSerializationPack
  import reactivemongo.bson.BSONDocument
  import reactivemongo.bson.DefaultBSONHandlers.BSONDocumentIdentity
  //import reactivemongo.api.collections.default.BSONDocumentReaderAsBufferReader

  def parse(response: Response): Iterator[BSONDocument] =
    ReplyDocumentIterator(BSONSerializationPack)(response.reply, response.documents)(BSONDocumentIdentity)
}

sealed trait MongoWireVersion extends Ordered[MongoWireVersion] {
  def value: Int

  final def compare(x: MongoWireVersion): Int =
    if (value == x.value) 0
    else if (value < x.value) -1
    else 1
}

object MongoWireVersion {
  /*
   * Original meaning of MongoWireVersion is more about protocol features.
   *
   * - RELEASE_2_4_AND_BEFORE (0)
   * - AGG_RETURNS_CURSORS (1)
   * - BATCH_COMMANDS (2)
   *
   * But wireProtocol=1 is virtually non-existent; Mongo 2.4 was 0 and Mongo 2.6 is 2.
   */
  object V24AndBefore extends MongoWireVersion { val value = 0 }
  object V26 extends MongoWireVersion { val value = 2 }
  object V30 extends MongoWireVersion { val value = 3 }

  def apply(v: Int): MongoWireVersion = 
    if (v >= V30.value) V30
    else if (v >= V26.value) V26
    else V24AndBefore

  def unapply(v: MongoWireVersion): Option[Int] = Some(v.value)
}


object ReplyDocumentIterator  {
  def apply[P <: SerializationPack, A](pack: P)(reply: Reply, buffer: ByteString)(implicit reader: pack.Reader[A]): Iterator[A] = new Iterator[A] {
    implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
    private var tail = buffer
    override def hasNext = tail.size > 4
    override def next =
      try {
        val l = tail.iterator.getInt
        val splitted2 = tail.splitAt(l)
        tail = splitted2._2
        val cbrb = new AkkaReadableBuffer(splitted2._1) //ChannelBufferReadableBuffer(buffer.readBytes(buffer.getInt(buffer.readerIndex)))
        val item = pack.readAndDeserialize(cbrb, reader)
        item
      } catch {
        case e: IndexOutOfBoundsException =>
          /*
           * If this happens, the buffer is exhausted, and there is probably a bug.
           * It may happen if an enumerator relying on it is concurrently applied to many iteratees – which should not be done!
           */
          throw new ReplyDocumentIteratorExhaustedException(e)
      }
  }
}

case class ReplyDocumentIteratorExhaustedException(
  val cause: Exception) extends Exception(cause)

