package reactivemongo.core.protocol

import shaded.netty.buffer.ByteBuf

import reactivemongo.core.errors._

/**
 * A Mongo Wire Protocol Response messages.
 *
 * @param header header of this response.
 * @param reply the reply operation contained in this response.
 * @param documents body of this response, a [[http://netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 * @param info some meta information about this response, see [[reactivemongo.core.protocol.ResponseInfo]].
 */
case class Response(
  header: MessageHeader,
  reply: Reply,
  documents: ByteBuf,
  info: ResponseInfo) {
  /**
   * if this response is in error, explain this error.
   */
  lazy val error: Option[DatabaseException] = {
    if (reply.inError) {
      val bson = Response.parse(this)

      if (bson.hasNext) Some(ReactiveMongoException(bson.next))
      else None
    } else None
  }
}

object Response {
  import reactivemongo.api.BSONSerializationPack
  import reactivemongo.bson.BSONDocument
  import reactivemongo.bson.DefaultBSONHandlers.BSONDocumentIdentity

  /** Parses the response as BSON. */
  def parse(response: Response): Iterator[BSONDocument] =
    ReplyDocumentIterator(BSONSerializationPack)(
      response.reply, response.documents)(BSONDocumentIdentity)
}
