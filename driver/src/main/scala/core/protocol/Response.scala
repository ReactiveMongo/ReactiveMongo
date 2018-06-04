package reactivemongo.core.protocol

import shaded.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.core.errors._

import reactivemongo.api.SerializationPack

/**
 * A Mongo Wire Protocol Response messages.
 *
 * @param header the header of this response
 * @param reply the reply operation contained in this response
 * @param documents the body of this response, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents
 * @param info some meta information about this response, see [[reactivemongo.core.protocol.ResponseInfo]]
 */
sealed abstract class Response(
  val header: MessageHeader,
  val reply: Reply,
  val documents: ByteBuf,
  val info: ResponseInfo) extends Product4[MessageHeader, Reply, ByteBuf, ResponseInfo] with Serializable {
  @inline def _1 = header
  @inline def _2 = reply
  @inline def _3 = documents
  @inline def _4 = info

  def canEqual(that: Any): Boolean = that match {
    case _: Response => true
    case _           => false
  }

  /** If this response is in error, explain this error. */
  lazy val error: Option[DatabaseException] = {
    if (reply.inError) {
      val bson = Response.parse(this)

      if (bson.hasNext) Some(DatabaseException(bson.next))
      else None
    } else None
  }

  private[reactivemongo] def cursorID(id: Long): Response

  private[reactivemongo] def startingFrom(offset: Int): Response

  override def toString = s"Response($header, $reply, $info)"
}

object Response {
  import reactivemongo.api.BSONSerializationPack
  import reactivemongo.bson.BSONDocument
  import reactivemongo.bson.DefaultBSONHandlers.BSONDocumentIdentity

  def apply(
    header: MessageHeader,
    reply: Reply,
    documents: ByteBuf,
    info: ResponseInfo): Response = Successful(header, reply, documents, info)

  def parse(response: Response): Iterator[BSONDocument] = parse[BSONSerializationPack.type, BSONDocument](BSONSerializationPack)(response, BSONDocumentIdentity)

  @inline private[reactivemongo] def parse[P <: SerializationPack, T](
    pack: P)(response: Response, reader: pack.Reader[T]): Iterator[T] =
    ReplyDocumentIterator.parse(pack)(response)(reader)

  def unapply(response: Response): Option[(MessageHeader, Reply, ByteBuf, ResponseInfo)] = Some((response.header, response.reply, response.documents, response.info))

  // ---

  private[reactivemongo] final case class Successful(
    _header: MessageHeader,
    _reply: Reply,
    _documents: ByteBuf,
    _info: ResponseInfo) extends Response(
    _header, _reply, _documents, _info) {

    private[reactivemongo] def cursorID(id: Long): Response =
      copy(_reply = this._reply.copy(cursorID = id))

    private[reactivemongo] def startingFrom(offset: Int): Response =
      copy(_reply = this._reply.copy(startingFrom = offset))
  }

  // For MongoDB 3.2+ response with cursor
  private[reactivemongo] final case class WithCursor(
    _header: MessageHeader,
    _reply: Reply,
    _documents: ByteBuf,
    _info: ResponseInfo,
    ns: String,
    private[core]preloaded: Seq[BSONDocument]) extends Response(
    _header, _reply, _documents, _info) {
    private[reactivemongo] def cursorID(id: Long): Response =
      copy(_reply = this._reply.copy(cursorID = id))

    private[reactivemongo] def startingFrom(offset: Int): Response =
      copy(_reply = this._reply.copy(startingFrom = offset))
  }

  private[reactivemongo] final case class CommandError(
    _header: MessageHeader,
    _reply: Reply,
    _info: ResponseInfo,
    private[reactivemongo]cause: DatabaseException) extends Response(_header, _reply, Unpooled.EMPTY_BUFFER, _info) {
    override lazy val error: Option[DatabaseException] = Some(cause)

    private[reactivemongo] def cursorID(id: Long): Response =
      copy(_reply = this._reply.copy(cursorID = id))

    private[reactivemongo] def startingFrom(offset: Int): Response =
      copy(_reply = this._reply.copy(startingFrom = offset))
  }
}
