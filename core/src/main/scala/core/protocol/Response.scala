package reactivemongo.core.protocol

import scala.util.{ Failure, Success }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.core.errors.{ DatabaseException, GenericDriverException }
import reactivemongo.api.bson.collection.BSONSerializationPack

/**
 * A Mongo Wire Protocol Response messages.
 *
 * @param header the header of this response
 * @param reply the reply operation contained in this response
 * @param documents the body of this response, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents
 * @param info some meta information about this response
 */
private[reactivemongo] sealed abstract class Response(
  val header: MessageHeader,
  val reply: Reply,
  val documents: ByteBuf,
  val info: ResponseInfo) {

  /** If this response is in error, explain this error. */
  lazy val error: Option[DatabaseException] = {
    if (reply.inError) {
      val bson = Response.parse(this)

      if (bson.hasNext) {
        Some(DatabaseException(BSONSerializationPack)(bson.next()))
      } else None
    } else None
  }

  private[reactivemongo] def cursorID(id: Long): Response

  private[reactivemongo] def startingFrom(offset: Int): Response

  override def toString = s"Response($header, $reply, $info)"
}

private[reactivemongo] object Response {
  import reactivemongo.api.SerializationPack
  import reactivemongo.api.bson.BSONDocument

  def apply(
    header: MessageHeader,
    reply: Reply,
    documents: ByteBuf,
    info: ResponseInfo): Response = Successful(header, reply, documents, info)

  @inline def parse(response: Response): Iterator[BSONDocument] =
    parse(BSONSerializationPack)(response)

  def parse[P <: SerializationPack](pack: P)(
    response: Response): Iterator[pack.Document] =
    ReplyDocumentIterator.parse(pack)(response)(pack.IdentityReader)

  def preload(response: Response)(
    implicit
    ec: ExecutionContext): Future[(Response, BSONDocument)] =
    response match {
      case r @ WithCursor(_, _, _, _, _, cursorDoc, _) =>
        Future.successful(r -> cursorDoc)

      case CommandError(_, _, _, cause) =>
        Future.failed(cause)

      case Successful(_, Reply(_, _, _, 0), _, _) =>
        Future.failed(new GenericDriverException(
          s"Cannot preload empty response: $response"))

      case Successful(header, reply, docs, info) => {
        val buf = docs.duplicate()

        ResponseDecoder.first(buf) match {
          case Success(first) => Future {
            buf.resetReaderIndex()

            val other = Successful(header, reply, buf, info)
            other.first = Option(first)

            other -> first
          }

          case Failure(cause) => Future.failed(cause)
        }
      }
    }

  // ---

  private[reactivemongo] final case class Successful(
    _header: MessageHeader,
    _reply: Reply,
    _documents: ByteBuf,
    _info: ResponseInfo) extends Response(
    _header, _reply, _documents, _info) {

    @volatile private[reactivemongo] var first = Option.empty[BSONDocument]

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
    private[core]cursor: BSONDocument,
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
