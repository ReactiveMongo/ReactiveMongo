package reactivemongo.core.protocol

import scala.util.{ Failure, Success }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.core.errors._

/**
 * A Mongo Wire Protocol Response messages.
 *
 * @param header the header of this response
 * @param reply the reply operation contained in this response
 * @param documents the body of this response, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents
 * @param info some meta information about this response
 */
@deprecated("Will be private/internal", "0.16.0")
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

@deprecated("Will be private/internal", "0.16.0")
object Response {
  import reactivemongo.api.BSONSerializationPack
  import reactivemongo.bson.BSONDocument
  import reactivemongo.bson.DefaultBSONHandlers.BSONDocumentIdentity

  def apply(
    header: MessageHeader,
    reply: Reply,
    documents: ByteBuf,
    info: ResponseInfo): Response = Successful(header, reply, documents, info)

  def parse(response: Response): Iterator[BSONDocument] =
    ReplyDocumentIterator.parse(BSONSerializationPack)(
      response)(BSONDocumentIdentity)

  def unapply(response: Response): Option[(MessageHeader, Reply, ByteBuf, ResponseInfo)] = Some((response.header, response.reply, response.documents, response.info))

  private[reactivemongo] def preload(response: Response)(
    implicit
    ec: ExecutionContext): Future[(Response, BSONDocument)] =
    response match {
      case r @ WithCursor(_, _, _, _, _, preloaded +: _) =>
        Future.successful(r -> preloaded)

      case WithCursor(_, _, _, _, _, _) =>
        Future.failed(ReactiveMongoException(
          s"Cannot preload cursor response: $response"))

      case CommandError(_, _, _, cause) =>
        Future.failed(cause)

      case Successful(_, Reply(_, _, _, 0), _, _) =>
        Future.failed(ReactiveMongoException(
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
