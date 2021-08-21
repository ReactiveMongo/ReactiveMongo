package reactivemongo.core.protocol

import scala.util.{ Failure, Success, Try }

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.netty.BufferSequence

import reactivemongo.api.{ Compressor, ReadPreference }
import reactivemongo.api.commands.CommandKind

/**
 * A helper to build requests.
 *
 * @param kind the kind of command to be requested
 * @param op the write operation
 * @param documents body of this request
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
private[reactivemongo] final class RequestMaker(
  kind: CommandKind,
  val op: RequestOp,
  val documents: BufferSequence,
  val readPreference: ReadPreference,
  val channelIdHint: Option[ChannelId],
  compressors: Seq[Compressor]) {

  @inline def apply(requestID: Int): Request = make(requestID)

  private val make: Int => Request = compressors.headOption match {
    case Some(first) if (CommandKind canCompress kind) =>
      { id: Int =>
        compress(prepare(id), first, compressors.tail) match {
          case Success(compressed) => compressed
          case Failure(cause)      => throw cause
        }
      }

    case _ =>
      prepare(_: Int)
  }

  private def prepare(requestID: Int): Request = Request(
    requestID, 0, op, documents, readPreference, channelIdHint)

  @annotation.tailrec private def compress(
    request: Request,
    next: Compressor,
    alternatives: Seq[Compressor]): Try[Request] =
    Request.compress(request, next) match {
      case compressed @ Success(_) =>
        compressed

      case failed @ Failure(_) => alternatives.headOption match {
        case Some(c) =>
          compress(request, c, alternatives.tail)

        case _ =>
          failed
      }
    }
}

private[reactivemongo] object RequestMaker {
  def apply(
    kind: CommandKind,
    op: RequestOp,
    documents: BufferSequence = BufferSequence.empty,
    readPreference: ReadPreference = ReadPreference.primary,
    channelIdHint: Option[ChannelId] = None,
    compressors: Seq[Compressor] = Seq.empty): RequestMaker = new RequestMaker(
    kind, op, documents, readPreference, channelIdHint, compressors)

  def unapply(maker: RequestMaker): Option[(RequestOp, BufferSequence, ReadPreference, Option[ChannelId])] = Some((maker.op, maker.documents, maker.readPreference, maker.channelIdHint))

}
