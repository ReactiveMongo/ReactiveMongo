package reactivemongo.core.protocol

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.netty.BufferSequence

import reactivemongo.api.ReadPreference
import reactivemongo.api.commands.CommandKind

/**
 * A helper to build requests.
 *
 * @param kind the kind of command to be requested
 * @param op the write operation
 * @param documents body of this request
 * @param channelIdHint a hint for sending this request on a particular channel
 */
private[reactivemongo] final class RequestMaker(
  kind: CommandKind,
  val op: RequestOp,
  val documents: BufferSequence,
  val readPreference: ReadPreference,
  val channelIdHint: Option[ChannelId],
  parentSTE: Seq[StackTraceElement]) {

  private val callerSTE: Seq[StackTraceElement] = {
    val current = Seq.newBuilder[StackTraceElement]

    current ++= reactivemongo.util.Trace.
      currentTraceElements.drop(3).dropRight(6)

    if (parentSTE.nonEmpty) {
      current += new StackTraceElement("---", "---", "---", -1)
      current ++= parentSTE
    }

    current.result()
  }

  @inline def apply(requestID: Int): Request =
    Request(kind, requestID, 0, op, documents, readPreference,
      channelIdHint, callerSTE)

}

private[reactivemongo] object RequestMaker {
  def apply(
    kind: CommandKind,
    op: RequestOp,
    documents: BufferSequence = BufferSequence.empty,
    readPreference: ReadPreference = ReadPreference.primary,
    channelIdHint: Option[ChannelId] = None,
    callerSTE: Seq[StackTraceElement] = Seq.empty): RequestMaker = new RequestMaker(
    kind, op, documents, readPreference, channelIdHint, callerSTE)

  def unapply(maker: RequestMaker): Option[(RequestOp, BufferSequence, ReadPreference, Option[ChannelId])] = Some((maker.op, maker.documents, maker.readPreference, maker.channelIdHint))

}
