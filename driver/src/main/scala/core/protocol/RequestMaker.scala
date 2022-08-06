package reactivemongo.core.protocol

import reactivemongo.io.netty.channel.ChannelId
import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

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
  val payload: ByteBuf,
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
    Request(kind, requestID, 0, op, payload, readPreference,
      channelIdHint, callerSTE)

}

private[reactivemongo] object RequestMaker {
  def apply(
    kind: CommandKind,
    op: Query,
    document: ByteBuf,
    readPreference: ReadPreference,
    channelIdHint: Option[ChannelId],
    callerSTE: Seq[StackTraceElement]): RequestMaker =
    new RequestMaker(
      kind, op, document, readPreference, channelIdHint, callerSTE)

  def apply(
    op: KillCursors,
    readPreference: ReadPreference): RequestMaker = new RequestMaker(
    CommandKind.KillCursors, op, Unpooled.EMPTY_BUFFER, readPreference, None, Seq.empty)

  def apply(
    op: GetMore,
    document: ByteBuf,
    readPreference: ReadPreference,
    channelIdHint: Option[ChannelId]): RequestMaker = new RequestMaker(
    CommandKind.Query, op, document, readPreference, channelIdHint, Seq.empty)

  def apply(
    kind: CommandKind,
    op: Message,
    section: ByteBuf,
    readPreference: ReadPreference,
    channelIdHint: Option[ChannelId],
    callerSTE: Seq[StackTraceElement]): RequestMaker =
    new RequestMaker(
      kind, op, section, readPreference, channelIdHint, callerSTE)

  def unapply(maker: RequestMaker): Option[(RequestOp, ByteBuf, ReadPreference, Option[ChannelId])] = Some((maker.op, maker.payload, maker.readPreference, maker.channelIdHint))

}
