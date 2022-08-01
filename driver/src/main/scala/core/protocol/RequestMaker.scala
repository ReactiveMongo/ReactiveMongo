package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.Unpooled
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
    op: Query,
    documents: BufferSequence,
    readPreference: ReadPreference,
    channelIdHint: Option[ChannelId],
    callerSTE: Seq[StackTraceElement]): RequestMaker =
    new RequestMaker(
      kind, op, documents, readPreference, channelIdHint, callerSTE)

  def apply(
    op: KillCursors,
    readPreference: ReadPreference): RequestMaker = new RequestMaker(
    CommandKind.KillCursors, op, BufferSequence.empty, readPreference, None, Seq.empty)

  def apply(
    op: GetMore,
    documents: BufferSequence,
    readPreference: ReadPreference,
    channelIdHint: Option[ChannelId]): RequestMaker = new RequestMaker(
    CommandKind.Query, op, documents, readPreference, channelIdHint, Seq.empty)

  private lazy val opMsgPayloadType0 = Unpooled.copiedBuffer(Array[Byte](0))

  def apply(
    kind: CommandKind,
    op: Message,
    document: BufferSequence,
    readPreference: ReadPreference,
    channelIdHint: Option[ChannelId],
    callerSTE: Seq[StackTraceElement]): RequestMaker = {
    val BufferSequence(head, tail @ _*) = document
    val payload0 = BufferSequence(opMsgPayloadType0, (head +: tail): _*)

    new RequestMaker(
      kind, op, payload0, readPreference, channelIdHint, callerSTE)
  }

  def unapply(maker: RequestMaker): Option[(RequestOp, BufferSequence, ReadPreference, Option[ChannelId])] = Some((maker.op, maker.documents, maker.readPreference, maker.channelIdHint))

}
