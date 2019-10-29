package reactivemongo.core.protocol

import reactivemongo.io.netty.channel.ChannelId

/**
 * Response meta information.
 *
 * @param channelId the id of the channel that carried this response.
 */
private[reactivemongo] final class ResponseInfo(
  val _channelId: ChannelId) extends AnyVal {
  override def toString = s"ResponseInfo(${_channelId})"
}

@deprecated("Internal: will be made private", "0.16.0")
object ResponseInfo
  extends scala.runtime.AbstractFunction1[ChannelId, ResponseInfo] {

  def apply(channelId: ChannelId): ResponseInfo = new ResponseInfo(channelId)

  def unapply(that: ResponseInfo): Option[ChannelId] = that match {
    case other: ResponseInfo => Some(other._channelId)
    case _                   => None
  }
}

