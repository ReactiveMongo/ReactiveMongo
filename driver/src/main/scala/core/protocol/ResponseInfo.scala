package reactivemongo.core.protocol

import shaded.netty.channel.ChannelId

/**
 * Response meta information.
 *
 * @param channelId the id of the channel that carried this response.
 */
@deprecated("Will be private", "0.12.8")
class ResponseInfo(val _channelId: ChannelId)
  extends Product1[ChannelId] with Serializable {

  @deprecated(message = "Use ChannelId", since = "0.12.8")
  def this(channelId: Int) = this(sys.error("Use ChannelId"): ChannelId)

  @deprecated(message = "Use _channelId", since = "0.12.8")
  @throws[UnsupportedOperationException]("Use _channelId")
  def channelId: Int = throw new UnsupportedOperationException("Use _channelId")

  @deprecated(message = "Use _channelId", since = "0.12.8")
  @inline def _1 = _channelId

  def canEqual(that: Any): Boolean = that match {
    case _: ResponseInfo => true
    case _               => false
  }
}

object ResponseInfo
  extends scala.runtime.AbstractFunction1[ChannelId, ResponseInfo] {

  @deprecated(message = "Use ChannelId", since = "0.12.8")
  @throws[UnsupportedOperationException]("Use ChannelId")
  def apply(channelId: Int): ResponseInfo =
    throw new UnsupportedOperationException("Use ChannelId")

  def apply(channelId: ChannelId): ResponseInfo = new ResponseInfo(channelId)

  def unapply(that: ResponseInfo): Option[ChannelId] = that match {
    case other: ResponseInfo => Some(other._channelId)
    case _                   => None
  }
}

