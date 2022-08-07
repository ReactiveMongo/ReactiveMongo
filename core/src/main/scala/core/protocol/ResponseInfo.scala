package reactivemongo.core.protocol

import reactivemongo.io.netty.channel.ChannelId

/**
 * Response meta information.
 *
 * @param channelId the id of the channel that carried this response.
 */
private[reactivemongo] final class ResponseInfo(
    val channelId: ChannelId)
    extends AnyVal {
  override def toString = s"ResponseInfo(${channelId})"
}
