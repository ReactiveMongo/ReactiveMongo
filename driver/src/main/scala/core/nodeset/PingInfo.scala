package reactivemongo.core.nodeset

import reactivemongo.io.netty.channel.ChannelId

/**
 * @param ping the response delay (in nanoseconds) for the last IsMaster request (duration between request and its response, or `Long.MaxValue`)
 * @param lastIsMasterTime the timestamp when the last IsMaster request has been sent (or 0)
 * @param lastIsMasterId the ID of the last IsMaster request (or -1 if none)
 */
@deprecated("Internal: will be made private", "0.18.2")
class PingInfo(
  val ping: Long,
  val lastIsMasterTime: Long,
  val lastIsMasterId: Int,
  private[core] val channelId: Option[ChannelId],
  private[core] val firstSent: Boolean) extends Product3[Long, Long, Int] with Serializable {

  // TODO#1.1: Remove
  def this(
    ping: Long,
    lastIsMasterTime: Long,
    lastIsMasterId: Int,
    channelId: Option[ChannelId]) =
    this(ping, lastIsMasterTime, lastIsMasterId, channelId, firstSent = false)

  @inline def _1 = ping
  @inline def _2 = lastIsMasterTime
  @inline def _3 = lastIsMasterId

  // TODO#1.1: Remove
  @inline def copy(
    ping: Long,
    lastIsMasterTime: Long,
    lastIsMasterId: Int,
    channelId: Option[ChannelId]): PingInfo = new PingInfo(
    ping, lastIsMasterTime, lastIsMasterId, channelId, this.firstSent)

  @inline def copy(
    ping: Long = this.ping,
    lastIsMasterTime: Long = this.lastIsMasterTime,
    lastIsMasterId: Int = this.lastIsMasterId,
    channelId: Option[ChannelId] = this.channelId,
    firstSent: Boolean = this.firstSent): PingInfo = new PingInfo(
    ping, lastIsMasterTime, lastIsMasterId, channelId, firstSent)

  def canEqual(that: Any): Boolean = that match {
    case _: PingInfo => false
    case _           => false
  }

  override def equals(that: Any): Boolean = that match {
    case other: PingInfo =>
      (tupled == other.tupled) && (channelId == other.channelId)

    case _ =>
      false
  }

  @inline override def hashCode: Int = (tupled -> channelId).hashCode

  @inline override def toString =
    s"PingInfo($ping, $lastIsMasterTime, $lastIsMasterId, $channelId)"

  private lazy val tupled = (ping, lastIsMasterTime, lastIsMasterId)
}

@deprecated("Will be internal", "0.18.2")
object PingInfo {
  def apply(
    ping: Long = Long.MaxValue,
    lastIsMasterTime: Long = 0,
    lastIsMasterId: Int = -1): PingInfo = new PingInfo(
    ping, lastIsMasterTime, lastIsMasterId, None, false)

  private[core] def apply(
    ping: Long,
    lastIsMasterTime: Long,
    lastIsMasterId: Int,
    channelId: ChannelId): PingInfo = new PingInfo(
    ping, lastIsMasterTime, lastIsMasterId, Some(channelId), false)

  def unapply(other: PingInfo): Option[(Long, Long, Int)] = Some(other.tupled)

  @deprecated("See MongoConnectionOptions.heartbeatFrequencyMS", "0.16.6")
  val pingTimeout = 60 * 1000
}
