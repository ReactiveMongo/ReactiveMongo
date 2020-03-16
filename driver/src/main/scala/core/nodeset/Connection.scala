package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import reactivemongo.io.netty.channel.{ Channel, ChannelFuture }

import reactivemongo.core.protocol.Request

/**
 * @param signaling if true it's a signaling connection (not for r/w ops)
 */
private[reactivemongo] class Connection(
  val channel: Channel,
  val status: ConnectionStatus,
  val authenticated: Set[Authenticated],
  val authenticating: Option[Authenticating],
  val signaling: Boolean) {

  def send(message: Request, writeConcern: Request): ChannelFuture = {
    channel.write(message)
    channel.writeAndFlush(writeConcern)
  }

  def send(message: Request): ChannelFuture =
    channel.writeAndFlush(message)

  /** Returns whether the `user` is authenticated against the `db`. */
  def isAuthenticated(db: String, user: String): Boolean =
    authenticated.exists(auth => auth.user == user && auth.db == db)

  private[core] def copy(
    channel: Channel = this.channel,
    status: ConnectionStatus = this.status,
    authenticated: Set[Authenticated] = this.authenticated,
    authenticating: Option[Authenticating] = this.authenticating): Connection =
    new Connection(channel, status, authenticated, authenticating, signaling)

  private[nodeset] def tupled = Tuple4(channel, status, authenticated, authenticating)

  override def equals(that: Any): Boolean = that match {
    case other: Connection =>
      (tupled == other.tupled) && signaling == other.signaling

    case _ => false
  }

  override def hashCode: Int = tupled.hashCode + signaling.hashCode

  override def toString: String = s"Connection$tupled"
}
