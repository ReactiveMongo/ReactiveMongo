package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import reactivemongo.io.netty.channel.{ Channel, ChannelFuture }

import reactivemongo.core.protocol.Request

/**
 * @param signaling if true it's a signaling connection (not for r/w ops)
 */
@deprecated("Internal: will be made private", "0.17.0")
class Connection(
  val channel: Channel,
  val status: ConnectionStatus,
  val authenticated: Set[Authenticated],
  val authenticating: Option[Authenticating],
  val signaling: Boolean) extends Product with scala.Serializable {

  def send(message: Request, writeConcern: Request): ChannelFuture = {
    channel.write(message)
    channel.writeAndFlush(writeConcern)
  }

  def send(message: Request): ChannelFuture =
    channel.writeAndFlush(message)

  /** Returns whether the `user` is authenticated against the `db`. */
  def isAuthenticated(db: String, user: String): Boolean =
    authenticated.exists(auth => auth.user == user && auth.db == db)

  @deprecated("No longer a case class", "0.17.0")
  def copy(
    channel: Channel = this.channel,
    status: ConnectionStatus = this.status,
    authenticated: Set[Authenticated] = this.authenticated,
    authenticating: Option[Authenticating] = this.authenticating): Connection =
    new Connection(channel, status, authenticated, authenticating, signaling)

  def canEqual(that: Any): Boolean = that match {
    case _: Connection => true
    case _             => false
  }

  @deprecated("No longer a case class", "0.17.0")
  val productArity: Int = 5

  @deprecated("No longer a case class", "0.17.0")
  def productElement(n: Int): Any = (n: @annotation.switch) match {
    case 0 => channel
    case 1 => status
    case 2 => authenticated
    case 3 => authenticating
    case 4 => signaling
  }

  private[nodeset] def tupled = Tuple4(channel, status, authenticated, authenticating)

  override def equals(that: Any): Boolean = that match {
    case other: Connection =>
      (tupled == other.tupled) && signaling == other.signaling

    case _ => false
  }

  override def hashCode: Int = tupled.hashCode + signaling.hashCode

  override def toString: String = s"Connection$tupled"
}

@deprecated("Internal: will be made private", "0.17.0")
object Connection extends scala.runtime.AbstractFunction4[Channel, ConnectionStatus, Set[Authenticated], Option[Authenticating], Connection] {
  @deprecated("", "")
  def apply(
    channel: Channel,
    status: ConnectionStatus,
    authenticated: Set[Authenticated],
    authenticating: Option[Authenticating]): Connection =
    new Connection(channel, status, authenticated, authenticating, false)

  def apply(
    channel: Channel,
    status: ConnectionStatus,
    authenticated: Set[Authenticated],
    authenticating: Option[Authenticating],
    signaling: Boolean): Connection =
    new Connection(channel, status, authenticated, authenticating, signaling)

  @deprecated("", "")
  def unapply(that: Any): Option[(Channel, ConnectionStatus, Set[Authenticated], Option[Authenticating])] = that match {
    case c: Connection => Some(c.tupled)
    case _             => None
  }
}
