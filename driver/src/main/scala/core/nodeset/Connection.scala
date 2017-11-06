package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import shaded.netty.channel.{ Channel, ChannelFuture }

import reactivemongo.core.protocol.Request

case class Connection(
  channel: Channel,
  status: ConnectionStatus,
  authenticated: Set[Authenticated],
  authenticating: Option[Authenticating]) {
  def send(message: Request, writeConcern: Request): ChannelFuture = {
    channel.write(message)
    channel.write(writeConcern)
  }

  def send(message: Request): ChannelFuture = channel.write(message)

  /** Returns whether the `user` is authenticated against the `db`. */
  def isAuthenticated(db: String, user: String): Boolean =
    authenticated.exists(auth => auth.user == user && auth.db == db)
}
