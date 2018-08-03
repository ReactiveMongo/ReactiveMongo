package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import reactivemongo.io.netty.channel.ChannelId

import akka.actor.ActorRef

import reactivemongo.bson.BSONDocument

/**
 * @param name the main name of the node
 */
@deprecated(message = "Will be made private", since = "0.11.10")
@SerialVersionUID(440354552L)
case class Node(
  name: String,
  @transient status: NodeStatus,
  @transient connections: Vector[Connection],
  @transient authenticated: Set[Authenticated], // TODO: connections.authenticated (remove)
  tags: Option[BSONDocument],
  protocolMetadata: ProtocolMetadata,
  pingInfo: PingInfo = PingInfo(),
  isMongos: Boolean = false) {

  private[nodeset] val aliases = Set.newBuilder[String]

  // TODO: Refactor as immutable once private
  def withAlias(as: String): Node = {
    aliases += as
    this
  }

  /** All the node names (including its aliases) */
  def names: Set[String] = aliases.result() + name

  val (host: String, port: Int) = {
    val splitted = name.span(_ != ':')
    splitted._1 -> (try {
      splitted._2.drop(1).toInt
    } catch {
      case _: Throwable => 27017
    })
  }

  @transient val connected: Vector[Connection] =
    connections.filter(_.status == ConnectionStatus.Connected)

  @transient val authenticatedConnections = new RoundRobiner(
    connected.filter(_.authenticated.forall { auth =>
      authenticated.exists(_ == auth)
    }))

  @deprecated(message = "Use `createNeededChannels` with an explicit `channelFactory`", since = "0.12-RC1")
  def createNeededChannels(receiver: ActorRef, upTo: Int)(implicit channelFactory: ChannelFactory): Node = createNeededChannels(channelFactory, receiver, upTo)

  private[core] def createNeededChannels(
    channelFactory: ChannelFactory,
    receiver: ActorRef,
    upTo: Int): Node = {
    if (connections.size < upTo) {
      _copy(connections = connections ++ (for {
        _ ← 0 until (upTo - connections.size)
      } yield createConnection(channelFactory, receiver)))
    } else this
  }

  private[core] def createConnection(
    channelFactory: ChannelFactory,
    receiver: ActorRef): Connection = Connection(
    channelFactory.create(host, port, receiver),
    ConnectionStatus.Connecting, Set.empty, None)

  // TODO: Remove when aliases is refactored
  private[reactivemongo] def _copy(
    name: String = this.name,
    status: NodeStatus = this.status,
    connections: Vector[Connection] = this.connections,
    authenticated: Set[Authenticated] = this.authenticated,
    tags: Option[BSONDocument] = this.tags,
    protocolMetadata: ProtocolMetadata = this.protocolMetadata,
    pingInfo: PingInfo = this.pingInfo,
    isMongos: Boolean = this.isMongos,
    aliases: Set[String] = this.aliases.result()): Node = {

    val node = copy(name, status, connections, authenticated, tags,
      protocolMetadata, pingInfo, isMongos)

    node.aliases ++= aliases

    node
  }

  @inline private[core] def pickConnectionByChannelId(id: ChannelId): Option[Connection] = connections.find(_.channel.id == id)

  private[core] def updateByChannelId(id: ChannelId)(fc: Connection => Connection)(fn: Node => Node): Node = {
    val (updCons, updated) = utils.update(connections) {
      case conn if (conn.channel.id == id) => fc(conn)
    }

    if (updated) fn(_copy(connections = updCons))
    else this
  }

  def toShortString = s"""Node[$name: $status (${connected.size}/${connections.size} available connections), latency=${pingInfo.ping}, authenticated={${authenticated mkString ", "}}]"""

  /** Returns the read-only information about this node. */
  def info = NodeInfo(name, aliases.result(), host, port, status,
    connections.size, connections.count(_.status == ConnectionStatus.Connected),
    authenticatedConnections.subject.size, tags,
    protocolMetadata, pingInfo, isMongos)

}

/**
 * @param connections the number of all the node connections
 * @param connected the number of established connections for this node
 * @param authenticated the number of authenticated connections
 */
case class NodeInfo(
  name: String,
  aliases: Set[String],
  host: String,
  port: Int,
  status: NodeStatus,
  connections: Int,
  connected: Int,
  authenticated: Int,
  tags: Option[BSONDocument],
  protocolMetadata: ProtocolMetadata,
  pingInfo: PingInfo,
  isMongos: Boolean) {

  /** All the node names (including its aliases) */
  def names: Set[String] = aliases + name

  override lazy val toString = s"Node[$name: $status ($connected/$connections available connections), latency=${pingInfo.ping}, auth=$authenticated]"
}
