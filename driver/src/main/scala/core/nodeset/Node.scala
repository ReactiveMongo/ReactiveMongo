package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import scala.util.{ Failure, Success, Try }

import reactivemongo.io.netty.channel.ChannelId

import akka.actor.ActorRef

import reactivemongo.bson.BSONDocument

/**
 * @param name the main name of the node
 */
@SerialVersionUID(440354552L)
private[reactivemongo] case class Node(
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

  /**
   * The [[connected]] connections with no required authentication,
   * or already authenticated (at least once).
   */
  @transient val authenticatedConnections = new RoundRobiner(
    connected.filter(_.authenticated.forall { auth =>
      authenticated.exists(_ == auth)
    }))

  private[core] def createNeededChannels(
    channelFactory: ChannelFactory,
    receiver: ActorRef,
    upTo: Int): Try[Node] = {
    if (connections.size < upTo) {
      createConnections(
        channelFactory, receiver, upTo - connections.size, Vector.empty).
        map { created =>
          _copy(connections = connections ++ created)
        }

    } else Success(this)
  }

  @annotation.tailrec
  private def createConnections(
    channelFactory: ChannelFactory,
    receiver: ActorRef,
    count: Int,
    created: Vector[Connection]): Try[Vector[Connection]] = {
    if (count > 0) {
      createConnection(channelFactory, receiver) match {
        case Success(con) =>
          createConnections(channelFactory, receiver, count - 1, con +: created)

        case Failure(cause) => Failure(cause)
      }
    } else {
      Success(created)
    }
  }

  @inline private[core] def createConnection(
    channelFactory: ChannelFactory,
    receiver: ActorRef): Try[Connection] =
    channelFactory.create(host, port, receiver).map { chan =>
      Connection(
        chan,
        ConnectionStatus.Connecting, Set.empty, None)
    }

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

  def toShortString = s"""Node[$name: $status (${authenticatedConnections.size}/${connected.size}/${connections.size} available connections), latency=${pingInfo.ping}ns, authenticated={${authenticated.map(_.toShortString) mkString ", "}}]"""

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
