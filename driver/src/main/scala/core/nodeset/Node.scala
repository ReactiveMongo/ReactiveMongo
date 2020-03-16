package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import scala.util.{ Failure, Success, Try }

import reactivemongo.io.netty.channel.ChannelId

import akka.actor.ActorRef

import reactivemongo.bson.{ BSONDocument, BSONElement, BSONString }

import reactivemongo.core.protocol.ProtocolMetadata

import reactivemongo.core.netty.ChannelFactory

/**
 * @param name the main name of the node
 */
private[reactivemongo] sealed class Node(
  val name: String,
  val aliases: Set[String],
  @transient val status: NodeStatus,
  @transient val connections: Vector[Connection],
  @transient val authenticated: Set[Authenticated],
  val tags: Map[String, String],
  val protocolMetadata: ProtocolMetadata,
  val pingInfo: PingInfo,
  val isMongos: Boolean) {

  def copy(
    name: String = this.name,
    aliases: Set[String] = this.aliases,
    status: NodeStatus = this.status,
    connections: Vector[Connection] = this.connections,
    authenticated: Set[Authenticated] = this.authenticated,
    tags: Map[String, String] = this.tags,
    protocolMetadata: ProtocolMetadata = this.protocolMetadata,
    pingInfo: PingInfo = this.pingInfo,
    isMongos: Boolean = this.isMongos): Node = new Node(
    name, aliases, status, connections, authenticated, tags,
    protocolMetadata, pingInfo, isMongos)

  def withAlias(as: String): Node =
    new Node(name, aliases + as, status, connections, authenticated, tags,
      protocolMetadata, pingInfo, isMongos)

  /** All the node names (including its aliases) */
  def names: Set[String] = aliases + name

  val (host: String, port: Int) = {
    val splitted = name.span(_ != ':')
    splitted._1 -> (try {
      splitted._2.drop(1).toInt
    } catch {
      case _: Throwable => 27017
    })
  }

  @transient val connected: Vector[Connection] = connections.filter(c =>
    !c.signaling && c.status == ConnectionStatus.Connected)

  /**
   * The [[connected]] connections with no required authentication,
   * or already authenticated (at least once).
   */
  @transient val authenticatedConnections = RoundRobiner(
    connected.filter(_.authenticated.forall { auth =>
      authenticated.exists(_ == auth)
    }))

  private[reactivemongo] lazy val signaling: Option[Connection] =
    connections.find(c => c.signaling && c.status == ConnectionStatus.Connected)

  private[reactivemongo] def createSignalingConnection(
    channelFactory: ChannelFactory,
    receiver: ActorRef): Try[Node] = signaling match {
    case Some(_) => Success(this)

    case _ =>
      createConnection(channelFactory, receiver, true).map { con =>
        _copy(connections = con +: connections)
      }
  }

  /* Create channels (not for signaling). */
  private[core] def createUserConnections(
    channelFactory: ChannelFactory,
    receiver: ActorRef,
    upTo: Int): Try[Node] = {
    val count = connections.count(!_.signaling)

    if (count < upTo) {
      createChannels(
        channelFactory, receiver, upTo - count, Vector.empty).map { created =>
        _copy(connections = connections ++ created)
      }
    } else Success(this)
  }

  @annotation.tailrec
  private def createChannels(
    channelFactory: ChannelFactory,
    receiver: ActorRef,
    count: Int,
    created: Vector[Connection]): Try[Vector[Connection]] = {
    if (count > 0) {
      createConnection(channelFactory, receiver, false) match {
        case Success(con) =>
          createChannels(channelFactory, receiver, count - 1, con +: created)

        case Failure(cause) => Failure(cause)
      }
    } else {
      Success(created)
    }
  }

  @inline private[core] def createConnection(
    channelFactory: ChannelFactory,
    receiver: ActorRef,
    signaling: Boolean): Try[Connection] =
    channelFactory.create(host, port, receiver).map { chan =>
      new Connection(
        chan, ConnectionStatus.Connecting, Set.empty, None, signaling)
    }

  // TODO#1.1: Remove when aliases is refactored
  private[reactivemongo] def _copy(
    name: String = this.name,
    status: NodeStatus = this.status,
    connections: Vector[Connection] = this.connections,
    authenticated: Set[Authenticated] = this.authenticated,
    tags: Map[String, String] = tags,
    protocolMetadata: ProtocolMetadata = this.protocolMetadata,
    pingInfo: PingInfo = this.pingInfo,
    isMongos: Boolean = this.isMongos,
    aliases: Set[String] = this.aliases): Node =
    new Node(name, aliases, status, connections, authenticated, tags,
      protocolMetadata, pingInfo, isMongos)

  @inline private[core] def pickConnectionByChannelId(id: ChannelId): Option[Connection] = connections.find(_.channel.id == id)

  private[core] def updateByChannelId(id: ChannelId)(fc: Connection => Connection)(fn: Node => Node): Node = {
    val (updCons, updated) = utils.update(connections) {
      case conn if (conn.channel.id == id) => fc(conn)
    }

    if (updated) fn(_copy(connections = updCons))
    else this
  }

  lazy val toShortString = s"""Node[$name: $status (${authenticatedConnections.size}/${connected.size}/${connections.filterNot(_.signaling).size} available connections), latency=${pingInfo.ping}ns, authenticated={${authenticated.map(_.toShortString) mkString ", "}}]"""

  /** Returns the read-only information about this node. */
  def info = new NodeInfo(name, aliases, host, port, status,
    connections.count(!_.signaling),
    connections.count(_.status == ConnectionStatus.Connected),
    authenticatedConnections.size, tags,
    protocolMetadata, pingInfo, isMongos)

  override def equals(that: Any): Boolean = that match {
    case other: Node =>
      other.tupled == this.tupled

    case _ => false
  }

  override def hashCode: Int = tupled.hashCode

  private[reactivemongo] lazy val tupled = (name, status, connections,
    authenticated, tags, protocolMetadata, pingInfo, isMongos)
}

private[reactivemongo] object Node {
  object Queryable {
    def unapply(node: Node): Option[Node] =
      Option(node).filter(_.status.queryable)
  }
}

/**
 * @param connections the number of all the node connections
 * @param connected the number of established connections for this node
 * @param authenticated the number of authenticated connections
 */
final class NodeInfo private[reactivemongo] (
  val name: String,
  val aliases: Set[String],
  val host: String,
  val port: Int,
  val status: NodeStatus,
  val connections: Int,
  val connected: Int,
  val authenticated: Int,
  val tags: Map[String, String],
  val protocolMetadata: ProtocolMetadata,
  val pingInfo: PingInfo,
  val isMongos: Boolean) {

  private[reactivemongo] lazy val tupled =
    (name, aliases, host, port, status, connections,
      connected, authenticated, tags, protocolMetadata, pingInfo, isMongos)

  /** All the node names (including its aliases) */
  def names: Set[String] = aliases + name

  override lazy val toString = s"Node[$name: $status ($connected/$connections available connections), latency=${pingInfo.ping}, auth=$authenticated]"

  override def equals(that: Any): Boolean = that match {
    case other: NodeInfo =>
      other.tupled == this.tupled

    case _ => false
  }

  override lazy val hashCode: Int = tupled.hashCode
}
