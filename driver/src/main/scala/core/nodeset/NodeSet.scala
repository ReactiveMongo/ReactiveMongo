package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import shaded.netty.channel.ChannelId

import akka.actor.ActorRef

import reactivemongo.bson.BSONDocument
import reactivemongo.api.ReadPreference

@SerialVersionUID(527078726L)
case class NodeSet(
  name: Option[String],
  version: Option[Long],
  nodes: Vector[Node],
  @transient authenticates: Set[Authenticate]) {

  /** The node which is the current primary one. */
  val primary: Option[Node] = nodes.find(_.status == NodeStatus.Primary)

  /** The `mongos` node, if any. */
  val mongos: Option[Node] = nodes.find(_.isMongos)

  @transient val secondaries = new RoundRobiner(
    nodes.filter(_.status == NodeStatus.Secondary))

  val queryable = secondaries.subject ++ primary

  /** See the [[https://docs.mongodb.com/manual/reference/read-preference/#nearest nearest]] read preference. */
  @transient val nearestGroup = new RoundRobiner(
    queryable.sortWith { _.pingInfo.ping < _.pingInfo.ping })

  /** The first node from the [[nearestGroup]]. */
  val nearest = nearestGroup.subject.headOption

  val protocolMetadata: ProtocolMetadata =
    primary.orElse(secondaries.subject.headOption).
      fold(ProtocolMetadata.Default)(_.protocolMetadata)

  def primary(authenticated: Authenticated): Option[Node] =
    primary.filter(_.authenticated.exists(_ == authenticated))

  def isReachable = !primary.isEmpty || !secondaries.subject.isEmpty

  def updateOrAddNode(f: PartialFunction[Node, Node], default: Node) = {
    val (maybeUpdatedNodes, updated) = utils.update(nodes)(f)
    if (!updated) copy(nodes = default +: nodes)
    else copy(nodes = maybeUpdatedNodes)
  }

  def updateOrAddNodes(f: PartialFunction[Node, Node], nodes: Seq[Node]) =
    nodes.foldLeft(this)(_.updateOrAddNode(f, _))

  def updateAll(f: Node => Node): NodeSet = copy(nodes = nodes.map(f))

  @deprecated(message = "Use updateNodeByChannelId with ChannelId", "0.12.8")
  @throws[UnsupportedOperationException](
    "Use updateNodeByChannelId with ChannelId")
  def updateNodeByChannelId(id: Int)(f: Node => Node): NodeSet =
    throw new UnsupportedOperationException(
      "Use updateNodeByChannelId with ChannelId")

  def updateNodeByChannelId(id: ChannelId)(f: Node => Node) =
    updateByChannelId(id)(identity)(f)

  def updateConnectionByChannelId(id: ChannelId)(f: Connection => Connection): NodeSet = updateByChannelId(id)(f)(identity)

  @deprecated(
    message = "Use updateConnectionByChannelId with ChannelId", "0.12.8")
  @throws[UnsupportedOperationException](
    "Use updateConnectionByChannelId with ChannelId")
  def updateConnectionByChannelId(id: Int)(f: Connection => Connection): NodeSet = throw new UnsupportedOperationException("Use updateConnectionByChannelId with ChannelId")

  @deprecated(message = "Use updateByChannelId with ChannelId", "0.12.8")
  @throws[UnsupportedOperationException]("Use updateByChannelId with ChannelId")
  def updateByChannelId(id: Int)(fc: Connection => Connection)(fn: Node => Node): NodeSet = throw new UnsupportedOperationException("Use updateByChannelId with ChannelId")

  def updateByChannelId(id: ChannelId)(fc: Connection => Connection)(fn: Node => Node): NodeSet = copy(nodes = nodes.map(_.updateByChannelId(id)(fc)(fn)))

  @deprecated(message = "Use pickByChanneId with ChannelId", "0.12.8")
  @throws[UnsupportedOperationException]("Use pickByChanneId with ChannelId")
  def pickByChannelId(id: Int): Option[(Node, Connection)] =
    throw new UnsupportedOperationException("Use pickByChanneId with ChannelId")

  def pickByChannelId(id: ChannelId): Option[(Node, Connection)] =
    nodes.view.map(node =>
      node -> node.connections.find(_.channel.id == id)).collectFirst {
      case (node, Some(con)) if (
        con.status == ConnectionStatus.Connected) => node -> con
    }

  @deprecated(message = "Unused", since = "0.12-RC0")
  def pickForWrite: Option[(Node, Connection)] = primary.view.map(node =>
    node -> node.authenticatedConnections.subject.headOption).collectFirst {
    case (node, Some(connection)) => node -> connection
  }

  private val pickConnectionAndFlatten: Option[Node] => Option[(Node, Connection)] = {
    val p: RoundRobiner[Connection, Vector] => Option[Connection] =
      if (authenticates.isEmpty) _.pick
      else _.pickWithFilter(c =>
        !c.authenticating.isDefined && !c.authenticated.isEmpty)

    _.flatMap(node => p(node.authenticatedConnections).map(node -> _))
  }

  private def pickFromGroupWithFilter(roundRobiner: RoundRobiner[Node, Vector], filter: Option[BSONDocument => Boolean], fallback: => Option[Node]) =
    filter.fold(fallback)(f =>
      roundRobiner.pickWithFilter(_.tags.fold(false)(f)))

  // http://docs.mongodb.org/manual/reference/read-preference/
  def pick(preference: ReadPreference): Option[(Node, Connection)] = {
    if (mongos.isDefined) {
      pickConnectionAndFlatten(mongos)
    } else preference match {
      case ReadPreference.Primary =>
        pickConnectionAndFlatten(primary)

      case ReadPreference.PrimaryPreferred(filter) =>
        pickConnectionAndFlatten(primary.orElse(
          pickFromGroupWithFilter(secondaries, filter, secondaries.pick)))

      case ReadPreference.Secondary(filter) =>
        pickConnectionAndFlatten(pickFromGroupWithFilter(
          secondaries, filter, secondaries.pick))

      case ReadPreference.SecondaryPreferred(filter) =>
        pickConnectionAndFlatten(pickFromGroupWithFilter(
          secondaries, filter, secondaries.pick).orElse(primary))

      case ReadPreference.Nearest(filter) =>
        pickConnectionAndFlatten(pickFromGroupWithFilter(
          nearestGroup, filter, nearest))
    }
  }

  /**
   * Returns a NodeSet with channels created to `upTo` given maximum,
   * per each member of the set.
   */
  @deprecated(message = "Use `createNeededChannels` with the explicit `channelFactory`", since = "0.12-RC1")
  def createNeededChannels(receiver: ActorRef, upTo: Int)(implicit channelFactory: ChannelFactory): NodeSet = createNeededChannels(channelFactory, receiver, upTo)

  /**
   * Returns a NodeSet with channels created to `upTo` given maximum,
   * per each member of the set.
   */
  private[core] def createNeededChannels(channelFactory: ChannelFactory, receiver: ActorRef, upTo: Int): NodeSet = updateAll(_.createNeededChannels(channelFactory, receiver, upTo))

  def toShortString =
    s"{{NodeSet $name ${nodes.map(_.toShortString).mkString(" | ")} }}"

  /** Returns the read-only information about this node. */
  def info = {
    val ns = nodes.map(_.info)

    NodeSetInfo(name, version, ns, primary.map(_.info),
      mongos.map(_.info), ns.filter(_.status == NodeStatus.Secondary),
      nearest.map(_.info))
  }
}

case class NodeSetInfo(
  name: Option[String],
  version: Option[Long],
  nodes: Vector[NodeInfo],
  primary: Option[NodeInfo],
  mongos: Option[NodeInfo],
  secondaries: Vector[NodeInfo],
  nearest: Option[NodeInfo]) {

  override lazy val toString = s"{{NodeSet $name ${nodes.mkString(" | ")} }}"
}
