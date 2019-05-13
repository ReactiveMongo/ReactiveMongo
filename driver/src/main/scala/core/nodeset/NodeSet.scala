package reactivemongo.core.nodeset

import scala.collection.immutable.Set

import scala.util.{ Failure, Success, Try }

import reactivemongo.io.netty.channel.ChannelId

import akka.actor.ActorRef

import reactivemongo.bson.BSONDocument
import reactivemongo.api.ReadPreference

/**
 * @param name the replicaSet name
 * @param version the replicaSet version
 */
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

  def updateOrAddNode(f: PartialFunction[Node, Node], default: Node): NodeSet = {
    val (maybeUpdatedNodes, updated) = utils.update(nodes)(f)
    if (!updated) copy(nodes = default +: nodes)
    else copy(nodes = maybeUpdatedNodes)
  }

  def updateOrAddNodes(f: PartialFunction[Node, Node], nodes: Seq[Node]) =
    nodes.foldLeft(this)(_.updateOrAddNode(f, _))

  def updateAll(f: Node => Node): NodeSet = copy(nodes = nodes.map(f))

  def updateNodeByChannelId(id: ChannelId)(f: Node => Node): NodeSet =
    updateByChannelId(id)(identity)(f)

  def updateConnectionByChannelId(id: ChannelId)(f: Connection => Connection): NodeSet = updateByChannelId(id)(f)(identity)

  def updateByChannelId(id: ChannelId)(fc: Connection => Connection)(fn: Node => Node): NodeSet = copy(nodes = nodes.map(_.updateByChannelId(id)(fc)(fn)))

  def pickByChannelId(id: ChannelId): Option[(Node, Connection)] =
    nodes.view.map(node =>
      node -> node.connections.find(_.channel.id == id)).collectFirst {
      case (node, Some(con)) if (
        con.status == ConnectionStatus.Connected) => node -> con
    }

  @deprecated("", "")
  def pick(preference: ReadPreference): Option[(Node, Connection)] =
    pick(preference, _ => true)

  // http://docs.mongodb.org/manual/reference/read-preference/
  private[reactivemongo] def pick(
    preference: ReadPreference,
    accept: Connection => Boolean): Option[(Node, Connection)] = {

    def filter(tags: Seq[Map[String, String]]) = ReadPreference.TagFilter(tags)

    val resolve = connectionAndFlatten(accept)

    if (mongos.isDefined) {
      resolve(mongos)
    } else preference match {
      case ReadPreference.Primary =>
        resolve(primary)

      case ReadPreference.PrimaryPreferred(tags) =>
        resolve(primary).orElse(
          resolve(findNode(secondaries, filter(tags), secondaries.pick)))

      case ReadPreference.Secondary(tags) =>
        resolve(findNode(
          secondaries, filter(tags), secondaries.pick))

      case ReadPreference.SecondaryPreferred(tags) =>
        resolve(findNode(
          secondaries, filter(tags), secondaries.pick)).orElse(resolve(primary))

      case ReadPreference.Nearest(tags) =>
        resolve(findNode(nearestGroup, filter(tags), nearest))
    }
  }

  private def connectionAndFlatten(accept: Connection => Boolean): Option[Node] => Option[(Node, Connection)] = {
    def p: RoundRobiner[Connection, Vector] => Option[Connection] =
      if (authenticates.isEmpty) _.pickWithFilter(accept)
      else _.pickWithFilter(c =>
        !c.authenticating.isDefined && !c.authenticated.isEmpty && accept(c))

    _.flatMap(node => p(node.authenticatedConnections).map(node -> _))
  }

  private def findNode(roundRobiner: RoundRobiner[Node, Vector], filter: Option[BSONDocument => Boolean], fallback: => Option[Node]): Option[Node] =
    filter.fold(fallback)(f =>
      roundRobiner.pickWithFilter(_.tags.fold(false)(f)).orElse(fallback))

  /**
   * Returns a NodeSet with channels created to `upTo` given maximum,
   * per each member of the set.
   */
  private[core] def createUserConnections(
    channelFactory: ChannelFactory,
    receiver: ActorRef,
    upTo: Int): Try[NodeSet] = {

    @annotation.tailrec
    def update(ns: Vector[Node], upd: Vector[Node]): Try[Vector[Node]] =
      ns.headOption match {
        case Some(node) =>
          node.createUserConnections(channelFactory, receiver, upTo) match {
            case Failure(cause) =>
              Failure(cause)

            case Success(updated) =>
              update(ns.tail, updated +: upd)
          }

        case _ =>
          Success(upd)
      }

    update(nodes, Vector.empty).map { upd => copy(nodes = upd) }
  }

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
