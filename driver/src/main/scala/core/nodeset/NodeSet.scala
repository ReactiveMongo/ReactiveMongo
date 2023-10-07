package reactivemongo.core.nodeset

import scala.util.{ Failure, Success, Try }

import scala.collection.immutable.{ ListSet, Set }

import scala.math.Ordering

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.netty.ChannelFactory
import reactivemongo.core.protocol.ProtocolMetadata

import reactivemongo.api.{ Compressor, ReadPreference }

import reactivemongo.actors.actor.ActorRef

/**
 * @param name the replicaSet name
 * @param version the replicaSet version
 * @param compression the negociated compression
 */
private[reactivemongo] class NodeSet(
    val name: Option[String],
    val version: Option[Long],
    val nodes: Vector[Node],
    @transient val authenticates: Set[Authenticate],
    @transient val compression: ListSet[Compressor]) {

  /** The node which is the current primary one. */
  val primary: Option[Node] = nodes.find(_.status == NodeStatus.Primary)

  @inline private[core] def isMongos: Boolean = primary.exists(_.isMongos)

  /** The `mongos` node, if any. */
  val mongos: Option[Node] = nodes.find(_.isMongos)

  private val _secondaries = nodes.filter(_.status == NodeStatus.Secondary)

  @transient val secondaries = RoundRobiner(_secondaries)

  val queryable = _secondaries ++ primary

  /** See the [[https://docs.mongodb.com/manual/reference/read-preference/#nearest nearest]] read preference. */
  @transient val nearestGroup = RoundRobiner(queryable.sortWith {
    _.pingInfo.ping < _.pingInfo.ping
  })

  /** The first node from the [[nearestGroup]]. */
  val nearest = nearestGroup.pick

  val protocolMetadata: ProtocolMetadata =
    primary
      .orElse(secondaries.pick)
      .fold(ProtocolMetadata.Default)(_.protocolMetadata)

  def primary(authenticated: Authenticated): Option[Node] =
    primary.filter(_.authenticated contains authenticated)

  def isReachable = primary.nonEmpty || _secondaries.nonEmpty

  def updateOrAddNode(
      f: PartialFunction[Node, Node],
      default: Node
    ): NodeSet = {
    val (maybeUpdatedNodes, updated) = utils.update(nodes)(f)
    if (!updated) copy(nodes = default +: nodes)
    else copy(nodes = maybeUpdatedNodes)
  }

  def updateOrAddNodes(f: PartialFunction[Node, Node], ns: Seq[Node]) =
    ns.foldLeft(this)(_.updateOrAddNode(f, _))

  def updateAll(f: Node => Node): NodeSet = copy(nodes = nodes.map(f))

  def updateNodeByChannelId(id: ChannelId)(f: Node => Node): NodeSet =
    updateByChannelId(id)(identity)(f)

  def updateConnectionByChannelId(
      id: ChannelId
    )(f: Connection => Connection
    ): NodeSet = updateByChannelId(id)(f)(identity)

  def updateByChannelId(
      id: ChannelId
    )(fc: Connection => Connection
    )(fn: Node => Node
    ): NodeSet = copy(nodes = nodes.map(_.updateByChannelId(id)(fc)(fn)))

  def pickByChannelId(id: ChannelId): Option[(Node, Connection)] =
    nodes.view
      .map(node => node -> node.connections.find(_.channel.id == id))
      .collectFirst {
        case (node, Some(con)) if (con.status == ConnectionStatus.Connected) =>
          node -> con
      }

  // http://docs.mongodb.org/manual/reference/read-preference/
  private[reactivemongo] def pick(
      preference: ReadPreference,
      unpriorised: Int,
      accept: Connection => Boolean
    )(implicit
      ord: Ordering[Node]
    ): Option[(Node, Connection)] = {

    def filter(tags: Seq[Map[String, String]]) = ReadPreference.TagFilter(tags)

    val resolve = connectionAndFlatten(accept)

    if (mongos.isDefined) {
      resolve(mongos)
    } else
      preference match {
        case ReadPreference.PrimaryPreferred(tags) =>
          resolve(primary).orElse(
            resolve(
              findNode(secondaries, filter(tags), secondaries.pick, unpriorised)
            )
          )

        case ReadPreference.Secondary(tags) =>
          resolve(
            findNode(secondaries, filter(tags), secondaries.pick, unpriorised)
          )

        case ReadPreference.SecondaryPreferred(tags) =>
          resolve(
            findNode(secondaries, filter(tags), secondaries.pick, unpriorised)
          ).orElse(resolve(primary))

        case ReadPreference.Nearest(tags) =>
          resolve(findNode(nearestGroup, filter(tags), nearest, unpriorised))

        case _ =>
          resolve(primary)
      }
  }

  private def connectionAndFlatten(
      accept: Connection => Boolean
    ): Option[Node] => Option[(Node, Connection)] = {
    val p: RoundRobiner[Connection, Vector] => Option[Connection] =
      if (authenticates.isEmpty) _.pickWithFilter(accept)
      else
        _.pickWithFilter(c =>
          !c.authenticating.isDefined && c.authenticated.nonEmpty && accept(c)
        )

    _.flatMap(node => p(node.authenticatedConnections).map(node -> _))
  }

  private def findNode(
      roundRobiner: RoundRobiner[Node, Vector],
      filter: Option[Map[String, String] => Boolean],
      fallback: => Option[Node],
      unpriorised: Int
    )(implicit
      ord: Ordering[Node]
    ): Option[Node] =
    filter match {
      case Some(f) =>
        {
          val nodeFilter = { (n: Node) =>
            if (n.tags.isEmpty) false
            else f(n.tags)
          }

          if (unpriorised > 1) {
            roundRobiner.pickWithFilterAndPriority(nodeFilter, unpriorised)
          } else {
            roundRobiner.pickWithFilter(nodeFilter)
          }
        }.orElse(fallback)

      case _ => fallback
    }

  /**
   * Returns a NodeSet with channels created to `upTo` given maximum,
   * per each member of the set.
   */
  private[core] def createUserConnections(
      channelFactory: ChannelFactory,
      maxIdleTimeMS: Int,
      receiver: ActorRef,
      upTo: Int
    ): Try[NodeSet] = {

    @annotation.tailrec
    def update(ns: Vector[Node], upd: Vector[Node]): Try[Vector[Node]] =
      ns.headOption match {
        case Some(node) =>
          node.createUserConnections(
            channelFactory,
            maxIdleTimeMS,
            receiver,
            upTo
          ) match {
            case Failure(cause) =>
              Failure(cause)

            case Success(updated) =>
              update(ns.drop(1), updated +: upd)
          }

        case _ =>
          Success(upd)
      }

    update(nodes, Vector.empty).map { upd => copy(nodes = upd) }
  }

  def toShortString =
    s"{{NodeSet $name ${nodes.map(_.toShortString).mkString(" | ")} }}"

  /** Returns the read-only information about this node. */
  def info: NodeSetInfo = {
    val ns = nodes.map(_.info)

    new NodeSetInfo(
      name,
      version,
      ns,
      primary.map(_.info),
      mongos.map(_.info),
      ns.filter(_.status == NodeStatus.Secondary),
      nearest.map(_.info),
      awaitingRequests = None,
      maxAwaitingRequestsPerChannel = None,
      compression = compression
    )
  }

  @SuppressWarnings(Array("VariableShadowing"))
  def copy(
      name: Option[String] = this.name,
      version: Option[Long] = this.version,
      nodes: Vector[Node] = this.nodes,
      authenticates: Set[Authenticate] = this.authenticates,
      compression: ListSet[Compressor] = this.compression
    ): NodeSet =
    new NodeSet(name, version, nodes, authenticates, compression)

  private[core] lazy val tupled =
    Tuple5(name, version, nodes, authenticates, compression)

  override def equals(that: Any): Boolean = that match {
    case other: NodeSet =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"NodeSet${tupled.toString}"
}
