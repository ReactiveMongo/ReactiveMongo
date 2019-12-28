package reactivemongo.core.nodeset

@deprecated("Will be final", "0.17.0")
class NodeSetInfo(
  val name: Option[String],
  val version: Option[Long],
  val nodes: Vector[NodeInfo],
  val primary: Option[NodeInfo],
  val mongos: Option[NodeInfo],
  val secondaries: Vector[NodeInfo],
  val nearest: Option[NodeInfo],
  val awaitingRequests: Option[Int],
  val maxAwaitingRequestsPerChannel: Option[Int]) extends Product with Serializable {

  private[reactivemongo] def withAwaitingRequests(count: Int, maxPerChannel: Int): NodeSetInfo = new NodeSetInfo(name, version, nodes, primary, mongos, secondaries, nearest, Some(count), Some(maxPerChannel))

  @deprecated("No longer a ReactiveMongo case class", "0.17.0")
  val productArity = 9

  @deprecated("No longer a ReactiveMongo case class", "0.17.0")
  def productElement(n: Int): Any = (n: @annotation.switch) match {
    case 0 => name
    case 1 => version
    case 2 => nodes
    case 3 => primary
    case 4 => mongos
    case 5 => secondaries
    case 6 => nearest
    case 7 => awaitingRequests
    case 8 => maxAwaitingRequestsPerChannel
  }

  def canEqual(that: Any): Boolean = that match {
    case _: NodeSetInfo => true
    case _              => false
  }

  override def equals(that: Any): Boolean = that match {
    case other: NodeSetInfo => this.tupled == other.tupled
    case _                  => false
  }

  override def hashCode: Int = tupled.hashCode

  private lazy val tupled = Tuple9(name, version, nodes, primary, mongos, secondaries, nearest, awaitingRequests, maxAwaitingRequestsPerChannel)

  override lazy val toString = s"{{NodeSet $name ${nodes.mkString(" | ")} }}"
}

object NodeSetInfo extends scala.runtime.AbstractFunction7[Option[String], Option[Long], Vector[NodeInfo], Option[NodeInfo], Option[NodeInfo], Vector[NodeInfo], Option[NodeInfo], NodeSetInfo] {

  @deprecated("Use `NodeSetInfo` constructor", "0.17.0")
  def apply(
    name: Option[String],
    version: Option[Long],
    nodes: Vector[NodeInfo],
    primary: Option[NodeInfo],
    mongos: Option[NodeInfo],
    secondaries: Vector[NodeInfo],
    nearest: Option[NodeInfo]): NodeSetInfo = new NodeSetInfo(
    name, version, nodes, primary, mongos, secondaries, nearest, None, None)

  @deprecated("No longer a ReactiveMongo case class", "0.17.0")
  def unapply(info: NodeSetInfo): Option[(Option[String], Option[Long], Vector[NodeInfo], Option[NodeInfo], Option[NodeInfo], Vector[NodeInfo], Option[NodeInfo])] = Some((info.name, info.version, info.nodes, info.primary, info.mongos, info.secondaries, info.nearest))
}
/*
(
  name: Option[String],
  version: Option[Long],
  nodes: Vector[NodeInfo],
  primary: Option[NodeInfo],
  mongos: Option[NodeInfo],
  secondaries: Vector[NodeInfo],
  nearest: Option[NodeInfo])
 */
