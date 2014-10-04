package reactivemongo.core.nodeset

import reactivemongo.core.protocol.Request
import akka.actor.ActorRef
import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import java.util.concurrent.{ Executor, Executors }
import reactivemongo.utils.LazyLogger
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.buffer.HeapChannelBufferFactory
import org.jboss.netty.channel.{ Channel, ChannelPipeline, Channels }
import reactivemongo.core.protocol._
import reactivemongo.api.{ MongoConnectionOptions, ReadPreference }
import reactivemongo.bson._

package object utils {
  def updateFirst[A, M[T] <: Iterable[T]](coll: M[A])(ƒ: A => Option[A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): M[A] = {
    val builder = cbf.apply
    def run(iterator: Iterator[A]): Unit = {
      while (iterator.hasNext) {
        val e = iterator.next
        val updated = ƒ(e)
        if (updated.isDefined) {
          builder += updated.get
          builder ++= iterator
        } else builder += e
      }
    }
    builder.result
  }

  def update[A, M[T] <: Iterable[T]](coll: M[A])(f: PartialFunction[A, A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): (M[A], Boolean) = {
    val builder = cbf.apply
    val (head, tail) = coll.span(!f.isDefinedAt(_))
    builder ++= head
    if (!tail.isEmpty) {
      builder += f(tail.head)
      builder ++= tail.drop(1)
    }
    builder.result -> !tail.isEmpty
  }
}

case class NodeSet(
    name: Option[String],
    version: Option[Long],
    nodes: Vector[Node],
    authenticates: Set[Authenticate]) {
  val primary: Option[Node] = nodes.find(_.status == NodeStatus.Primary)
  val secondaries = new RoundRobiner(nodes.filter(_.status == NodeStatus.Secondary))
  val queryable = secondaries.subject ++ primary
  val nearestGroup = new RoundRobiner(queryable.sortWith { _.pingInfo.ping < _.pingInfo.ping })
  val nearest = nearestGroup.subject.headOption

  def primary(authenticated: Authenticated): Option[Node] =
    primary.filter(_.authenticated.exists(_ == authenticated))

  def isReachable = !primary.isEmpty || !secondaries.subject.isEmpty

  def updateOrAddNode(ƒ: PartialFunction[Node, Node], default: Node) = {
    val (maybeUpdatedNodes, updated) = utils.update(nodes)(ƒ)
    if (!updated)
      copy(nodes = default +: nodes)
    else copy(nodes = maybeUpdatedNodes)
  }

  def updateOrAddNodes(ƒ: PartialFunction[Node, Node], nodes: Seq[Node]) =
    nodes.foldLeft(this)(_ updateOrAddNode (ƒ, _))

  def updateAll(ƒ: Node => Node) =
    copy(nodes = nodes.map(ƒ))

  def updateNodeByChannelId(id: Int)(ƒ: Node => Node) =
    updateByChannelId(id)(identity)(ƒ)

  def updateConnectionByChannelId(id: Int)(ƒ: Connection => Connection) =
    updateByChannelId(id)(ƒ)(identity)

  def updateByChannelId(id: Int)(ƒc: Connection => Connection)(ƒn: Node => Node) = {
    copy(nodes = nodes.map { node =>
      val (connections, updated) = utils.update(node.connections) {
        case conn if {
          conn.channel.getId == id
        } => ƒc(conn)
      }
      if (updated)
        ƒn(node.copy(connections = connections))
      else node
    })
  }

  def pickByChannelId(id: Int): Option[(Node, Connection)] =
    nodes.view.map(node => node -> node.connections.find(_.channel.getId() == id)).collectFirst {
      case (node, connection) if connection.exists(_.status == ConnectionStatus.Connected) => node -> connection.get
    }

  def pickForWrite: Option[(Node, Connection)] =
    primary.view.map(node => node -> node.authenticatedConnections.subject.headOption).collectFirst {
      case (node, Some(connection)) => node -> connection
    }

  private val pickConnectionAndFlatten: Option[Node] => Option[(Node, Connection)] = { node =>
    node.map(node => node -> node.authenticatedConnections.pick).collect { case (node, Some(connection)) => (node, connection) }
  }

  private def pickFromGroupWithFilter(roundRobiner: RoundRobiner[Node, Vector], filter: Option[BSONDocument => Boolean], fallback: => Option[Node]) = {
    def nodeMatchesFilter(filter: BSONDocument => Boolean): Node => Boolean = _.tags.map(filter(_)).getOrElse(false)

    filter.map { filter =>
      roundRobiner.pickWithFilter(nodeMatchesFilter(filter))
    }.getOrElse(fallback)
  }

  // http://docs.mongodb.org/manual/reference/read-preference/
  def pick(preference: ReadPreference): Option[(Node, Connection)] = preference match {
    case ReadPreference.Primary                   => pickConnectionAndFlatten(primary)
    case ReadPreference.PrimaryPrefered(filter)   => pickConnectionAndFlatten(primary.orElse(pickFromGroupWithFilter(secondaries, filter, secondaries.pick)))
    case ReadPreference.Secondary(filter)         => pickConnectionAndFlatten(pickFromGroupWithFilter(secondaries, filter, secondaries.pick))
    case ReadPreference.SecondaryPrefered(filter) => pickConnectionAndFlatten(pickFromGroupWithFilter(secondaries, filter, secondaries.pick).orElse(primary))
    case ReadPreference.Nearest(filter)           => pickConnectionAndFlatten(pickFromGroupWithFilter(nearestGroup, filter, nearest))
  }

  def createNeededChannels(receiver: ActorRef, upTo: Int)(implicit channelFactory: ChannelFactory): NodeSet = {
    copy(nodes = nodes.foldLeft(Vector.empty[Node]) { (nodes, node) =>
      nodes :+ node.createNeededChannels(receiver, upTo)
    })
  }

  def toShortString = s"{{NodeSet $name ${nodes.map(_.toShortString).mkString(" | ")} }}"
}

case class Node(
    name: String,
    status: NodeStatus,
    connections: Vector[Connection],
    authenticated: Set[Authenticated],
    tags: Option[BSONDocument],
    pingInfo: PingInfo = PingInfo()) {

  val (host: String, port: Int) = {
    val splitted = name.span(_ != ':')
    splitted._1 -> (try {
      splitted._2.drop(1).toInt
    } catch {
      case _: Throwable => 27017
    })
  }

  val connected = connections.filter(_.status == ConnectionStatus.Connected)

  val authenticatedConnections = new RoundRobiner(connected.filter(_.authenticated.forall { auth =>
    authenticated.exists(_ == auth)
  }))

  def createNeededChannels(receiver: ActorRef, upTo: Int)(implicit channelFactory: ChannelFactory): Node = {
    if (connections.size < upTo) {
      copy(connections = connections.++(for (i <- 0 until (upTo - connections.size)) yield Connection(channelFactory.create(host, port, receiver), ConnectionStatus.Disconnected, Set.empty, None)))
    } else this
  }

  def toShortString = s"Node[$name: $status (${connected.size}/${connections.size} available connections), latency=${pingInfo.ping}], auth=${authenticated}"
}

case class Connection(
    channel: Channel,
    status: ConnectionStatus,
    authenticated: Set[Authenticated],
    authenticating: Option[Authenticating]) {
  def send(message: Request, writeConcern: Request) {
    channel.write(message)
    channel.write(writeConcern)
  }
  def send(message: Request) {
    channel.write(message)
  }

  def isAuthenticated(db: String, user: String) = authenticated.exists(auth => auth.user == user && auth.db == db)
}

case class PingInfo(
  ping: Long = 0,
  lastIsMasterTime: Long = 0,
  lastIsMasterId: Int = -1)

object PingInfo {
  val pingTimeout = 60 * 1000
}

sealed trait NodeStatus { def queryable = false }
sealed trait QueryableNodeStatus { self: NodeStatus => override def queryable = true }
sealed trait CanonicalNodeStatus { self: NodeStatus => }
object NodeStatus {
  object Uninitialized extends NodeStatus { override def toString = "Uninitialized" }
  object NonQueryableUnknownStatus extends NodeStatus { override def toString = "NonQueryableUnknownStatus" }

  /** Cannot vote. All members start up in this state. The mongod parses the replica set configuration document while in STARTUP. */
  object Startup extends NodeStatus with CanonicalNodeStatus { override def toString = "Startup" }
  /** Can vote. The primary is the only member to accept write operations. */
  object Primary extends NodeStatus with QueryableNodeStatus with CanonicalNodeStatus { override def toString = "Primary" }
  /** Can vote. The secondary replicates the data store. */
  object Secondary extends NodeStatus with QueryableNodeStatus with CanonicalNodeStatus { override def toString = "Secondary" }
  /** Can vote. Members either perform startup self-checks, or transition from completing a rollback or resync. */
  object Recovering extends NodeStatus with CanonicalNodeStatus { override def toString = "Recovering" }
  /** Cannot vote. Has encountered an unrecoverable error. */
  object Fatal extends NodeStatus with CanonicalNodeStatus { override def toString = "Fatal" }
  /** Cannot vote. Forks replication and election threads before becoming a secondary. */
  object Startup2 extends NodeStatus with CanonicalNodeStatus { override def toString = "Startup2" }
  /** Cannot vote. Has never connected to the replica set. */
  object Unknown extends NodeStatus with CanonicalNodeStatus { override def toString = "Unknown" }
  /** Can vote. Arbiters do not replicate data and exist solely to participate in elections. */
  object Arbiter extends NodeStatus with CanonicalNodeStatus { override def toString = "Arbiter" }
  /** Cannot vote. Is not accessible to the set. */
  object Down extends NodeStatus with CanonicalNodeStatus { override def toString = "Down" }
  /** Can vote. Performs a rollback. */
  object Rollback extends NodeStatus with CanonicalNodeStatus { override def toString = "Rollback" }
  /** Shunned. */
  object Shunned extends NodeStatus with CanonicalNodeStatus { override def toString = "Shunned" }

  def apply(code: Int): NodeStatus = code match {
    case 0  => Startup
    case 1  => Primary
    case 2  => Secondary
    case 3  => Recovering
    case 4  => Fatal
    case 5  => Startup2
    case 6  => Unknown
    case 7  => Arbiter
    case 8  => Down
    case 9  => Rollback
    case 10 => Shunned
    case _  => NonQueryableUnknownStatus
  }
}

sealed trait ConnectionStatus
object ConnectionStatus {
  object Disconnected extends ConnectionStatus { override def toString = "Disconnected" }
  object Connected extends ConnectionStatus { override def toString = "Connected" }
}

sealed trait Authentication {
  def user: String
  def db: String
}

case class Authenticate(db: String, user: String, password: String) extends Authentication {
  override def toString: String = "Authenticate(" + db + ", " + user + ")"
}
case class Authenticating(db: String, user: String, password: String, nonce: Option[String]) extends Authentication {
  override def toString: String = s"Authenticating($db, $user, ${nonce.map(_ => "<nonce>").getOrElse("<>")})"
}
case class Authenticated(db: String, user: String) extends Authentication

class ContinuousIterator[A](iterable: Iterable[A], private var toDrop: Int = 0) extends Iterator[A] {
  private var iterator = iterable.iterator
  private var i = 0

  val hasNext = iterator.hasNext

  if (hasNext) {
    drop(toDrop)
  }

  def next =
    if (!hasNext)
      throw new NoSuchElementException("empty iterator")
    else {
      if (!iterator.hasNext) {
        iterator = iterable.iterator
        i = 0
      }
      val a = iterator.next
      i += 1
      a
    }

  def nextIndex = i
}

class RoundRobiner[A, M[T] <: Iterable[T]](val subject: M[A], startAtIndex: Int = 0) {
  private val iterator = new ContinuousIterator(subject)
  private val length = subject.size

  def pick: Option[A] = if (iterator.hasNext) Some(iterator.next) else None

  def pickWithFilter(filter: A => Boolean): Option[A] = pickWithFilter(filter, 0)

  @scala.annotation.tailrec
  private def pickWithFilter(filter: A => Boolean, tested: Int): Option[A] =
    if (length > 0 && tested < length) {
      val a = pick
      if (!a.isDefined)
        None
      else if (filter(a.get))
        a
      else pickWithFilter(filter, tested + 1)
    } else None

  def copy(subject: M[A], startAtIndex: Int = iterator.nextIndex) = new RoundRobiner(subject, startAtIndex)
}

class ChannelFactory(options: MongoConnectionOptions, bossExecutor: Executor = Executors.newCachedThreadPool, workerExecutor: Executor = Executors.newCachedThreadPool) {
  private val logger = LazyLogger("reactivemongo.core.nodeset.ChannelFactory")

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef) = {
    val channel = makeChannel(receiver)
    logger.trace("created a new channel: " + channel)
    channel
  }

  val channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor)

  private val bufferFactory = new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN)

  private def makePipeline(receiver: ActorRef): ChannelPipeline = Channels.pipeline(new RequestEncoder(), new ResponseFrameDecoder(), new ResponseDecoder(), new MongoHandler(receiver))

  private def makeChannel(receiver: ActorRef): Channel = {
    val channel = channelFactory.newChannel(makePipeline(receiver))
    val config = channel.getConfig
    config.setTcpNoDelay(options.tcpNoDelay)
    config.setBufferFactory(bufferFactory)
    config.setKeepAlive(options.keepAlive)
    config.setConnectTimeoutMillis(options.connectTimeoutMS)
    channel
  }
}