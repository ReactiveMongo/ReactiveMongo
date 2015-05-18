package reactivemongo.core.nodeset

import java.net.InetSocketAddress
import java.util.concurrent.{Executor, Executors}

import akka.actor.Actor.Receive
import akka.io.Tcp.{Register, Connected, Connect}
import akka.io.{Tcp, IO}
import akka.pattern.ask
import akka.actor._
import akka.routing.{RoundRobinRoutingLogic, Router}
import akka.util.Timeout
import reactivemongo.core.actors.{AwaitingResponse, RequestMakerExpectingResponse}
import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
import org.jboss.netty.buffer.HeapChannelBufferFactory
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.{Channel, ChannelPipeline, Channels}
import reactivemongo.api.{MongoConnectionOptions, ReadPreference}
import reactivemongo.bson._
import reactivemongo.core._
import reactivemongo.core.{SocketHandler, ConnectionManager}
import reactivemongo.core.protocol.{Request, _}
import reactivemongo.utils.LazyLogger

import scala.collection.generic.CanBuildFrom

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

class NodeSet(
     val name: Option[String],
     val authenticates: Set[Authenticate]) {
  import NodeSet._
  import RandomPick._

  var nodes: Vector[ActorRef] = Vector.empty[ActorRef]
  var version: Option[Long] = None

  var primaries : Router = null
  var secondaries : Router = null
  var mongos : Router = null
  var primaryPrefered : Router = null


  //val mongos: Option[Node] = nodes.find(_.isMongos)
  /*
  val primary: Option[Node] = nodes.find(_.status == NodeStatus.Primary)
  val secondaries = new RoundRobiner(nodes.filter(_.status == NodeStatus.Secondary))
  val queryable = secondaries.subject ++ primary
  val nearestGroup = new RoundRobiner(queryable.sortWith { _.pingInfo.ping < _.pingInfo.ping })
  val nearest = nearestGroup.subject.headOption
  val protocolMetadata = primary.orElse(secondaries.subject.headOption).map(_.protocolMetadata).getOrElse(ProtocolMetadata.Default)
  */
//
//  def primary(authenticated: Authenticated): Option[Node] =
//    primary.filter(_.authenticated.exists(_ == authenticated))
//
//  def isReachable = !primary.isEmpty || !secondaries.subject.isEmpty
//
//  def updateNodeByChannelId(id: Int)(ƒ: Node => Node) =
//    updateByChannelId(id)(identity)(ƒ)
//
//  def updateConnectionByChannelId(id: Int)(ƒ: Connection => Connection) =
//    updateByChannelId(id)(ƒ)(identity)

//  def updateByChannelId(id: Int)(ƒc: Connection => Connection)(ƒn: Node => Node) = {
//    copy(nodes = nodes.map { node =>
//      val (connections, updated) = utils.update(node.connections) {
//        case conn if (conn.chanel == id) => ƒc(conn)
//      }
//      if (updated) ƒn(node.copy(connections = connections))
//      else node
//    })
//  }

  def pickByChannelId(id: Int) = channelsMapping.get(id)

  def pickConnection(channel: Int) = channelsMapping.get(channel)

//  def pickForWrite: Option[(Node, Connection)] =
//    primary.view.map(node => node -> node.authenticatedConnections.subject.headOption).collectFirst {
//      case (node, Some(connection)) => node -> connection
//    }

  private val pickConnectionAndFlatten: Option[Node] => Option[(Node, Connection)] = _.map(node => node -> node.authenticatedConnections.pick).collect {
    case (node, Some(connection)) => (node, connection)
  }
//
//  private def pickFromGroupWithFilter(roundRobiner: RoundRobiner[Node, Vector], filter: Option[BSONDocument => Boolean], fallback: => Option[Node]) =
//    filter.fold(fallback)(f =>
//      roundRobiner.pickWithFilter(_.tags.fold(false)(f)))

  def pick(preference: ReadPreference): Option[ActorRef] = if(mongosConnections.isEmpty){
    preference match {
      case ReadPreference.Primary => primary
    }
  } else {
    mongosConnections.getRandom._2
  }

  // http://docs.mongodb.org/manual/reference/read-preference/
//  def pick(preference: ReadPreference): Option[(Node, Connection)] = {
//    if (mongos.isDefined) {
//      pickConnectionAndFlatten(mongos)
//    } else preference match {
//      case ReadPreference.Primary                    => pickConnectionAndFlatten(primary)
//      case ReadPreference.PrimaryPreferred(filter)   => pickConnectionAndFlatten(primary.orElse(pickFromGroupWithFilter(secondaries, filter, secondaries.pick)))
//      case ReadPreference.Secondary(filter)          => pickConnectionAndFlatten(pickFromGroupWithFilter(secondaries, filter, secondaries.pick))
//      case ReadPreference.SecondaryPreferred(filter) => pickConnectionAndFlatten(pickFromGroupWithFilter(secondaries, filter, secondaries.pick).orElse(primary))
//      case ReadPreference.Nearest(filter)            => pickConnectionAndFlatten(pickFromGroupWithFilter(nearestGroup, filter, nearest))
//    }
//  }
//
//  def createNeededChannels(receiver: => ActorRef, connectionManager: => ActorRef, upTo: Int): NodeSet =
//    copy(nodes = nodes.foldLeft(Vector.empty[Node]) { (nodes, node) =>
//      nodes :+ node.createNeededChannels(receiver, connectionManager,  upTo)
//    })

//  override def receive: Receive = {
//    case AddNode(address) => {
//      log.info("Adding Node to NodeSet with address {}", address)
//      val node = context.actorOf(Props(classOf[Node]))
//        nodes = node +: nodes
//    }
//
//  }

  def createChannel(seed: )
}

object NodeSet {
  case class AddNode(address: InetSocketAddress)
  case class IsMaster(id: Int)
  case class OnDisconnect(chanel: Int)
  object PrimaryUnavaliable
}


case class ProtocolMetadata(
  minWireVersion: MongoWireVersion,
  maxWireVersion: MongoWireVersion,
  maxMessageSizeBytes: Int,
  maxBsonSize: Int,
  maxBulkSize: Int
)

object ProtocolMetadata {
  val Default = ProtocolMetadata(MongoWireVersion.V24AndBefore, MongoWireVersion.V24AndBefore, 48000000, 16 * 1024 * 1024, 1000)
}

case class Connection(
      connection: ActorRef,
      authenticated: Set[Authenticated],
      authenticating: Option[Authenticating],
      channel: Int) extends Actor {

  private var awaitingResponses = HashMap[Int, AwaitingResponse]()

  val socketHandler = context.actorOf(Props(classOf[SocketHandler], connection))
  connection ! Register(socketHandler, keepOpenOnPeerClosed = true)

  def send(message: Request, writeConcern: Request) {
    //channel.write(message)
    //channel.write(writeConcern)
  }

  def send(message: Request) = {


    //channel.write(message)
  }

  def isAuthenticated(db: String, user: String) =
    authenticated.exists(auth => auth.user == user && auth.db == db)

  override def receive: Actor.Receive = {
    case Connection.RequestExpectingResponse(request, req) =>{
      awaitingResponses = awaitingResponses + (request.requestID -> AwaitingResponse(request.requestID, channel,
        req.promise, isGetLastError = false, isMongo26WriteOp = req.isMongo26WriteOp))
    }
  }
}

object Connection {
  case class RequestExpectingResponse(request: Request, req: RequestMakerExpectingResponse)
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
  override def toString: String =
    s"Authenticating($db, $user, ${nonce.map(_ => "<nonce>").getOrElse("<>")})"
}
case class Authenticated(db: String, user: String) extends Authentication

object RandomPick {
  import scala.util.Random

  implicit class IterableExt[A](val coll: Iterable[A]) {
    def getRandom() : A ={
      val rnd = new Random(coll.hashCode())
      val next = rnd.nextInt(coll.size)
      coll.drop(next).head
    }
  }
}

class ContinuousIterator[A](iterable: Iterable[A], private var toDrop: Int = 0) extends Iterator[A] {
  private var iterator = iterable.iterator
  private var i = 0

  val hasNext = iterator.hasNext

  if (hasNext) drop(toDrop)

  def next =
    if (!hasNext) throw new NoSuchElementException("empty iterator")
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
      if (!a.isDefined) None
      else if (filter(a.get)) a
      else pickWithFilter(filter, tested + 1)
    } else None

  def copy(subject: M[A], startAtIndex: Int = iterator.nextIndex) =
    new RoundRobiner(subject, startAtIndex)
}

class ChannelFactory(options: MongoConnectionOptions, bossExecutor: Executor = Executors.newCachedThreadPool, workerExecutor: Executor = Executors.newCachedThreadPool) {
  import javax.net.ssl.SSLContext

  private val logger = LazyLogger("reactivemongo.core.nodeset.ChannelFactory")

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef) = {
    val channel = makeChannel(receiver)
    logger.trace("created a new channel: " + channel)
    channel
  }

  val channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor)

  private val bufferFactory = new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN)

  private def makePipeline(receiver: ActorRef): ChannelPipeline = {
    val pipeline = Channels.pipeline(new ResponseFrameDecoder(),
      new ResponseDecoder(), new RequestEncoder(), new MongoHandler(receiver))

    if (options.sslEnabled) {
      val sslCtx = {
        val tm: Array[javax.net.ssl.TrustManager] =
          if (options.sslAllowsInvalidCert) Array(TrustAny) else null

        val ctx = SSLContext.getInstance("SSL")
        ctx.init(null, tm, new java.security.SecureRandom())
        ctx
      }

      val sslEng = {
        val engine = sslCtx.createSSLEngine()
        engine.setUseClientMode(true)
        engine
      }

      val sslHandler =
        new org.jboss.netty.handler.ssl.SslHandler(sslEng, false/* TLS */)
      
      pipeline.addFirst("ssl", sslHandler)
    }

    pipeline
  }

  private def makeChannel(receiver: ActorRef): Channel = {
    val channel = channelFactory.newChannel(makePipeline(receiver))
    val config = channel.getConfig
    config.setTcpNoDelay(options.tcpNoDelay)
    config.setBufferFactory(bufferFactory)
    config.setKeepAlive(options.keepAlive)
    config.setConnectTimeoutMillis(options.connectTimeoutMS)
    channel
  }

  private object TrustAny extends javax.net.ssl.X509TrustManager {
    import java.security.cert.X509Certificate

    override def checkClientTrusted(cs: Array[X509Certificate], a: String) = {}
    override def checkServerTrusted(cs: Array[X509Certificate], a: String) = {}
    override def getAcceptedIssuers(): Array[X509Certificate] = null
  }
}
