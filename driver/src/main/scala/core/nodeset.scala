package reactivemongo.core.nodeset

import scala.language.higherKinds

import java.util.concurrent.{ TimeUnit, Executor, Executors }

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.Set

import shaded.netty.util.HashedWheelTimer
import shaded.netty.buffer.HeapChannelBufferFactory
import shaded.netty.channel.socket.nio.NioClientSocketChannelFactory
import shaded.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelPipeline,
  Channels
}
import shaded.netty.handler.timeout.IdleStateHandler

import akka.actor.ActorRef

import reactivemongo.util.LazyLogger
import reactivemongo.core.protocol.Request

import reactivemongo.bson.BSONDocument
import reactivemongo.core.protocol.{
  MongoHandler,
  MongoWireVersion,
  RequestEncoder,
  ResponseDecoder,
  ResponseFrameDecoder
}
import reactivemongo.api.{ MongoConnectionOptions, ReadPreference }

package object utils {
  def updateFirst[A, M[T] <: Iterable[T]](coll: M[A])(f: A => Option[A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): M[A] = {
    val builder = cbf.apply

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

  def updateNodeByChannelId(id: Int)(f: Node => Node) =
    updateByChannelId(id)(identity)(f)

  def updateConnectionByChannelId(id: Int)(f: Connection => Connection) =
    updateByChannelId(id)(f)(identity)

  def updateByChannelId(id: Int)(fc: Connection => Connection)(fn: Node => Node) = copy(nodes = nodes.map { node =>
    val (connections, updated) = utils.update(node.connections) {
      case conn if (conn.channel.getId == id) => fc(conn)
    }

    if (updated) fn(node._copy(connections = connections))
    else node
  })

  def pickByChannelId(id: Int): Option[(Node, Connection)] =
    nodes.view.map(node =>
      node -> node.connections.find(_.channel.getId == id)).collectFirst {
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

/**
 * @param name the main name of the node
 */
@deprecated(message = "Will be made private", since = "0.11.10")
@SerialVersionUID(440354552L)
case class Node(
  name: String,
  @transient status: NodeStatus,
  @transient connections: Vector[Connection],
  @transient authenticated: Set[Authenticated], // TODO: connections.authenticated
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
  def createNeededChannels(receiver: ActorRef, upTo: Int)(implicit channelFactory: ChannelFactory): Node = {
    if (connections.size < upTo) {
      _copy(connections = connections ++ (for {
        i ← 0 until (upTo - connections.size)
      } yield Connection(
        channelFactory.create(host, port, receiver),
        ConnectionStatus.Disconnected, Set.empty, None)))
    } else this
  }

  private[core] def createNeededChannels(channelFactory: ChannelFactory, receiver: ActorRef, upTo: Int): Node = {
    if (connections.size < upTo) {
      _copy(connections = connections ++ (for {
        i ← 0 until (upTo - connections.size)
      } yield Connection(
        channelFactory.create(host, port, receiver),
        ConnectionStatus.Disconnected, Set.empty, None)))
    } else this
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

    node.aliases ++= this.aliases.result()

    node
  }

  def toShortString = s"Node[$name: $status (${connected.size}/${connections.size} available connections), latency=${pingInfo.ping}], auth=$authenticated"

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

  override lazy val toString = s"Node[$name: $status ($connected/$connections available connections), latency=${pingInfo.ping}], auth=$authenticated"
}

case class ProtocolMetadata(
  minWireVersion: MongoWireVersion,
  maxWireVersion: MongoWireVersion,
  maxMessageSizeBytes: Int,
  maxBsonSize: Int,
  maxBulkSize: Int) {
  override lazy val toString =
    s"ProtocolMetadata($minWireVersion, $maxWireVersion)"
}

object ProtocolMetadata {
  val Default = ProtocolMetadata(
    MongoWireVersion.V30, MongoWireVersion.V30,
    48000000, 16 * 1024 * 1024, 1000)
}

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

/**
 * @param ping the response delay for the last IsMaster request (duration between request and its response, or `Long.MaxValue`)
 * @param lastIsMasterTime the timestamp when the last IsMaster request has been sent (or 0)
 * @param lastIsMasterId the ID of the last IsMaster request (or -1 if none)
 */
case class PingInfo(
  ping: Long = Long.MaxValue,
  lastIsMasterTime: Long = 0,
  lastIsMasterId: Int = -1)

object PingInfo {
  // TODO: Use MongoConnectionOption (e.g. monitorRefreshMS)
  val pingTimeout = 60 * 1000
}

sealed trait NodeStatus { def queryable = false }

sealed trait QueryableNodeStatus { self: NodeStatus =>
  override def queryable = true
}

sealed trait CanonicalNodeStatus { self: NodeStatus => }

object NodeStatus {
  object Uninitialized extends NodeStatus {
    override def toString = "Uninitialized"
  }

  object NonQueryableUnknownStatus extends NodeStatus {
    override def toString = "NonQueryableUnknownStatus"
  }

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
  object Disconnected extends ConnectionStatus {
    override def toString = "Disconnected"
  }

  object Connecting extends ConnectionStatus {
    override def toString = "Connecting"
  }

  object Connected extends ConnectionStatus {
    override def toString = "Connected"
  }
}

sealed trait Authentication {
  def user: String
  def db: String
}

/**
 * @param db the name of the database
 * @param user the name of the user
 * @param password the password for the [[user]]
 */
case class Authenticate(
  db: String,
  user: String,
  password: String) extends Authentication {

  override def toString = s"Authenticate($db, $user)"
}

sealed trait Authenticating extends Authentication {
  def password: String
}

object Authenticating {
  @deprecated(message = "Use [[reactivemongo.core.nodeset.CrAuthenticating]]", since = "0.11.10")
  def apply(db: String, user: String, password: String, nonce: Option[String]): Authenticating = CrAuthenticating(db, user, password, nonce)

  def unapply(auth: Authenticating): Option[(String, String, String)] =
    auth match {
      case CrAuthenticating(db, user, pass, _) =>
        Some((db, user, pass))

      case ScramSha1Authenticating(db, user, pass, _, _, _, _, _) =>
        Some((db, user, pass))

      case _ =>
        None
    }
}

case class CrAuthenticating(db: String, user: String, password: String, nonce: Option[String]) extends Authenticating {
  override def toString: String =
    s"Authenticating($db, $user, ${nonce.map(_ => "<nonce>").getOrElse("<>")})"
}

case class ScramSha1Authenticating(
  db: String, user: String, password: String,
  randomPrefix: String, saslStart: String,
  conversationId: Option[Int] = None,
  serverSignature: Option[Array[Byte]] = None,
  step: Int = 0) extends Authenticating {

  override def toString: String =
    s"Authenticating($db, $user})"
}

case class Authenticated(db: String, user: String) extends Authentication

@deprecated("Internal class: will be made private", "0.11.14")
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

@deprecated(message = "Will be made private", since = "0.11.10")
class RoundRobiner[A, M[T] <: Iterable[T]](val subject: M[A], startAtIndex: Int = 0) {
  private val iterator = new ContinuousIterator(subject)
  private val length = subject.size

  def pick: Option[A] = if (iterator.hasNext) Some(iterator.next) else None

  def pickWithFilter(filter: A => Boolean): Option[A] =
    pickWithFilter(filter, 0)

  @annotation.tailrec
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

/**
 * @param supervisor the name of the driver supervisor
 * @param connection the name of the connection pool
 */
@deprecated("Internal class: will be made private", "0.11.14")
final class ChannelFactory private[reactivemongo] (
  supervisor: String,
  connection: String,
  options: MongoConnectionOptions,
  bossExecutor: Executor = Executors.newCachedThreadPool,
  workerExecutor: Executor = Executors.newCachedThreadPool) {

  @deprecated("Initialize with related mongosystem", "0.11.14")
  def this(opts: MongoConnectionOptions) =
    this(
      s"unknown-${System identityHashCode opts}",
      s"unknown-${System identityHashCode opts}", opts)

  @deprecated("Initialize with related mongosystem", "0.11.14")
  def this(opts: MongoConnectionOptions, bossEx: Executor) =
    this(
      s"unknown-${System identityHashCode opts}",
      s"unknown-${System identityHashCode opts}", opts, bossEx)

  @deprecated("Initialize with related mongosystem", "0.11.14")
  def this(opts: MongoConnectionOptions, bossEx: Executor, workerEx: Executor) =
    this(
      s"unknown-${System identityHashCode opts}",
      s"unknown-${System identityHashCode opts}", opts, bossEx, workerEx)

  import javax.net.ssl.SSLContext

  private val logger = LazyLogger("reactivemongo.core.nodeset.ChannelFactory")
  private val timer = new HashedWheelTimer()

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef): Channel = {
    val channel = makeChannel(receiver)
    logger.trace(s"[$supervisor/$connection] Created a new channel: $channel")
    channel
  }

  val channelFactory = new NioClientSocketChannelFactory(
    bossExecutor, workerExecutor)

  private val bufferFactory = new HeapChannelBufferFactory(
    java.nio.ByteOrder.LITTLE_ENDIAN)

  private def makePipeline(timeoutMS: Long, receiver: ActorRef): ChannelPipeline = {
    val idleHandler = new IdleStateHandler(
      timer, 0, 0, timeoutMS, TimeUnit.MILLISECONDS)

    val pipeline = Channels.pipeline(idleHandler, new ResponseFrameDecoder(),
      new ResponseDecoder(), new RequestEncoder(),
      new MongoHandler(supervisor, connection, receiver))

    if (options.sslEnabled) {

      val sslEng = {
        val engine = sslContext.createSSLEngine()
        engine.setUseClientMode(true)
        engine
      }

      val sslHandler =
        new shaded.netty.handler.ssl.SslHandler(sslEng, false /* TLS */ )

      pipeline.addFirst("ssl", sslHandler)
    }

    pipeline
  }

  private def sslContext = {
    import java.io.FileInputStream
    import java.security.KeyStore
    import javax.net.ssl.{ KeyManagerFactory, TrustManager }

    val keyManagers = Option(System.getProperty("javax.net.ssl.keyStore")).map { path =>

      val password = Option(System.getProperty("javax.net.ssl.keyStorePassword")).getOrElse("")

      val ks = {
        val ksType = Option(System.getProperty("javax.net.ssl.keyStoreType")).getOrElse("JKS")
        val res = KeyStore.getInstance(ksType)

        val fis = new FileInputStream(path)
        try {
          res.load(fis, password.toCharArray)
        } finally {
          fis.close()
        }

        res
      }

      val kmf = {
        val res = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        res.init(ks, password.toCharArray)
        res
      }

      kmf.getKeyManagers
    }

    val sslCtx = {
      val res = SSLContext.getInstance("SSL")

      val tm: Array[TrustManager] = if (options.sslAllowsInvalidCert) Array(TrustAny) else null

      val rand = new scala.util.Random(System.identityHashCode(tm))
      val seed = Array.ofDim[Byte](128)
      rand.nextBytes(seed)

      res.init(keyManagers.orNull, tm, new java.security.SecureRandom(seed))
      res
    }

    sslCtx
  }

  private def makeChannel(receiver: ActorRef): Channel = {
    val channel = channelFactory.newChannel(makePipeline(
      options.maxIdleTimeMS.toLong, receiver))
    val config = channel.getConfig

    config.setTcpNoDelay(options.tcpNoDelay)
    config.setBufferFactory(bufferFactory)
    config.setKeepAlive(options.keepAlive)
    config.setConnectTimeoutMillis(options.connectTimeoutMS)

    logger.debug(s"Netty channel configuration:\n- connectTimeoutMS: ${options.connectTimeoutMS}\n- maxIdleTimeMS: ${options.maxIdleTimeMS}ms\n- tcpNoDelay: ${options.tcpNoDelay}\n- keepAlive: ${options.keepAlive}\n- sslEnabled: ${options.sslEnabled}")

    channel
  }

  private object TrustAny extends javax.net.ssl.X509TrustManager {
    import java.security.cert.X509Certificate

    override def checkClientTrusted(cs: Array[X509Certificate], a: String) = {}
    override def checkServerTrusted(cs: Array[X509Certificate], a: String) = {}
    override def getAcceptedIssuers(): Array[X509Certificate] = null
  }
}
