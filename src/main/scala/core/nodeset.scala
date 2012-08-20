package reactivemongo.core.nodeset

import akka.actor._
import java.net.InetSocketAddress
import java.util.concurrent.{Executor, Executors}
import org.jboss.netty.buffer._
import org.jboss.netty.channel.{Channels, Channel, ChannelPipeline}
import org.jboss.netty.channel.group._
import org.jboss.netty.channel.socket.nio._
import org.slf4j.{Logger, LoggerFactory}
import reactivemongo.bson._
import reactivemongo.core.protocol._
import reactivemongo.core.protocol.ChannelState._
import reactivemongo.core.commands.{Authenticate => AuthenticateCommand, _}
import reactivemongo.core.protocol.NodeState._
import reactivemongo.utils.LazyLogger

case class MongoChannel(
  channel: Channel,
  state: ChannelState,
  loggedIn: Set[LoggedIn]
) {
  lazy val usable = state match {
    case _ :Usable => true
    case _ => false
  }
}

object MongoChannel {
  implicit def mongoChannelToChannel(mc: MongoChannel) :Channel = mc.channel
}

case class Node(
  name: String,
  channels: IndexedSeq[MongoChannel],
  state: NodeState,
  mongoId: Option[Int]
)(implicit channelFactory: ChannelFactory) {
  lazy val (host :String, port :Int) = {
    val splitted = name.span(_ != ':')
    splitted._1 -> (try {
      splitted._2.drop(1).toInt
    } catch {
      case _ => 27017
    })
  }

  lazy val isQueryable :Boolean = (state == PRIMARY || state == SECONDARY) && queryable.size > 0

  lazy val queryable :IndexedSeq[MongoChannel] = channels.filter(_.usable == true)

  def updateChannelById(channelId: Int, transform: (MongoChannel) => MongoChannel) :Node =
    copy(channels = channels.map(channel => if(channel.getId == channelId) transform(channel) else channel))

  def connect() :Unit = channels.foreach(channel => if(!channel.isConnected) channel.connect(new InetSocketAddress(host, port)))

  def disconnect() :Unit = channels.foreach(channel => if(channel.isConnected) channel.disconnect)

  def close() :Unit = channels.foreach(channel => if(channel.isOpen) channel.close)

  def createNeededChannels(receiver: ActorRef, upTo: Int) :Node = {
    if(channels.size < upTo) {
      copy(channels = channels.++(for(i <- 0 until (upTo - channels.size)) yield
          MongoChannel(channelFactory.create(host, port, receiver), NotConnected, Set.empty)))
    } else this
  }
}

object Node {
  def apply(name: String)(implicit channelFactory: ChannelFactory) :Node = new Node(name, Vector.empty, NONE, None)
  def apply(name: String, state: NodeState)(implicit channelFactory: ChannelFactory) :Node = new Node(name, Vector.empty, state, None)
}

case class NodeSet(
  name: Option[String],
  version: Option[Long],
  nodes: IndexedSeq[Node]
)(implicit channelFactory: ChannelFactory) {
  def connected :IndexedSeq[Node] = nodes.filter(node => node.state != NOT_CONNECTED)

  def queryable :IndexedSeq[Node] = nodes.filter(_.isQueryable)

  def primary :Option[Node] = nodes.find(_.state == PRIMARY)

  def isReplicaSet :Boolean = name.isDefined

  def connectAll() :Unit = nodes.foreach(_.connect)

  def closeAll() :Unit = nodes.foreach(_.close)

  def findNodeByChannelId(channelId: Int) :Option[Node] = nodes.find(_.channels.exists(_.getId == channelId))

  def findByChannelId(channelId: Int) :Option[(Node, MongoChannel)] = nodes.flatMap(node => node.channels.map(node -> _)).find(_._2.getId == channelId)

  def updateByMongoId(mongoId: Int, transform: (Node) => Node) :NodeSet = {
    new NodeSet(name, version, nodes.updated(mongoId, transform(nodes(mongoId))))
  }

  def updateByChannelId(channelId :Int, transform: (Node) => Node) :NodeSet = {
    new NodeSet(name, version, nodes.map(node => if(node.channels.exists(_.getId == channelId)) transform(node) else node))
  }

  def updateAll(transform: (Node) => Node) :NodeSet = {
    new NodeSet(name, version, nodes.map(transform))
  }

  def channels = nodes.flatMap(_.channels)

  def addNode(node: Node) :NodeSet = {
    nodes.indexWhere(_.name == node.name) match {
      case -1 => this.copy(nodes = node +: nodes)
      case i => {
        val replaced = nodes(i)
        this.copy(nodes = nodes.updated(i, Node(node.name, replaced.channels, if(node.state != NONE) node.state else replaced.state, node.mongoId)))
      }
    }
  }

  def addNodes(nodes: Seq[Node]) :NodeSet = {
    nodes.foldLeft(this)(_ addNode _)
  }

  def merge(nodeSet: NodeSet) :NodeSet = {
    NodeSet(nodeSet.name, nodeSet.version, nodeSet.nodes.map { node =>
      nodes.find(_.name == node.name).map { oldNode =>
        node.copy(channels = oldNode.channels.union(node.channels).distinct)
      }.getOrElse(node)
    })
  }

  def createNeededChannels(receiver: ActorRef, upTo: Int) :NodeSet = {
    copy(nodes = nodes.foldLeft(Vector.empty[Node]) { (nodes, node) =>
      nodes :+ node.createNeededChannels(receiver, upTo)
    })
  }

  def makeChannelGroup() :ChannelGroup = {
    val result = new DefaultChannelGroup
    for(node <- nodes) {
      for(channel <- node.channels)
        result.add(channel.channel)
    }
    result
  }
}

class RoundRobiner[A](val subject: IndexedSeq[A], private var i: Int = 0) {
  private val length = subject.length

  if(i < 0) i = 0

  def pick :Option[A] = if(length > 0) {
    val result = Some(subject(i))
    i = if(i == length - 1) 0 else i + 1
    result
  } else None
}

case class NodeWrapper(node: Node) extends RoundRobiner(node.queryable) {
  import NodeWrapper._
  def send(message :Request, writeConcern :Request) {
    pick.map { channel =>
      logger.trace("connection " + channel.getId + " will send Request " + message + " followed by writeConcern " + writeConcern)
      channel.write(message)
      channel.write(writeConcern)
    }
  }
  def send(message: Request) {
    pick.map { channel =>
      logger.trace("connection " + channel.getId + " will send Request " + message)
      channel.write(message)
    }
  }
}

object NodeWrapper {
  private val logger = LazyLogger(LoggerFactory.getLogger("NodeWrapper"))
}

case class NodeSetManager(nodeSet: NodeSet) extends RoundRobiner(nodeSet.queryable.map { node => NodeWrapper(node)}) {
  def pickNode :Option[Node] = pick.map(_.node)
  def pickChannel :Option[Channel] = pick.flatMap(_.pick.map(_.channel))

  def getNodeWrapperByChannelId(channelId: Int) = subject.find(_.node.channels.exists(_.getId == channelId))

  def primaryWrapper :Option[NodeWrapper] = subject.find(_.node.state == PRIMARY)
}

case class LoggedIn(db: String, user: String)

class ChannelFactory(bossExecutor: Executor = Executors.newCachedThreadPool, workerExecutor: Executor = Executors.newCachedThreadPool) {
  private val logger = LazyLogger(LoggerFactory.getLogger("ChannelFactory"))

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef) = {
    val channel = makeChannel(receiver)
    logger.trace("created a new channel: " + channel)
    channel
  }

  val channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor)

  private val bufferFactory = new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN)

  private def makeOptions :java.util.HashMap[String, Object] = {
    val map = new java.util.HashMap[String, Object]()
    map.put("tcpNoDelay", true :java.lang.Boolean)
    map.put("bufferFactory", bufferFactory)
    map
  }

  private def makePipeline(receiver: ActorRef) :ChannelPipeline = Channels.pipeline(new RequestEncoder(), new ResponseFrameDecoder(), new ResponseDecoder(), new MongoHandler(receiver))

  private def makeChannel(receiver: ActorRef) :Channel = {
    val channel = channelFactory.newChannel(makePipeline(receiver))
    channel.getConfig.setOptions(makeOptions)
    channel
  }
}