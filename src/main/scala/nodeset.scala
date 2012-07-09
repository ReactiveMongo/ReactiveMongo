package org.asyncmongo.nodeset

import akka.actor._
import org.asyncmongo.bson._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.ChannelState._
import org.asyncmongo.protocol.messages.{Authenticate => AuthenticateCommand, _}
import org.asyncmongo.protocol.NodeState._
import java.net.InetSocketAddress
import org.jboss.netty.buffer._
import org.jboss.netty.channel.{Channels, Channel, ChannelPipeline}
import org.jboss.netty.channel.socket.nio._

case class MongoChannel(
  channel: Channel,
  state: ChannelState,
  loggedIn: Set[LoggedIn]
) {
  lazy val useable = state == Useable
}

object MongoChannel {
  implicit def mongoChannelToChannel(mc: MongoChannel) :Channel = mc.channel
}

case class Node(
  name: String,
  channels: IndexedSeq[MongoChannel],
  state: NodeState,
  mongoId: Option[Int]
) {
  lazy val (host :String, port :Int) = {
    val splitted = name.span(_ != ':')
    splitted._1 -> (try {
      splitted._2.drop(1).toInt
    } catch {
      case _ => 27017
    })
  }

  lazy val isQueryable :Boolean = (state == PRIMARY || state == SECONDARY) && queryable.size > 0

  lazy val queryable :IndexedSeq[MongoChannel] = channels.filter(_.useable == true)

  def updateChannelById(channelId: Int, transform: (MongoChannel) => MongoChannel) :Node =
    copy(channels = channels.map(channel => if(channel.getId == channelId) transform(channel) else channel))

  def connect() :Unit = channels.foreach(channel => if(!channel.isConnected) channel.connect(new InetSocketAddress(host, port)))

  def disconnect() :Unit = channels.foreach(channel => if(channel.isConnected) channel.disconnect)

  def close() :Unit = channels.foreach(channel => if(channel.isOpen) channel.close)
}

object Node {
  def apply(name: String) :Node = new Node(name, Vector.empty, NONE, None)
  def apply(name: String, state: NodeState) :Node = new Node(name, Vector.empty, state, None)
}

case class NodeSet(
  name: Option[String],
  version: Option[Long],
  nodes: IndexedSeq[Node]
) {
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
  def send(message :Request, writeConcern :Request) {
    pick.map { channel =>
      log("connection " + channel.getId + " will send Request " + message + " followed by writeConcern " + writeConcern)
      channel.write(message)
      channel.write(writeConcern)
    }
  }
  def send(message: Request) {
    pick.map { channel =>
      log("connection " + channel.getId + " will send Request " + message)
      channel.write(message)
    }
  }
  def log(s: String) = println("NodeWrapper[" + node + "] :: " + s)
}

case class NodeSetManager(nodeSet: NodeSet) extends RoundRobiner(nodeSet.queryable.map { node => NodeWrapper(node)}) {
  def pickNode :Option[Node] = pick.map(_.node)
  def pickChannel :Option[Channel] = pick.flatMap(_.pick.map(_.channel))

  def getNodeWrapperByChannelId(channelId: Int) = subject.find(_.node.channels.exists(_.getId == channelId))

  def primaryWrapper :Option[NodeWrapper] = {
    val yy = subject.find(_.node.state == PRIMARY)
    println("finding primaryWrapper = " + yy)
    yy
  }
}

case class LoggedIn(db: String, user: String)

object ChannelFactory {
  import java.net.InetSocketAddress
  import java.util.concurrent.Executors

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef) = {
    val channel = makeChannel(receiver)
    println("created a new channel: " + channel)
    channel
  }

  private val channelFactory = new NioClientSocketChannelFactory(
    Executors.newCachedThreadPool,
    Executors.newCachedThreadPool
  )

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