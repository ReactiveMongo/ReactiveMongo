package org.asyncmongo.actors

import akka.actor._
import akka.dispatch.Future
import akka.pattern.ask
import akka.routing.{Broadcast, RoundRobinRouter}
import akka.util.Timeout

import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.messages._

import java.net.InetSocketAddress
import java.nio.ByteOrder

import org.jboss.netty.bootstrap._
import org.jboss.netty.buffer._
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._


private[asyncmongo] class RoundRobin[A] (
  init: Seq[A]
) extends Iterable[A] {
  import scala.collection.mutable.ArrayBuffer
  private val elems = new ArrayBuffer[A]()
  private var indx = Int.MaxValue

  elems ++= init

  def pick :Option[A] = if(elems.length > 0) {
    indx = if(indx >= elems.length - 1) 0 else indx + 1
    Some(elems(indx))
  } else None

  def add(elem: A) :Unit = elems += elem

  override def iterator :Iterator[A] = elems.iterator
}

private[asyncmongo] case class Node private(
  host: String,
  port: Int,
  channels: RoundRobin[Channel]
) {
  def send(message :WritableMessage[WritableOp], writeConcern :WritableMessage[WritableOp]) {
    channels.pick.map { channel =>
      log("connection " + channel.getId + " will send WritableMessage " + message + " followed by writeConcern " + writeConcern)
      channel.write(message)
      channel.write(writeConcern)
    }
  }
  def send(message: WritableMessage[WritableOp]) {
    channels.pick.map { channel =>
      log("connection " + channel.getId + " will send WritableMessage " + message)
      channel.write(message)
    }
  }
  def log(s: String) = "Node[" + url + "] :: " + s
  def deeperToString = "Node[" + url + "] of connections: \n\t" + channels.map(channel => {
    val address = channel.getRemoteAddress
    address.toString + "/" + channel.getId
  }).mkString(",\n\t") + "."

  lazy val url :String = host + ":" + port
  override def toString :String = deeperToString//"Node[" + url + "]"
}

private[asyncmongo] object Node {
  def apply(host: String, port: Int, creator: ActorRef, numberOfConnections: Int) : Node = {
    Node(host, port, new RoundRobin(for(i <- 0 until numberOfConnections) yield ChannelFactory.create(host, port, creator)))
  }
}

private[asyncmongo] class NodeManager private(
  val nodes :RoundRobin[Node],
  val creator :ActorRef,
  val numberOfConnectionsPerNode :Int = 3
) {
  def addNode(host: String, port: Int, numberOfConnections :Int = numberOfConnectionsPerNode) :Node = {
    nodes.find(_.url == (host + ":" + port)).getOrElse({
      val node = Node(host, port, creator, numberOfConnections)
      nodes.add(node)
      node
    })
  }
  def findByChannelId(channelId: Int) :Option[(Node, Channel)] = nodes.flatMap(node => {
    node.channels.map { channel =>
      node -> channel
    }
  }).find(_._2.getId == channelId)
  def pick :Option[Node] = nodes.pick
  override def toString = {
    "NodeManager [" + nodes.mkString("\n") + "]"
  }
}

private[asyncmongo] object NodeManager {
  def apply(creator: ActorRef, numberOfConnections: Int, node: (String, Int), nodes: (String, Int)*) :NodeManager = {
    apply(creator, numberOfConnections, (node +: nodes.toList))
  }
  def apply(creator: ActorRef, numberOfConnections: Int, nodes: List[(String, Int)]) :NodeManager = {
    new NodeManager(new RoundRobin( nodes.map { n =>
      Node(n._1, n._2, creator, numberOfConnections)
    }), creator, numberOfConnections)
  }
}

object ChannelFactory {
  import java.net.InetSocketAddress
  import java.util.concurrent.Executors

  val factory = new NioClientSocketChannelFactory(
    Executors.newCachedThreadPool,
    Executors.newCachedThreadPool
  )

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef) = {
    val bootstrap = new ClientBootstrap(factory)

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline :ChannelPipeline = {
        Channels.pipeline(new WritableMessageEncoder(), new ReplyFrameDecoder(), new ReplyDecoder(), new MongoHandler(receiver))
      }
    })

    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("bufferFactory", new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN))
    bootstrap.connect(new InetSocketAddress(host, port)).await.getChannel
  }
}

class MongoDBSystem(val nodes: List[(String, Int)]) extends Actor {
  val nodeManager = NodeManager(self, 3, nodes)

  var primary :Option[Node] = None
  var isElecting = false

  log("starting with " + nodeManager)
  elect

  // todo: if primary changes again while sending queued messages ?
  private val queuedMessages = scala.collection.mutable.Queue[(ActorRef, Either[(WritableMessage[WritableOp], WritableMessage[WritableOp]), WritableMessage[WritableOp]])]()
  private val awaitingResponses = scala.collection.mutable.ListMap[Int, ActorRef]()

  override def receive = {
    // todo: refactoring
    case message: WritableMessage[WritableOp] => {
      log("received a message!")
      if(message.op.expectsResponse) {
        awaitingResponses += ((message.requestID, sender))
        log("registering awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      } else log("NOT registering awaiting response for requestID " + message.requestID)
      if(secondaryOK(message)) {
        val server = pickNode(message)
        log("node " + server + "will send the query " + message.op)
        server.map(_.send(message))
      } else if(!primary.isDefined) {
        log("delaying send because primary is not available")
        queuedMessages += (sender -> Right(message))
        if(!isElecting) elect
      } else {
        primary.get.send(message)
      }
    }
    case (message: WritableMessage[WritableOp], writeConcern: WritableMessage[WritableOp]) => {
      log("received a message!")
      awaitingResponses += ((message.requestID, sender))
      log("registering writeConcern-awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      if(secondaryOK(message)) {
        pickNode(message).map(_.send(message, writeConcern))
      } else if(!primary.isDefined) {
        log("delaying send because primary is not available")
        queuedMessages += (sender -> Left(message -> writeConcern))
        if(!isElecting) elect
      } else {
        primary.get.send(message, writeConcern)
      }
    }
    case message: ReadReply => {
      awaitingResponses.get(message.header.responseTo) match {
        case Some(_sender) => {
          log("Got a response from " + message.info.channelId + "! Will give back message="+message + " to sender " + _sender)
          awaitingResponses -= message.header.responseTo
          _sender ! message
        }
        case None => {
          log("oups. " + message.header.responseTo + " not found! complete message is " + message)
          log("maybe it's for myself?") // check if the message is an ismaster msg.
          val isMasterResponse = handleIsMasterResponse(DefaultBSONReaderHandler.handle(message.reply, message.documents).next)
          log("response ::: "+ isMasterResponse)
          isMasterResponse.map { isMaster =>
            if(isMaster.state == PRIMARY)
              nodeManager.findByChannelId(message.info.channelId).map( node => foundPrimary(node._1))
            isMaster.knownNodes.foreach { node =>
              nodeManager.addNode(node._1, node._2)
            }
          }
          log("added nodes, now nodes are : " + nodeManager)
        }
      }
    }
    case _ => log("not supported")
  }

  def log(s: String) = println("MongoDBSystem [" + self.path + "] : " + s)

  def foundPrimary(node: Node) {
    primary = Some(node)
    isElecting = false
    log("ok, found primary, sending all the queued messages... node=" + node)
    queuedMessages.dequeueAll( _ => true).foreach {
      case (originalSender, Left( (message, writeConcern) )) => node.send(message, writeConcern)
      case (originalSender, Right(message)) => node.send(message)
    }
  }

  def secondaryOK(message: WritableMessage[WritableOp]) = !message.op.requiresPrimary && (message.op match {
    case query: Query => (query.flags & QueryFlags.SlaveOk) != 0
    case _ :KillCursors => true
    case _ :GetMore => true
    case _ => false
  })

  def pickNode(message: WritableMessage[WritableOp]) :Option[Node] = {
    message.channelIdHint.flatMap(channelId => {
      nodeManager.findByChannelId(channelId).map(_._1)
    }).orElse(nodeManager.pick)
  }

  def elect = {
    log("starting electing process...")
    isElecting = true
    for(node <- nodeManager.nodes) {
      node.send(IsMaster().makeWritableMessage)
    }
    //nodeManager
  }


/*
Map(
  setName -> BSONString(setName,testmongodriver),
  secondary -> BSONBoolean(secondary,true),
  maxBsonObjectSize -> BSONInteger(maxBsonObjectSize,16777216),
  hosts -> BSONArray(hosts,LittleEndianHeapChannelBuffer(ridx=0, widx=164, cap=164)),
  me -> BSONString(me,MacBook-Pro-de-Stephane-Godbillon.local:27018),
  ok -> BSONDouble(ok,1.0),
  ismaster -> BSONBoolean(ismaster,false),
  primary -> BSONString(primary,MacBook-Pro-de-Stephane-Godbillon.local:27017))
*/


  sealed trait NodeState
  object PRIMARY extends NodeState
  object SECONDARY extends NodeState
  object ANY_OTHER_STATE extends NodeState // well, we dont care for other states right now

  case class IsMasterResponse(
    state: NodeState,
    me: Option[(String, Int)],
    knownNodes: List[(String, Int)]
  )

  def handleIsMasterResponse(document: BSONIterator) :Option[IsMasterResponse] = {
    val map = document.mapped
    println(map)
    map.get("ismaster").flatMap {
      case BSONBoolean(_, primary) => {
        val secondary = (map.get("secondary") map {
          case BSONBoolean(_, b) => b
          case _ => false
        }).getOrElse(false)
        Some(IsMasterResponse(
          if(primary) PRIMARY else if(secondary) SECONDARY else ANY_OTHER_STATE,
          map.get("me") flatMap {
            case BSONString(_, me) => Some(parseNodeAddress(me))
            case _ => None
          },
          map.get("hosts").map({
            case BSONArray(_, buffer) => (for(e <- DefaultBSONIterator(buffer)) yield {
              parseNodeAddress(e.asInstanceOf[BSONString].value)
            }).toList
            case _ => Nil
          }).getOrElse(Nil)
        ))
      }
      case _ => None
    }
  }

  def parseNodeAddress(address: String) :(String, Int) = {
    val (name, port) = address.span(_ != ':')
    (name, try {
      port.drop(1).toInt
    } catch {
      case _ => 27017
    })
  }

  def parseForRSNodes(document: BSONIterator) :Option[List[(String, Int, NodeState)]]= {
    val map = document.mapped
    println(map)
    map.get("members").flatMap {
      case BSONArray(_, b) => {
        val it: Iterator[(String, Int, NodeState)] = for(o <- DefaultBSONIterator(b)) yield {
          val member = DefaultBSONIterator(o.asInstanceOf[BSONDocument].value).mapped
          val (name, port) = member.get("name").get.asInstanceOf[BSONString].value.span(_ != ':')
          val p2 = port.drop(1)
          (name, port.drop(1).toInt, member.get("state").get.asInstanceOf[BSONInteger].value match {
            case 1 => PRIMARY
            case 2 => SECONDARY
            case _ => ANY_OTHER_STATE
          })
        }
        Some(it.toList)
      }
      case _ => None
    }
  }
}

class MongoConnection(
  val mongosystem: ActorRef
) {
  /** write an op and wait for db response */
  def ask(message: WritableMessage[WritableOp])(implicit timeout: Timeout) :Future[ReadReply] = {
    (mongosystem ? message).mapTo[ReadReply]
  }

  /** write a no-response op followed by a GetLastError command and wait for its response */
  def ask(message: WritableMessage[WritableOp], writeConcern: GetLastError = GetLastError())(implicit timeout: Timeout) = {
    (mongosystem ? ((message, writeConcern.makeWritableMessage("plugin", message.header.requestID)))).mapTo[ReadReply]
  }

  /** write a no-response op without getting a future */
  def send(message: WritableMessage[WritableOp]) = mongosystem ! message

  def stop = MongoConnection.system.stop(mongosystem)
}

object MongoConnection {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load()

  val system = ActorSystem("mongodb", config.getConfig("mongo-async-driver"))

  def apply(nodes: List[(String, Int)], name: Option[String]= None) = {
    val props = Props(new MongoDBSystem(nodes))
    new MongoConnection(if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props))
  }
}