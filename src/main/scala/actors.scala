package org.asyncmongo.actors

import akka.actor._
import akka.actor.Status.Failure
import akka.dispatch.Future
import akka.pattern.ask
import akka.routing.{Broadcast, RoundRobinRouter}
import akka.util.duration._
import akka.util.Timeout
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.ChannelState._
import org.asyncmongo.protocol.messages.{Authenticate => AuthenticateCommand, _}
import org.asyncmongo.protocol.NodeState._
import java.net.InetSocketAddress
import java.nio.ByteOrder
import org.jboss.netty.bootstrap._
import org.jboss.netty.buffer._
import org.jboss.netty.channel.{Channels, Channel, ChannelPipeline}
import org.jboss.netty.channel.socket.nio._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ObservableBuffer
import org.asyncmongo.handlers.DefaultBSONHandlers

case class MongoChannel(channel: Channel, state: ChannelState) {
  lazy val useable = state.isInstanceOf[Useable]
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

  def connect() :Unit = channels.foreach(channel => if(!channel.isConnected) channel.connect(new InetSocketAddress(host, port)))

  def disconnect() :Unit = channels.foreach(channel => if(channel.isConnected) channel.disconnect)

  def close() :Unit = channels.foreach(channel => if(channel.isOpen) channel.close)
}

object Node {
  def apply(name: String) :Node = new Node(name, Vector.empty, NONE, None)
  def apply(name: String, state: NodeState) :Node = new Node(name, Vector.empty, state, None)
  def apply(name: String, nbConnections: Int, creator: ActorRef) :Node = {
    val node = Node(name)
    node.copy(channels = for(i <- 0 until nbConnections) yield MongoChannel(ChannelFactory.create(node.host, node.port, creator), NotConnected))
  }
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

trait MongoError extends Throwable {
  val code: Option[Int]
  val message: Option[String]
  override def getMessage :String = "MongoError[code=" + code.getOrElse("None") + " => message: " + message.getOrElse("None") + "]"
}
case class DefaultMongoError(
  message: Option[String],
  code: Option[Int]
) extends MongoError

case class LastError(
  ok: Boolean,
  err: Option[String],
  code: Option[Int],
  message: Option[String],
  original: Map[String, BSONElement]
) {
  lazy val inError :Boolean = !ok || err.isDefined
  lazy val stringify :String = toString + " [inError: " + inError + "]"
}

object LastError {
  def apply(bson: BSONIterator) :LastError = {
    val mapped = bson.mapped
    LastError(
      mapped.get("ok").flatMap {
        case d: BSONDouble => Some(true)
        case _ => None
      }.getOrElse(true),
      mapped.get("err").flatMap {
        case s: BSONString => Some(s.value)
        case _ => None
      },
      mapped.get("code").flatMap {
        case i: BSONInteger => Some(i.value)
        case _ => None
      },
      mapped.get("errmsg").flatMap {
        case s: BSONString => Some(s.value)
        case _ => None
      },
      mapped
    )
  }
}

private[asyncmongo] case class AwaitingResponse(
  requestID: Int,
  actor: ActorRef,
  isGetLastError: Boolean
)

case object Init
case object ConnectAll
case object RefreshAllNodes
case object Close
case class Authenticate(db: String, user: String, password: String)

case class Connected(channelId: Int)
case class Disconnected(channelId: Int)

class MongoDBSystem(seeds: List[String] = List("localhost:27017")) extends Actor {
  val requestIdIterator :Iterator[Int] = Iterator.iterate(3000)(i => if(i == Int.MaxValue) 3000 else i + 1)
  def requestId :Int = requestIdIterator.next

  // todo: if primary changes again while sending queued messages ? -> don't care, an error will be sent as the result of the promise
  private val queuedMessages = scala.collection.mutable.Queue[(ActorRef, Either[(Request, Request), Request])]()
  private val awaitingResponses = scala.collection.mutable.ListMap[Int, AwaitingResponse]()
  
  private val connectAllJob = context.system.scheduler.schedule(MongoDBSystem.DefaultConnectionRetryInterval milliseconds,
    MongoDBSystem.DefaultConnectionRetryInterval milliseconds,
    self,
    ConnectAll)
  // for tests only
  private val refreshAllJob = context.system.scheduler.schedule(MongoDBSystem.DefaultConnectionRetryInterval*5 milliseconds,
    MongoDBSystem.DefaultConnectionRetryInterval*5 milliseconds,
    self,
    RefreshAllNodes)
  
  val receiveRequest : (Request) => Unit = request => {
    log("received a request!")
    if(request.op.expectsResponse) {
      awaitingResponses += request.requestID -> AwaitingResponse(request.requestID, sender, false)
      log("registering awaiting response for requestID " + request.requestID + ", awaitingResponses: " + awaitingResponses)
    } else log("NOT registering awaiting response for requestID " + request.requestID)
    if(secondaryOK(request)) {
      val server = pickNode(request)
      log("node " + server + "will send the query " + request.op)
      server.map(_.send(request))
    } else if(!nodeSetManager.primaryWrapper.isDefined) {
      log("delaying send because primary is not available")
      queuedMessages += (sender -> Right(request))
    } else {
      nodeSetManager.primaryWrapper.get.send(request)
    }
  }
  
  val receiveCheckedRequest: (Request, Request) => Unit = (request, writeConcern) => {
    log("received a message!")
    awaitingResponses += request.requestID -> AwaitingResponse(request.requestID, sender, true)
    log("registering writeConcern-awaiting response for requestID " + request.requestID + ", awaitingResponses: " + awaitingResponses)
    if(secondaryOK(request)) {
      pickNode(request).map(_.send(request, writeConcern))
    } else if(!nodeSetManager.primaryWrapper.isDefined) {
      log("delaying send because primary is not available")
      queuedMessages += (sender -> Left(request -> writeConcern))
      log("now queuedMessages is " + queuedMessages)
    } else {
      nodeSetManager.primaryWrapper.get.send(request, writeConcern)
    }
  }

  override def receive = {
    // todo: refactoring
    case request: Request => receiveRequest(request)
    case requestMaker :RequestMaker => receiveRequest(requestMaker(requestId))
    case (request: Request, writeConcern: Request) => receiveCheckedRequest(request, writeConcern)
    case (requestMaker: RequestMaker, writeConcernMaker: RequestMaker) => {
      val id = requestId
      receiveCheckedRequest(requestMaker(id), writeConcernMaker(id))
    }
    
    // monitor
    case ConnectAll => {
      log("ConnectAll Job running...")
      nodeSetManager = NodeSetManager(createNeededChannels(nodeSetManager.nodeSet))
      nodeSetManager.nodeSet.connectAll
    }
    case RefreshAllNodes => {
      val state = "setName=" + nodeSetManager.nodeSet.name + " with nodes={" +
      (for(node <- nodeSetManager.nodeSet.nodes) yield "['" + node.name + "' in state " + node.state + " with " + node.channels.foldLeft(0) { (count, channel) =>
        if(channel.isConnected) count + 1 else count
      } + " connected channels]") + "}";
      log("RefreshAllNodes Job running... current state is: " + state)
      nodeSetManager.nodeSet.nodes.foreach { node =>
        log("try to refresh " + node.name)
        node.channels.find(_.isConnected).map(_.write(IsMaster().maker(isMasterRequestId)))
      }
    }
    case Connected(channelId) => {
      if(seedSet.isDefined) {
        seedSet = seedSet.map(_.updateByChannelId(channelId, node => node.copy(state = CONNECTED)))
        seedSet.map(_.findNodeByChannelId(channelId).get.channels(0).write(IsMaster().maker(isMasterRequestId)))
      } else {
        nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(channelId, node => {
            //node.channels.find(_.getId == channelId).map(_.write(IsMaster().maker(monitoringRequestId)))
          node.copy(channels = node.channels.map { channel =>
            if(channel.getId == channelId) {
              channel.write(IsMaster().maker(isMasterRequestId))
              channel.copy(state = Useable(None))
            } else channel
          }, state = node.state match {
            case _: MongoNodeState => node.state
            case _ => CONNECTED
          })
        }))
        //nodeSetManager.getNodeWrapperByChannelId(channelId).map(_.send(message))
      }
      log(channelId + " is connected")
    }
    case Disconnected(channelId) => {
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(channelId, node => {
        //node.channels.foreach(channel => if(channel.isOpen) channel.close)
        node.copy(state = NOT_CONNECTED, channels = node.channels.collect{ case channel if channel.isOpen => channel })
      }))
      log(channelId + " is disconnected")
    }
    case Authenticate(db, user, password) => {
      log("authenticate process starts...")
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateAll(node =>
        node.copy(channels = node.channels.map { channel =>
          channel.write(Getnonce(db).maker(getNonceRequestId))
          channel.copy(state = Authenticating(db, user, password, None))
        })
      ))
    }
    // isMaster response
    case response: Response if response.header.responseTo < 1000 => {
      val isMaster = IsMasterResponse(response)
      nodeSetManager = if(isMaster.hosts.isDefined) {// then it's a ReplicaSet
        val mynodes = isMaster.hosts.get.map(name => Node(name, if(isMaster.me.exists(_ == name)) isMaster.state else NONE))
        NodeSetManager(createNeededChannels(nodeSetManager.nodeSet.addNodes(mynodes).copy(name = isMaster.setName)))
      } else if(nodeSetManager.nodeSet.nodes.length > 0) {
        log("single node, update..." + nodeSetManager)
        NodeSetManager(createNeededChannels(NodeSet(None, None, nodeSetManager.nodeSet.nodes.slice(0, 1).map(_.copy(state = isMaster.state)))))
      } else if(seedSet.isDefined && seedSet.get.findNodeByChannelId(response.info.channelId).isDefined) {
        log("single node, creation..." + nodeSetManager)
        NodeSetManager(createNeededChannels(NodeSet(None, None, Vector(Node(seedSet.get.findNodeByChannelId(response.info.channelId).get.name)))))
      } else throw new RuntimeException("single node discovery failure...")
      log("NodeSetManager is now " + nodeSetManager)
      if(nodeSetManager.primaryWrapper.isDefined)
        foundPrimary()
      nodeSetManager.nodeSet.connectAll
      if(seedSet.isDefined) {
        seedSet.get.closeAll
        seedSet = None
        println("Init is done")
      }
    }
    // getnonce response
    case response: Response if response.header.responseTo >= 1000 && response.header.responseTo < 2000 => {
      val getnonce = GetnonceResult(response)
      log("got nonce for channel " + response.info.channelId + ": " + getnonce.nonce)
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(response.info.channelId, node =>
        node.copy(channels = node.channels.map { channel =>
          if(channel.getId == response.info.channelId) {
            val authenticating = channel.state.asInstanceOf[Authenticating]
            channel.write(AuthenticateCommand(authenticating.db, authenticating.user, authenticating.password, getnonce.nonce).maker(authenticateRequestId))
            channel.copy(state = authenticating.copy(nonce = Some(getnonce.nonce)))
          } else channel
        })
      ))
    }
    // authenticate response
    case response: Response if response.header.responseTo >= 2000 && response.header.responseTo < 3000 => {
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(response.info.channelId, node =>
        node.copy(channels = node.channels.map { channel =>
          if(channel.getId == response.info.channelId) {
            val authenticating = channel.state.asInstanceOf[Authenticating]
            log("channel " + channel.channel + " is authenticated with " + authenticating + "!")
            channel.copy(state = Useable(None))
          } else channel
        })
      ))
    }
    
    // any other response
    case response: Response if response.header.responseTo >= 3000 => {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _sender, isGetLastError)) => {
          log("Got a response from " + response.info.channelId + "! Will give back message="+response + " to sender " + _sender)
          awaitingResponses -= response.header.responseTo
          if(isGetLastError) {
            log("{" + response.header.responseTo + "} it's a getlasterror")
            // todo, for now rewinding buffer at original index
            val ridx = response.documents.readerIndex
            val lastError = LastError(DefaultBSONReaderHandler.handle(response.reply, response.documents).next)
            if(lastError.code.isDefined && isNotPrimaryErrorCode(lastError.code.get)) {
              log("{" + response.header.responseTo + "} sending a failure...")
              self ! RefreshAllNodes
              nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateAll(node => if(node.state == PRIMARY) node.copy(state = UNKNOWN) else node))
              _sender ! Failure(DefaultMongoError(lastError.message, lastError.code))
            } else {
              response.documents.readerIndex(ridx)
              _sender ! response
            }
          } else {
            log("{" + response.header.responseTo + "} sending a success!")
            _sender ! response
          }
        }
        case None => {
          log("oups. " + response.header.responseTo + " not found! complete message is " + response)
        }
      }
    }
    case a @ _ => log("not supported " + a)
  }
  
  // monitor -->
  var seedSet :Option[NodeSet] = Some(NodeSet(None, None, seeds.map(seed => Node(seed, 1, self)).toIndexedSeq))
  seedSet.get.connectAll
  
  var nodeSetManager = NodeSetManager(NodeSet(None, None, Vector.empty))
  
  // all requestIds [0, 1000[ are for isMaster messages
  val isMasterRequestIdIterator :Iterator[Int] = Iterator.iterate(0)(i => if(i == 1000) 0 else i + 1)
  def isMasterRequestId = isMasterRequestIdIterator.next
  
  // all requestIds [1000, 2000[ are for getnonce messages
  val getNonceRequestIdIterator :Iterator[Int] = Iterator.iterate(1000)(i => if(i == 2000) 1000 else i + 1)
  def getNonceRequestId = getNonceRequestIdIterator.next
  
  // all requestIds [2000, 3000[ are for authenticate messages
  val authenticateRequestIdIterator :Iterator[Int] = Iterator.iterate(2000)(i => if(i == 3000) 2000 else i + 1)
  def authenticateRequestId = authenticateRequestIdIterator.next
    
  def createNeededChannels(nodeSet: NodeSet) :NodeSet = {
    nodeSet.copy(nodes = nodeSet.nodes.foldLeft(Vector.empty[Node]) { (nodes, node) =>
      nodes :+ createNeededChannels(node)
    })
  }
  
  def createNeededChannels(node: Node) :Node = {
    node.copy(channels = node.channels ++ (
      for(i <- 0 until (3 - node.channels.size))
        yield MongoChannel(ChannelFactory.create(node.host, node.port, self), NotConnected))
    )
  }
  
  def makeChannels(node: Node) :Node = {
    if(node.channels.size < 3) {
      node.copy(channels = node.channels.++(for(i <- 0 until (3 - node.channels.size)) yield MongoChannel(ChannelFactory.create(node.host, node.port, self), NotConnected)))
    } else node
  }
  // <-- monitor

  def isNotPrimaryErrorCode(code: Int) = code match {
    case 10054 | 10056 | 10058 | 10107 | 13435 | 13436 => true
    case _ => false
  }

  def log(s: String) = println("MongoDBSystem [" + self.path + "] : " + s)
  
  def foundPrimary() {
    log("ok, found primary, sending all the queued messages... ")
    queuedMessages.dequeueAll( _ => true).foreach {
      case (originalSender, Left( (message, writeConcern) )) => {
        nodeSetManager.primaryWrapper.get.send(message, writeConcern)
      }
      case (originalSender, Right(message)) => nodeSetManager.primaryWrapper.get.send(message)
    }
  }

  def secondaryOK(message: Request) = !message.op.requiresPrimary && (message.op match {
    case query: Query => (query.flags & QueryFlags.SlaveOk) != 0
    case _ :KillCursors => true
    case _ :GetMore => true
    case _ => false
  })

  def pickNode(message: Request) :Option[NodeWrapper] = {
    message.channelIdHint.flatMap(channelId => {
      nodeSetManager.getNodeWrapperByChannelId(channelId)
    }).orElse(nodeSetManager.pick)
  }
}

object MongoDBSystem {
  private[asyncmongo] val DefaultConnectionRetryInterval :Int = 2000 // milliseconds
}

class MongoConnection(
  val mongosystem: ActorRef
) {
  /** write an op and wait for db response */
  def ask(message: RequestMaker)(implicit timeout: Timeout) :Future[Response] = {
    (mongosystem ? message).mapTo[Response]
  }

  /** write a no-response op followed by a GetLastError command and wait for its response */
  def ask(message: RequestMaker, writeConcern: GetLastError)(implicit timeout: Timeout) = {
    (mongosystem ? ((message, writeConcern.maker))).mapTo[Response] // Broken
  }

  /** write a no-response op without getting a future */
  def send(message: RequestMaker) = mongosystem ! message

  /** authenticate on the given db. Must return a future. TODO */
  def authenticate(db: String, user: String, password: String) :Unit = {
    mongosystem ! Authenticate(db, user, password)
  }

  def stop = MongoConnection.system.stop(mongosystem)
}

object MongoConnection {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load()

  val system = ActorSystem("mongodb", config.getConfig("mongo-async-driver"))

  def apply(nodes: List[String], name: Option[String]= None) = {
    val props = Props(new MongoDBSystem(nodes))
    new MongoConnection(if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props))
  }
}