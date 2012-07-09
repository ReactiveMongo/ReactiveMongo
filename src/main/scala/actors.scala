package org.asyncmongo.actors

import akka.actor._
import akka.actor.Status.Failure
import akka.util.duration._
import org.asyncmongo.bson._
import org.asyncmongo.nodeset._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.ChannelState._
import org.asyncmongo.protocol.messages.{Authenticate => AuthenticateCommand, _}
import org.asyncmongo.protocol.NodeState._

// messages
case class Authenticate(db: String, user: String, password: String)
case object Close
private[asyncmongo] case object ConnectAll
private[asyncmongo] case object RefreshAllNodes
private[asyncmongo] case class Connected(channelId: Int)
private[asyncmongo] case class Disconnected(channelId: Int)

class MongoDBSystem(seeds: List[String] = List("localhost:27017"), auth :List[Authenticate]) extends Actor {
  val requestIdGenerator = new {
    // all requestIds [0, 1000[ are for isMaster messages
    val isMasterRequestIdIterator :Iterator[Int] = Iterator.iterate(0)(i => if(i == 1000) 0 else i + 1)
    def isMaster = isMasterRequestIdIterator.next

    // all requestIds [1000, 2000[ are for getnonce messages
    val getNonceRequestIdIterator :Iterator[Int] = Iterator.iterate(1000)(i => if(i == 2000) 1000 else i + 1)
    def getNonce = getNonceRequestIdIterator.next

    // all requestIds [2000, 3000[ are for authenticate messages
    val authenticateRequestIdIterator :Iterator[Int] = Iterator.iterate(2000)(i => if(i == 3000) 2000 else i + 1)
    def authenticate = authenticateRequestIdIterator.next

    // all requestIds [3000[ are for user messages
    val userIterator :Iterator[Int] = Iterator.iterate(3000)(i => if(i == Int.MaxValue) 3000 else i + 1)
    def user :Int = userIterator.next
  }

  private var authenticationHistory :AuthHistory = AuthHistory(for(a <- auth) yield a -> Nil)

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
  
  def receiveRequest(request: Request): Unit = {
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
  
  def receiveCheckedRequest(request: Request, writeConcern: Request) :Unit = {
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

  def authenticateChannel(channel: MongoChannel, continuing: Boolean = false) :MongoChannel = channel.state match {
    case _: Authenticating if !continuing => {log("AUTH: delaying auth on " + channel);channel}
    case _ => if(channel.loggedIn.size < authenticationHistory.authenticates.size) {
      val nextAuth = authenticationHistory.authenticates(channel.loggedIn.size)
      log("channel " + channel.channel + " is now starting to process the next auth with " + nextAuth + "!")
      channel.write(Getnonce(nextAuth.db).maker(requestIdGenerator.getNonce))
      channel.copy(state = Authenticating(nextAuth.db, nextAuth.user, nextAuth.password, None))
    } else { log("AUTH: nothing to do. authenticationHistory is " + authenticationHistory); channel.copy(state = Useable) }
  }

  override def receive = {
    // todo: refactoring
    case request: Request => receiveRequest(request)
    case requestMaker :RequestMaker => receiveRequest(requestMaker(requestIdGenerator.user))
    case (request: Request, writeConcern: Request) => receiveCheckedRequest(request, writeConcern)
    case (requestMaker: RequestMaker, writeConcernMaker: RequestMaker) => {
      val id = requestIdGenerator.user
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
        node.channels.find(_.isConnected).map(_.write(IsMaster().maker(requestIdGenerator.isMaster)))
      }
    }
    case Connected(channelId) => {
      if(seedSet.isDefined) {
        seedSet = seedSet.map(_.updateByChannelId(channelId, node => node.copy(state = CONNECTED)))
        seedSet.map(_.findNodeByChannelId(channelId).get.channels(0).write(IsMaster().maker(requestIdGenerator.isMaster)))
      } else {
        nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(channelId, node => {
          node.copy(channels = node.channels.map { channel =>
            if(channel.getId == channelId) {
              channel.write(IsMaster().maker(requestIdGenerator.isMaster))
              channel.copy(state = Useable)
            } else channel
          }, state = node.state match {
            case _: MongoNodeState => node.state
            case _ => CONNECTED
          })
        }))
      }
      log(channelId + " is connected")
    }
    case Disconnected(channelId) => {
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(channelId, node => {
        node.copy(state = NOT_CONNECTED, channels = node.channels.collect{ case channel if channel.isOpen => channel })
      }))
      log(channelId + " is disconnected")
    }
    case auth @ Authenticate(db, user, password) => {
      if(!authenticationHistory.authenticates.contains(auth)) {
        log("authenticate process starts with " + auth + "...")
        authenticationHistory = AuthHistory(authenticationHistory.authenticateRequests :+ (auth -> List(sender)))
        nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateAll(node =>
          node.copy(channels = node.channels.map(authenticateChannel(_)))
        ))
      } else log("auth not performed as already registered...")
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
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(response.info.channelId,
        _.updateChannelById(response.info.channelId, authenticateChannel(_))))
    }
    // getnonce response
    case response: Response if response.header.responseTo >= 1000 && response.header.responseTo < 2000 => {
      val getnonce = GetnonceResult(response)
      log("AUTH: got nonce for channel " + response.info.channelId + ": " + getnonce.nonce)
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(response.info.channelId, node =>
        node.copy(channels = node.channels.map { channel =>
          if(channel.getId == response.info.channelId) {
            val authenticating = channel.state.asInstanceOf[Authenticating]
            log("AUTH: authentifying with " + authenticating)
            channel.write(AuthenticateCommand(authenticating.db, authenticating.user, authenticating.password, getnonce.nonce).maker(requestIdGenerator.authenticate))
            channel.copy(state = authenticating.copy(nonce = Some(getnonce.nonce)))
          } else channel
        })
      ))
    }
    // authenticate response
    case response: Response if response.header.responseTo >= 2000 && response.header.responseTo < 3000 => {
      log("AUTH: got authentified response! " + response.info.channelId)
      nodeSetManager = NodeSetManager(nodeSetManager.nodeSet.updateByChannelId(response.info.channelId, { node =>
        log("AUTH: updating node " + node + "...")
        node.updateChannelById(response.info.channelId, { channel =>
          val authenticating = channel.state.asInstanceOf[Authenticating]
          log("AUTH: channel " + channel.channel + " is authenticated with " + authenticating + "!")
          authenticationHistory = authenticationHistory.succeeded(a => a.db == authenticating.db && a.user == authenticating.user)
          authenticateChannel(channel.copy(loggedIn = channel.loggedIn + LoggedIn(authenticating.db, authenticating.user)), true)
        })
      }
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
            val lastError = LastError(response)
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
      log("YYYYY:::: " + nodeSetManager.nodeSet)
    }
    case a @ _ => log("not supported " + a)
  }
  
  // monitor -->
  var seedSet :Option[NodeSet] = Some(NodeSet(None, None, seeds.map(seed => createNeededChannels(Node(seed), 1)).toIndexedSeq))
  seedSet.get.connectAll
  
  var nodeSetManager = NodeSetManager(NodeSet(None, None, Vector.empty))
    
  def createNeededChannels(nodeSet: NodeSet) :NodeSet = {
    nodeSet.copy(nodes = nodeSet.nodes.foldLeft(Vector.empty[Node]) { (nodes, node) =>
      nodes :+ createNeededChannels(node)
    })
  }
  
  def createNeededChannels(node: Node, limit: Int = 3) :Node = {
    if(node.channels.size < limit) {
      node.copy(channels = node.channels.++(for(i <- 0 until (limit - node.channels.size)) yield
          MongoChannel(ChannelFactory.create(node.host, node.port, self), NotConnected, Set.empty)))
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

trait MongoError extends Throwable {
  val code: Option[Int]
  val message: Option[String]
  override def getMessage :String = "MongoError[code=" + code.getOrElse("None") + " => message: " + message.getOrElse("None") + "]"
}

case class DefaultMongoError(
  message: Option[String],
  code: Option[Int]
) extends MongoError

private[actors] case class AuthHistory(
  authenticateRequests: List[(Authenticate, List[ActorRef])]
) {
  lazy val authenticates : List[Authenticate] = authenticateRequests.map(_._1)

  lazy val expectingAuthenticationCompletion = authenticateRequests.filter(!_._2.isEmpty)

  def failed(selector: (Authenticate) => Boolean) :AuthHistory = AuthHistory(authenticateRequests.filterNot { request =>
    if(selector(request._1)) {
      request._2.foreach(_ ! Failure(new RuntimeException("authentication failed")))
      true
    } else false
  })

  def succeeded(selector: (Authenticate) => Boolean) :AuthHistory = AuthHistory(authenticateRequests.map { request =>
    if(selector(request._1)) {
      request._2.foreach(_ ! true)
      request._1 -> Nil
    } else request
  })
}

private[actors] case class AwaitingResponse(
  requestID: Int,
  actor: ActorRef,
  isGetLastError: Boolean
)