package org.asyncmongo.actors

import akka.actor._
import akka.actor.Status.Failure
import akka.util.duration._
import org.asyncmongo.bson._
import org.asyncmongo.nodeset._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.ChannelState._
import org.asyncmongo.protocol.commands.{Authenticate => AuthenticateCommand, _}
import org.asyncmongo.protocol.NodeState._
import org.asyncmongo.handlers.DefaultBSONHandlers
import org.jboss.netty.channel.group._
import org.slf4j.{Logger, LoggerFactory}

// messages
/**
 * Authenticate message.
 *
 * @param db The name of the target database
 * @param user The username
 * @param password The password
 */
case class Authenticate(db: String, user: String, password: String)
/**
 * Message to close all active connections.
 * The MongoDBSystem actor must be used after this message has been sent.
 */
case object Close
private[asyncmongo] case object ConnectAll
private[asyncmongo] case object RefreshAllNodes
private[asyncmongo] case class Connected(channelId: Int)
private[asyncmongo] case class Disconnected(channelId: Int)

/**
 * Main actor that processes the requests.
 *
 * @param seeds nodes that will be probed to discover the whole replica set (or one standalone node)
 * @param auth list of authenticate messages - all the nodes will be authenticated as soon they are connected.
 */
class MongoDBSystem(seeds: List[String] = List("localhost:27017"), auth :List[Authenticate]) extends Actor {
  import MongoDBSystem._

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
    logger.trace("received a request")
    if(request.op.expectsResponse) {
      awaitingResponses += request.requestID -> AwaitingResponse(request.requestID, sender, false)
      logger.trace("registering awaiting response for requestID " + request.requestID + ", awaitingResponses: " + awaitingResponses)
    } else logger.trace("NOT registering awaiting response for requestID " + request.requestID)
    if(secondaryOK(request)) {
      val server = pickNode(request)
      logger.debug("node " + server + "will send the query " + request.op)
      server.map(_.send(request))
    } else if(!nodeSetManager.get.primaryWrapper.isDefined) {
      logger.info("delaying send because primary is not available")
      queuedMessages += (sender -> Right(request))
    } else {
      nodeSetManager.get.primaryWrapper.get.send(request)
    }
  }

  def receiveCheckedWriteRequest(checkedWriteRequest: CheckedWriteRequest) :Unit = {
    logger.trace("received a checked write request")
    val requestId = requestIdGenerator.user
    val (request, writeConcern) = {
      val tuple = checkedWriteRequest()
      tuple._1(requestId) -> tuple._2(requestId)
    }
    awaitingResponses += requestId -> AwaitingResponse(requestId, sender, true)
    logger.trace("registering writeConcern-awaiting response for requestID " + requestId + ", awaitingResponses: " + awaitingResponses)
    if(secondaryOK(request)) {
      pickNode(request).map(_.send(request, writeConcern))
    } else if(!nodeSetManager.get.primaryWrapper.isDefined) {
      logger.info("delaying send because primary is not available")
      queuedMessages += (sender -> Left(request -> writeConcern))
      logger.debug("now queuedMessages is " + queuedMessages)
    } else {
      nodeSetManager.get.primaryWrapper.get.send(request, writeConcern)
    }
  }

  def authenticateChannel(channel: MongoChannel, continuing: Boolean = false) :MongoChannel = channel.state match {
    case _: Authenticating if !continuing => {logger.debug("AUTH: delaying auth on " + channel);channel}
    case _ => if(channel.loggedIn.size < authenticationHistory.authenticates.size) {
      val nextAuth = authenticationHistory.authenticates(channel.loggedIn.size)
      logger.debug("channel " + channel.channel + " is now starting to process the next auth with " + nextAuth + "!")
      channel.write(Getnonce(nextAuth.db).maker(requestIdGenerator.getNonce))
      channel.copy(state = Authenticating(nextAuth.db, nextAuth.user, nextAuth.password, None))
    } else { logger.debug("AUTH: nothing to do. authenticationHistory is " + authenticationHistory); channel.copy(state = Useable) }
  }

  override def receive = {
    case Close => {
      if(nodeSetManager.isDefined)
        nodeSetManager.get.nodeSet.makeChannelGroup.close.addListener(new ChannelGroupFutureListener {
          override def operationComplete(future: ChannelGroupFuture) :Unit = {
            nodeSetManager = None
            sender ! "done"
          }
        })
      else sender ! "done"
    }
    case message if !nodeSetManager.isDefined => {
      logger.error("dropping message " + message + " as this system is closed")
    }
    // todo: refactoring
    case request: Request => receiveRequest(request)
    case requestMaker :RequestMaker => receiveRequest(requestMaker(requestIdGenerator.user))
    case checkedWriteRequest :CheckedWriteRequest => receiveCheckedWriteRequest(checkedWriteRequest)

    // monitor
    case ConnectAll => {
      logger.debug("ConnectAll Job running...")
      updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.createNeededChannels(self, 1)))
      nodeSetManager.get.nodeSet.connectAll
    }
    case RefreshAllNodes => {
      val state = "setName=" + nodeSetManager.get.nodeSet.name + " with nodes={" +
      (for(node <- nodeSetManager.get.nodeSet.nodes) yield "['" + node.name + "' in state " + node.state + " with " + node.channels.foldLeft(0) { (count, channel) =>
        if(channel.isConnected) count + 1 else count
      } + " connected channels]") + "}";
      logger.debug("RefreshAllNodes Job running... current state is: " + state)
      nodeSetManager.get.nodeSet.nodes.foreach { node =>
        logger.trace("try to refresh " + node.name)
        node.channels.find(_.isConnected).map(_.write(IsMaster().maker(requestIdGenerator.isMaster)))
      }
    }
    case Connected(channelId) => {
      if(seedSet.isDefined) {
        seedSet = seedSet.map(_.updateByChannelId(channelId, node => node.copy(state = CONNECTED)))
        seedSet.map(_.findNodeByChannelId(channelId).get.channels(0).write(IsMaster().maker(requestIdGenerator.isMaster)))
      } else {
        updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateByChannelId(channelId, node => {
          node.copy(channels = node.channels.map { channel =>
            if(channel.getId == channelId) {
              channel.write(IsMaster().maker(requestIdGenerator.isMaster))
              channel.copy(state = Useable)
            } else channel
          }, state = node.state match {
            case _: MongoNodeState => node.state
            case _ => CONNECTED
          })
        })))
      }
      logger.trace(channelId + " is connected")
    }
    case Disconnected(channelId) => {
      updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateByChannelId(channelId, node => {
        node.copy(state = NOT_CONNECTED, channels = node.channels.collect{ case channel if channel.isOpen => channel })
      })))
      logger.debug(channelId + " is disconnected")
    }
    case auth @ Authenticate(db, user, password) => {
      if(!authenticationHistory.authenticates.contains(auth)) {
        logger.debug("authenticate process starts with " + auth + "...")
        authenticationHistory = AuthHistory(authenticationHistory.authenticateRequests :+ (auth -> List(sender)))
        updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateAll(node =>
          node.copy(channels = node.channels.map(authenticateChannel(_)))
        )))
      } else logger.debug("auth not performed as already registered...")
    }
    // isMaster response
    case response: Response if response.header.responseTo < 1000 => {
      val isMaster = IsMaster.ResultMaker(response)
      updateNodeSetManager(if(isMaster.hosts.isDefined) {// then it's a ReplicaSet
        val mynodes = isMaster.hosts.get.map(name => Node(name, if(isMaster.me.exists(_ == name)) isMaster.state else NONE))
        NodeSetManager(nodeSetManager.get.nodeSet.addNodes(mynodes).copy(name = isMaster.setName).createNeededChannels(self, 1))
      } else if(nodeSetManager.get.nodeSet.nodes.length > 0) {
        logger.debug("single node, update..." + nodeSetManager)
        NodeSetManager(NodeSet(None, None, nodeSetManager.get.nodeSet.nodes.slice(0, 1).map(_.copy(state = isMaster.state))).createNeededChannels(self, 1))
      } else if(seedSet.isDefined && seedSet.get.findNodeByChannelId(response.info.channelId).isDefined) {
        logger.debug("single node, creation..." + nodeSetManager)
        NodeSetManager(NodeSet(None, None, Vector(Node(seedSet.get.findNodeByChannelId(response.info.channelId).get.name))).createNeededChannels(self, 1))
      } else throw new RuntimeException("single node discovery failure..."))
      logger.debug("NodeSetManager is now " + nodeSetManager)
      nodeSetManager.get.nodeSet.connectAll
      if(seedSet.isDefined) {
        seedSet.get.closeAll
        seedSet = None
        logger.debug("Init is done")
      }
      updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateByChannelId(response.info.channelId,
        _.updateChannelById(response.info.channelId, authenticateChannel(_)))))
    }
    // getnonce response
    case response: Response if response.header.responseTo >= 1000 && response.header.responseTo < 2000 => {
      val getnonce = Getnonce.ResultMaker(response)
      logger.debug("AUTH: got nonce for channel " + response.info.channelId + ": " + getnonce.nonce)
      updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateByChannelId(response.info.channelId, node =>
        node.copy(channels = node.channels.map { channel =>
          if(channel.getId == response.info.channelId) {
            val authenticating = channel.state.asInstanceOf[Authenticating]
            logger.debug("AUTH: authenticating with " + authenticating)
            channel.write(AuthenticateCommand(authenticating.user, authenticating.password, getnonce.nonce)(authenticating.db).maker(requestIdGenerator.authenticate))
            channel.copy(state = authenticating.copy(nonce = Some(getnonce.nonce)))
          } else channel
        })
      )))
    }
    // authenticate response
    case response: Response if response.header.responseTo >= 2000 && response.header.responseTo < 3000 => {
      logger.debug("AUTH: got authenticated response! " + response.info.channelId)
      updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateByChannelId(response.info.channelId, { node =>
        logger.debug("AUTH: updating node " + node + "...")
        node.updateChannelById(response.info.channelId, { channel =>
          authenticationHistory = authenticationHistory
          val authenticating = channel.state.asInstanceOf[Authenticating]
          logger.debug("AUTH: got auth response from channel " + channel.channel + " for auth=" + authenticating + "!")
          val (success, history) = authenticationHistory.handleResponse(authenticating, response)
          authenticationHistory = history;
          if(success)
            authenticateChannel(channel.copy(loggedIn = channel.loggedIn + LoggedIn(authenticating.db, authenticating.user)), true)
          else {
            logger.warn("AUTH: failed !!!");
            authenticateChannel(channel, true)
          }
        })
      })
      ))
    }

    // any other response
    case response: Response if response.header.responseTo >= 3000 => {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _sender, isGetLastError)) => {
          logger.debug("Got a response from " + response.info.channelId + "! Will give back message="+response + " to sender " + _sender)
          awaitingResponses -= response.header.responseTo
          if(isGetLastError) {
            logger.debug("{" + response.header.responseTo + "} it's a getlasterror")
            // todo, for now rewinding buffer at original index
            val ridx = response.documents.readerIndex
            val lastError = LastError(response)
            if(lastError.code.isDefined && isNotPrimaryErrorCode(lastError.code.get)) {
              logger.debug("{" + response.header.responseTo + "} sending a failure...")
              self ! RefreshAllNodes
              updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateAll(node => if(node.state == PRIMARY) node.copy(state = UNKNOWN) else node)))
              _sender ! Failure(DefaultMongoError(lastError.message, lastError.code))
            } else {
              response.documents.readerIndex(ridx)
              _sender ! response
            }
          } else {
            logger.trace("{" + response.header.responseTo + "} sending a success!")
            _sender ! response
          }
        }
        case None => {
          logger.error("oups. " + response.header.responseTo + " not found! complete message is " + response)
        }
      }
    }
    case a @ _ => logger.error("not supported " + a)
  }

  // monitor -->
  var seedSet :Option[NodeSet] = Some(NodeSet(None, None, seeds.map(seed => Node(seed).createNeededChannels(self, 1)).toIndexedSeq))
  seedSet.get.connectAll

  var nodeSetManager :Option[NodeSetManager] = Some(NodeSetManager(NodeSet(None, None, Vector.empty)))
  // <-- monitor

  def isNotPrimaryErrorCode(code: Int) = code match {
    case 10054 | 10056 | 10058 | 10107 | 13435 | 13436 => true
    case _ => false
  }

  def updateNodeSetManager(nodeSetManager: NodeSetManager) :NodeSetManager = {
    this.nodeSetManager = Some(nodeSetManager)
    if(nodeSetManager.primaryWrapper.isDefined)
      foundPrimary()
    nodeSetManager
  }

  def foundPrimary() {
    if(!queuedMessages.isEmpty) {
      logger.info("ok, found the primary node, sending all the queued messages... ")
      queuedMessages.dequeueAll( _ => true).foreach {
        case (originalSender, Left( (message, writeConcern) )) => {
          nodeSetManager.map(_.primaryWrapper.get.send(message, writeConcern))
        }
        case (originalSender, Right(message)) => nodeSetManager.map(_.primaryWrapper.get.send(message))
      }
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
      nodeSetManager.flatMap(_.getNodeWrapperByChannelId(channelId))
    }).orElse(nodeSetManager.flatMap(_.pick))
  }

  override def postStop() {
    if(nodeSetManager.isDefined)
      nodeSetManager.get.nodeSet.makeChannelGroup.close
  }
}

object MongoDBSystem {
  private[asyncmongo] val DefaultConnectionRetryInterval :Int = 2000 // milliseconds
  private val logger = LoggerFactory.getLogger("MongoDBSystem")
}

/**
 * A mongo error
 */
trait MongoError extends Throwable {
  /** error code */
  val code: Option[Int]
  /** explanation message */
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

  def failed(selector: (Authenticate) => Boolean, err: FailedAuthentication) :AuthHistory = AuthHistory(authenticateRequests.filterNot { request =>
    if(selector(request._1)) {
      request._2.foreach(_ ! Failure(err))
      true
    } else false
  })

  def succeeded(selector: (Authenticate) => Boolean, auth: SuccessfulAuthentication) :AuthHistory = AuthHistory(authenticateRequests.map { request =>
    if(selector(request._1)) {
      request._2.foreach(_ ! auth)
      request._1 -> Nil
    } else request
  })

  def handleResponse(authenticating: Authenticating, response: Response) :(Boolean, AuthHistory) = {
    AuthenticateCommand(response) match {
      case auth @ SuccessfulAuthentication(db, user, _) => true -> succeeded(a => a.db == db && a.user == user, auth)
      case err: FailedAuthentication => false -> failed(a => a.db == authenticating.db && a.user == authenticating.user, err)
    }
  }
}

private[actors] case class AwaitingResponse(
  requestID: Int,
  actor: ActorRef,
  isGetLastError: Boolean
)