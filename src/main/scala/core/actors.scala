package reactivemongo.core.actors

import akka.actor._
import akka.actor.Status.Failure
import akka.util.duration._
import org.jboss.netty.channel.group._
import org.slf4j.{Logger, LoggerFactory}
import reactivemongo.bson._
import reactivemongo.bson.handlers.DefaultBSONHandlers
import reactivemongo.core.errors._
import reactivemongo.core.nodeset._
import reactivemongo.core.protocol._
import reactivemongo.core.protocol.ChannelState._
import reactivemongo.core.protocol.NodeState._
import reactivemongo.utils.LazyLogger
import reactivemongo.core.commands.{Authenticate => AuthenticateCommand, _}
import scala.concurrent.{Future, Promise}

// messages

/**
 * A message expecting a response from database.
 * It holds a promise that will be completed by the MongoDBSystem actor.
 * The future can be used to get the error or the successful response.
 */
trait ExpectingResponse {
  private[reactivemongo] val promise: Promise[Response] = Promise()
  /** The future response of this request. */
  val future: Future[Response] = promise.future
}

/**
 * A request expecting a response.
 *
 * @param requestMaker The request maker.
 */
case class RequestMakerExpectingResponse(
  requestMaker: RequestMaker
) extends ExpectingResponse

/**
 * A checked write request expecting a response.
 *
 * @param checkedWriteRequest The request maker.
 */
case class CheckedWriteRequestExpectingResponse(
  checkedWriteRequest: CheckedWriteRequest
) extends ExpectingResponse

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
 * The MongoDBSystem actor must not be used after this message has been sent.
 */
case object Close
/**
 * Message to send in order to get warned the next time a primary is found.
 */
private[reactivemongo] case object ConnectAll
private[reactivemongo] case object RefreshAllNodes
private[reactivemongo] case class Connected(channelId: Int)
private[reactivemongo] case class Disconnected(channelId: Int)

/** Message sent when the primary has been discovered. */
case object PrimaryAvailable
/** Message sent when the primary has been lost. */
case object PrimaryUnavailable
// TODO
case object SetAvailable
// TODO
case object SetUnavailable
/** Register a monitor. */
case object RegisterMonitor
case object Closed

/**
 * Main actor that processes the requests.
 *
 * @param seeds nodes that will be probed to discover the whole replica set (or one standalone node)
 * @param auth list of authenticate messages - all the nodes will be authenticated as soon they are connected.
 * @param nbChannelsPerNode number of open channels by node
 */
class MongoDBSystem(
  seeds: List[String],
  auth :List[Authenticate],
  nbChannelsPerNode :Int,
  channelFactory: ChannelFactory = new ChannelFactory()) extends Actor {
  import MongoDBSystem._

  private implicit val cFactory = channelFactory

  val requestIdGenerator = new {
    // all requestIds [0, 1000[ are for isMaster messages
    val isMasterRequestIdIterator :Iterator[Int] = Iterator.iterate(0)(i => if(i == 999) 0 else i + 1)
    def isMaster = isMasterRequestIdIterator.next

    // all requestIds [1000, 2000[ are for getnonce messages
    val getNonceRequestIdIterator :Iterator[Int] = Iterator.iterate(1000)(i => if(i == 1999) 1000 else i + 1)
    def getNonce = getNonceRequestIdIterator.next

    // all requestIds [2000, 3000[ are for authenticate messages
    val authenticateRequestIdIterator :Iterator[Int] = Iterator.iterate(2000)(i => if(i == 2999) 2000 else i + 1)
    def authenticate = authenticateRequestIdIterator.next

    // all requestIds [3000[ are for user messages
    val userIterator :Iterator[Int] = Iterator.iterate(3000)(i => if(i == Int.MaxValue - 1) 3000 else i + 1)
    def user :Int = userIterator.next
  }

  private var authenticationHistory :AuthHistory = AuthHistory(for(a <- auth) yield a -> Nil)

  private val awaitingResponses = scala.collection.mutable.ListMap[Int, AwaitingResponse]()

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()

  private val connectAllJob = context.system.scheduler.schedule(MongoDBSystem.DefaultConnectionRetryInterval milliseconds,
    MongoDBSystem.DefaultConnectionRetryInterval milliseconds,
    self,
    ConnectAll)
  // for tests only
  private val refreshAllJob = context.system.scheduler.schedule(MongoDBSystem.DefaultConnectionRetryInterval*5 milliseconds,
    MongoDBSystem.DefaultConnectionRetryInterval*5 milliseconds,
    self,
    RefreshAllNodes)

  def authenticateChannel(channel: MongoChannel, continuing: Boolean = false) :MongoChannel = channel.state match {
    case _: Authenticating if !continuing => {logger.debug("AUTH: delaying auth on " + channel);channel}
    case _ => if(channel.loggedIn.size < authenticationHistory.authenticates.size) {
      val nextAuth = authenticationHistory.authenticates(channel.loggedIn.size)
      logger.debug("channel " + channel.channel + " is now starting to process the next auth with " + nextAuth + "!")
      channel.write(Getnonce(nextAuth.db).maker(requestIdGenerator.getNonce))
      channel.copy(state = Authenticating(nextAuth.db, nextAuth.user, nextAuth.password, None))
    } else { logger.debug("AUTH: nothing to do. authenticationHistory is " + authenticationHistory); channel.copy(state = Ready) }
  }

  override def receive = {
    case RegisterMonitor => monitors += sender

    case req :RequestMakerExpectingResponse =>
      logger.trace("received a request")
      val request = req.requestMaker(requestIdGenerator.user)
      if(request.op.expectsResponse) {
        awaitingResponses += request.requestID -> AwaitingResponse(request.requestID, req.promise, false)
        logger.trace("registering awaiting response for requestID " + request.requestID + ", awaitingResponses: " + awaitingResponses)
      } else logger.trace("NOT registering awaiting response for requestID " + request.requestID)
      if(secondaryOK(request)) {
        val server = pickNode(request)
        logger.debug("node " + server + "will send the query " + request.op)
        server.map(_.send(request))
      } else if(!nodeSetManager.get.primaryWrapper.isDefined) {
        req.promise.failure(PrimaryUnavailableException)
      } else {
        nodeSetManager.get.primaryWrapper.get.send(request)
      }

    case req :CheckedWriteRequestExpectingResponse =>
      logger.trace("received a checked write request")
      val checkedWriteRequest = req.checkedWriteRequest
      val requestId = requestIdGenerator.user
      val (request, writeConcern) = {
        val tuple = checkedWriteRequest()
        tuple._1(requestId) -> tuple._2(requestId)
      }
      awaitingResponses += requestId -> AwaitingResponse(requestId, req.promise, true)
      logger.trace("registering writeConcern-awaiting response for requestID " + requestId + ", awaitingResponses: " + awaitingResponses)
      if(secondaryOK(request)) {
        pickNode(request).map(_.send(request, writeConcern))
      } else if(!nodeSetManager.get.primaryWrapper.isDefined) {
        req.promise.failure(PrimaryUnavailableException)
      } else {
        nodeSetManager.get.primaryWrapper.get.send(request, writeConcern)
      }

    // monitor
    case ConnectAll => {
      logger.debug("ConnectAll Job running...")
      updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.createNeededChannels(self, nbChannelsPerNode)))
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
              channel.copy(state = Ready)
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
        node.copy(state = NOT_CONNECTED, channels = node.channels.filter{ _.isOpen })
      })))
      if(!nodeSetManager.exists(_.primaryWrapper.isDefined))
        broadcastMonitors(PrimaryUnavailable)
      logger.debug(channelId + " is disconnected")
    }
    case auth @ Authenticate(db, user, password) => {
      if(!authenticationHistory.authenticates.contains(auth)) {
        logger.debug("authenticate process starts with " + auth + "...")
        authenticationHistory = AuthHistory(authenticationHistory.authenticateRequests :+ (auth -> List(sender)))
        updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateAll(node =>
          node.copy(channels = node.channels.map(authenticateChannel(_)))
        )))
      } else {
        logger.debug("auth not performed as already registered...")
        sender ! SuccessfulAuthentication(db, user, false) // TODO refactor auth
      }
    }
    // isMaster response
    case response: Response if response.header.responseTo < 1000 => {
      val isMaster = IsMaster.ResultMaker(response)
      updateNodeSetManager(if(isMaster.hosts.isDefined) {// then it's a ReplicaSet
        val mynodes = isMaster.hosts.get.map(name => Node(name, if(isMaster.me.exists(_ == name)) isMaster.state else NONE))
        NodeSetManager(nodeSetManager.get.nodeSet.addNodes(mynodes).copy(name = isMaster.setName).createNeededChannels(self, nbChannelsPerNode))
      } else if(nodeSetManager.get.nodeSet.nodes.length > 0) {
        logger.debug("single node, update..." + nodeSetManager)
        NodeSetManager(NodeSet(None, None, nodeSetManager.get.nodeSet.nodes.slice(0, 1).map(_.copy(state = isMaster.state))).createNeededChannels(self, nbChannelsPerNode))
      } else if(seedSet.isDefined && seedSet.get.findNodeByChannelId(response.info.channelId).isDefined) {
        logger.debug("single node, creation..." + nodeSetManager)
        NodeSetManager(NodeSet(None, None, Vector(Node(seedSet.get.findNodeByChannelId(response.info.channelId).get.name))).createNeededChannels(self, nbChannelsPerNode))
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
        case Some(AwaitingResponse(_, promise, isGetLastError)) => {
          logger.debug("Got a response from " + response.info.channelId + "! Will give back message="+response + " to promise " + promise)
          awaitingResponses -= response.header.responseTo
          if(response.error.isDefined) {
            logger.debug("{" + response.header.responseTo + "} sending a failure... (" + response.error.get + ")")
            if(response.error.get.isNotAPrimaryError)
              onPrimaryUnavailable()
              promise.failure(response.error.get)
          } else if(isGetLastError) {
            logger.debug("{" + response.header.responseTo + "} it's a getlasterror")
            // todo, for now rewinding buffer at original index
            val ridx = response.documents.readerIndex
            val lastError = LastError(response)
            if(lastError.isNotAPrimaryError) {
              onPrimaryUnavailable()
              promise.failure(lastError)
            } else {
              response.documents.readerIndex(ridx)
              promise.success(response)
            }
          } else {
            logger.trace("{" + response.header.responseTo + "} sending a success!")
            promise.success(response)
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

  def onPrimaryUnavailable() {
    self ! RefreshAllNodes
    updateNodeSetManager(NodeSetManager(nodeSetManager.get.nodeSet.updateAll(node => if(node.state == PRIMARY) node.copy(state = UNKNOWN) else node)))
    broadcastMonitors(PrimaryUnavailable)
  }

  def updateNodeSetManager(nodeSetManager: NodeSetManager) :NodeSetManager = {
    this.nodeSetManager = Some(nodeSetManager)
    if(nodeSetManager.primaryWrapper.isDefined)
      broadcastMonitors(PrimaryAvailable)
    nodeSetManager
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
    import org.jboss.netty.channel.group.{ChannelGroupFuture, ChannelGroupFutureListener}

    if(nodeSetManager.isDefined) {
      nodeSetManager.get.nodeSet.makeChannelGroup.close.addListener(new ChannelGroupFutureListener {
        def operationComplete(future: ChannelGroupFuture) :Unit = {
          logger.debug("all channels are closed.")
          channelFactory.channelFactory.releaseExternalResources
          broadcastMonitors(Closed)
        }
      })
    }

    if(seedSet.isDefined) {
      seedSet.get.makeChannelGroup.close.addListener(new ChannelGroupFutureListener {
        def operationComplete(future: ChannelGroupFuture) :Unit = {
          logger.debug("(seeds) all channels are closed.")
          channelFactory.channelFactory.releaseExternalResources
          broadcastMonitors(Closed)
        }
      })
    }
    logger.debug("MongoDBSystem stopped.")
  }

  def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)
}

object MongoDBSystem {
  private[reactivemongo] val DefaultConnectionRetryInterval :Int = 2000 // milliseconds
  private val logger = LazyLogger(LoggerFactory.getLogger("MongoDBSystem"))
}

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
  promise: Promise[Response],
  isGetLastError: Boolean
)

/** A message to send to a MonitorActor to be warned when a primary has been discovered. */
case object WaitForPrimary

/**
 * A monitor for MongoDBSystem actors.
 *
 * This monitor will be sent node state change events (like PrimaryAvailable, PrimaryUnavailable, etc.).
 * See WaitForPrimary message.
 */
class MonitorActor(sys: ActorRef) extends Actor {
  import MonitorActor._
  import scala.collection.mutable.Queue

  sys ! RegisterMonitor

  private val waitingForPrimary = Queue[ActorRef]()
  var primaryAvailable = false

  private val waitingForClose = Queue[ActorRef]()
  var killed = false

  override def receive = {
    case PrimaryAvailable =>
      logger.debug("set: a primary is available")
      primaryAvailable = true
      waitingForPrimary.dequeueAll(_ => true).foreach(_ ! PrimaryAvailable)
    case PrimaryUnavailable =>
      logger.debug("set: no primary available")
      primaryAvailable = false
    case WaitForPrimary =>
      if(killed)
        sender ! Failure(new RuntimeException("MongoDBSystem actor shutting down or no longer active"))
      else if(primaryAvailable) {
        logger.debug(sender + " is waiting for a primary... available right now, go!")
        sender ! PrimaryAvailable
      } else {
        logger.debug(sender + " is waiting for a primary...  not available, warning as soon a primary is available.")
        waitingForPrimary += sender
      }
    case Close =>
      killed = true
      sys ! PoisonPill
      waitingForClose += sender
      waitingForPrimary.dequeueAll(_ => true).foreach(_ ! Failure(new RuntimeException("MongoDBSystem actor shutting down or no longer active")))
    case Closed =>
      waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
      self ! PoisonPill
  }

  override def postStop {
    logger.debug("Monitor actor stopped.")
  }
}

object MonitorActor {
  private val logger = LazyLogger(LoggerFactory.getLogger("MonitorActor"))
}

// exceptions
/** An exception to be thrown when a request needs a non available primary. */
object PrimaryUnavailableException extends RuntimeException {
  override def getMessage() = "No primary node is available!"
}