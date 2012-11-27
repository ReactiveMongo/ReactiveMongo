package reactivemongo.core.actors

import akka.actor._
import akka.actor.Status.Failure
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

  import scala.concurrent.duration._
  
  val requestIds = new RequestIds

  private var authenticationHistory :AuthHistory = AuthHistory(for(a <- auth) yield a -> Nil)

  private val awaitingResponses = scala.collection.mutable.ListMap[Int, AwaitingResponse]()

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()
  implicit val ec = context.system.dispatcher
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
      channel.write(Getnonce(nextAuth.db).maker(requestIds.getNonce.next))
      channel.copy(state = Authenticating(nextAuth.db, nextAuth.user, nextAuth.password, None))
    } else { logger.debug("AUTH: nothing to do. authenticationHistory is " + authenticationHistory); channel.copy(state = Ready) }
  }

  override def receive = {
    case RegisterMonitor => monitors += sender

    case req: RequestMaker =>
      
      logger.debug("WARNING received a request")
      val r = req(requestIds.common.next)
      pickChannel(r).right.map(_._2.send(r))

    case req :RequestMakerExpectingResponse =>
      logger.debug("received a request expecting a response")
      val request = req.requestMaker(requestIds.common.next)
      pickChannel(request).fold(
        error => {
          logger.debug("NO CHANNEL, error with promise " + req.promise)
          req.promise.failure(error)
        },
        nodeChannel => {
          logger.debug("Sending request expecting response " + request + " by channel " + nodeChannel)
          if(request.op.expectsResponse) {
            awaitingResponses += request.requestID -> AwaitingResponse(request.requestID, nodeChannel._2.channel.getId(), req.promise, false)
            logger.trace("registering awaiting response for requestID " + request.requestID + ", awaitingResponses: " + awaitingResponses)
          } else logger.trace("NOT registering awaiting response for requestID " + request.requestID)
          nodeChannel._2.send(request)
        })

    case req :CheckedWriteRequestExpectingResponse =>
      logger.debug("received a checked write request")
      val checkedWriteRequest = req.checkedWriteRequest
      val requestId = requestIds.common.next
      val (request, writeConcern) = {
        val tuple = checkedWriteRequest()
        tuple._1(requestId) -> tuple._2(requestId)
      }
      pickChannel(request).fold(
        error => req.promise.failure(error),
        nodeChannel => {
          logger.debug("Sending checked write request " + request + " by channel " + nodeChannel)
          awaitingResponses += requestId -> AwaitingResponse(requestId, nodeChannel._2.channel.getId(), req.promise, true)
          logger.trace("registering writeConcern-awaiting response for requestID " + requestId + ", awaitingResponses: " + awaitingResponses)
          nodeChannel._2.send(request, writeConcern)
        })

    // monitor
    case ConnectAll => {
      logger.debug("ConnectAll Job running...")
      updateNodeSet(nodeSet.createNeededChannels(self, nbChannelsPerNode))
      nodeSet.connectAll
    }
    case RefreshAllNodes => {
      val state = "setName=" + nodeSet.name + " with nodes={" +
      (for(node <- nodeSet.nodes) yield "['" + node.name + "' in state " + node.state + " with " + node.channels.foldLeft(0) { (count, channel) =>
        if(channel.isConnected) count + 1 else count
      } + " connected channels]") + "}";
      logger.debug("RefreshAllNodes Job running... current state is: " + state)
      nodeSet.nodes.foreach { node =>
        logger.trace("try to refresh " + node.name)
        node.channels.find(_.isConnected).map(_.write(IsMaster().maker(requestIds.isMaster.next)))
      }
    }
    case Connected(channelId) => {
      updateNodeSet(nodeSet.updateByChannelId(channelId, node => {
        node.copy(channels = node.channels.map { channel =>
          if(channel.getId == channelId) {
            channel.write(IsMaster().maker(requestIds.isMaster.next))
            channel.copy(state = Ready)
          } else channel
        }, state = node.state match {
          case _: MongoNodeState => node.state
          case _ => CONNECTED
        })
      }))
      logger.trace(channelId + " is connected")
    }
    case Disconnected(channelId) => {
      updateNodeSet(nodeSet.updateByChannelId(channelId, node => {
        node.copy(state = NOT_CONNECTED, channels = node.channels.filter{ _.isOpen })
      }))
      awaitingResponses.retain { (_, awaitingResponse) =>
        if(awaitingResponse.channelID == channelId) {
          logger.debug("completing promise " + awaitingResponse.promise + " with error='socket disconnected'")
          awaitingResponse.promise.failure(GenericMongoError("socket disconnected"))
          false
        } else true
      }
      if(!nodeSet.isReachable) {
        logger.error("The entire node set is unreachable, is there a network problem?")
        broadcastMonitors(PrimaryUnavailable) // TODO
      } else if(!nodeSet.primary.isDefined) {
        logger.warn("The primary is unavailable, is there a network problem?")
        broadcastMonitors(PrimaryUnavailable)
      }
      logger.debug(channelId + " is disconnected")
    }
    case auth @ Authenticate(db, user, password) => {
      if(!authenticationHistory.authenticates.contains(auth)) {
        logger.debug("authenticate process starts with " + auth + "...")
        authenticationHistory = AuthHistory(authenticationHistory.authenticateRequests :+ auth -> List(sender))
        updateNodeSet(nodeSet.updateAll(node =>
          node.copy(channels = node.channels.map(authenticateChannel(_)))
        ))
      } else {
        logger.debug("auth not performed as already registered...")
        sender ! VerboseSuccessfulAuthentication(db, user, false)
      }
    }
    // isMaster response
    case response: Response if requestIds.isMaster accepts response => {
      val isMaster = IsMaster.ResultMaker(response).right.get
      updateNodeSet(if(isMaster.hosts.isDefined) {// then it's a ReplicaSet
        val mynodes = isMaster.hosts.get.map(name => Node(name, if(isMaster.me.exists(_ == name)) isMaster.state else NONE))
        nodeSet.addNodes(mynodes).copy(name = isMaster.setName).createNeededChannels(self, nbChannelsPerNode)
      } else if(nodeSet.nodes.length > 0) {
        logger.debug("single node, update..." + nodeSet)
        NodeSet(None, None, nodeSet.nodes.slice(0, 1).map(_.copy(state = isMaster.state))).createNeededChannels(self, nbChannelsPerNode)
      } else throw new RuntimeException("single node discovery failure..."))
      logger.debug("NodeSet is now " + nodeSet)
      nodeSet.connectAll
      updateNodeSet(nodeSet.updateByChannelId(response.info.channelId,
        _.updateChannelById(response.info.channelId, authenticateChannel(_))))
    }
    // getnonce response
    case response: Response if requestIds.getNonce accepts response => {
      val nonce = Getnonce.ResultMaker(response).right.get
      logger.debug("AUTH: got nonce for channel " + response.info.channelId + ": " + nonce)
      updateNodeSet(nodeSet.updateByChannelId(response.info.channelId, node =>
        node.copy(channels = node.channels.map { channel =>
          if(channel.getId == response.info.channelId) {
            val authenticating = channel.state.asInstanceOf[Authenticating]
            logger.debug("AUTH: authenticating with " + authenticating)
            channel.write(AuthenticateCommand(authenticating.user, authenticating.password, nonce)(authenticating.db).maker(requestIds.authenticate.next))
            channel.copy(state = authenticating.copy(nonce = Some(nonce)))
          } else channel
        })
      ))
    }
    // authenticate response
    case response: Response if requestIds.authenticate accepts response => {
      logger.debug("AUTH: got authenticated response! " + response.info.channelId)
      updateNodeSet(nodeSet.updateByChannelId(response.info.channelId, { node =>
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
      )
    }

    // any other response
    case response: Response if requestIds.common accepts response => {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _, promise, isGetLastError)) => {
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
            val lastError = LastError(response).right.get
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
  var nodeSet :NodeSet = NodeSet(None, None, seeds.map(seed => Node(seed).createNeededChannels(self, 1)).toIndexedSeq)
  nodeSet.connectAll
  // <-- monitor

  def onPrimaryUnavailable() {
    self ! RefreshAllNodes
    updateNodeSet(nodeSet.updateAll(node => if(node.state == PRIMARY) node.copy(state = UNKNOWN) else node))
    broadcastMonitors(PrimaryUnavailable)
  }

  def updateNodeSet(nodeSet: NodeSet) :NodeSet = {
    this.nodeSet = nodeSet
    if(nodeSet.primary.isDefined)
      broadcastMonitors(PrimaryAvailable)
    nodeSet
  }

  def secondaryOK(message: Request) = !message.op.requiresPrimary && (message.op match {
    case query: Query => (query.flags & QueryFlags.SlaveOk) != 0
    case _ :KillCursors => true
    case _ :GetMore => true
    case _ => false
  })

  def pickChannel(request: Request) :Either[ReactiveMongoError, (Node, MongoChannel)] = {
    if(request.channelIdHint.isDefined)
      nodeSet.findByChannelId(request.channelIdHint.get).toRight(Exceptions.ChannelNotFound)
    else if(secondaryOK(request))
      nodeSet.queryable.pick.flatMap(node => node.pick.map(node.node -> _)).toRight(Exceptions.NodeSetNotReachable)
    else nodeSet.queryable.primaryRoundRobiner.flatMap(node => node.pick.map(node.node -> _)).toRight(Exceptions.PrimaryUnavailableException)
  }

  def pickNode(message: Request) :Option[NodeRoundRobiner] = {
    message.channelIdHint.flatMap(channelId => {
      nodeSet.queryable.getNodeRoundRobinerByChannelId(channelId)
    }).orElse(nodeSet.queryable.pick)
  }

  override def postStop() {
    import org.jboss.netty.channel.group.{ChannelGroupFuture, ChannelGroupFutureListener}

    val listener = new ChannelGroupFutureListener {
      val factory = channelFactory
      val monitorActors = monitors
      def operationComplete(future: ChannelGroupFuture) :Unit = {
        logger.debug("all channels are closed.")
        factory.channelFactory.releaseExternalResources
        monitorActors foreach (_ ! Closed)
      }
    }

    nodeSet.makeChannelGroup.close.addListener(listener)

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

  def failed(authenticating: Authenticating, err: Throwable) :AuthHistory = AuthHistory(authenticateRequests.filterNot { request =>
    if(request._1.db == authenticating.db && request._1.user == authenticating.user) {
      request._2.foreach(_ ! Failure(err))
      true
    } else false
  })

  def succeeded(authenticating: Authenticating, auth: SuccessfulAuthentication) :AuthHistory = AuthHistory(authenticateRequests.map { request =>
    if(request._1.db == authenticating.db && request._1.user == authenticating.user) {
      request._2.foreach(_ ! auth)
      request._1 -> Nil
    } else request
  })

  def handleResponse(authenticating: Authenticating, response: Response) :(Boolean, AuthHistory) = {
    AuthenticateCommand(response) match {
      case Right(auth) => true -> succeeded(authenticating, auth)
      case Left(err) => false -> failed(authenticating, err)
    } 
  }
}

private[actors] case class AwaitingResponse(
  requestID: Int,
  channelID: Int,
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
object Exceptions {
  /** An exception thrown when a request needs a non available primary. */
  object PrimaryUnavailableException extends ReactiveMongoError {
    val message = "No primary node is available!"
  }
  /** An exception thrown when the entire node set is unavailable. The application may not have access to the network anymore. */
  object NodeSetNotReachable extends ReactiveMongoError {
    val message = "The node set can not be reached! Please check your network connectivity."
  }
  object ChannelNotFound extends ReactiveMongoError {
    val message = "ChannelNotFound"
  }
}

private[actors] class RequestIds {
  // all requestIds [0, 1000[ are for isMaster messages
  val isMaster = RequestIdGenerator(0, 999)
  // all requestIds [1000, 2000[ are for getnonce messages
  val getNonce = RequestIdGenerator(1000, 1999)
  // all requestIds [2000, 3000[ are for authenticate messages
  val authenticate = RequestIdGenerator(2000, 2999)
  // all requestIds [3000[ are for common messages
  val common = RequestIdGenerator(3000, Int.MaxValue - 1)
}

private[actors] case class RequestIdGenerator(
  lower: Int,
  upper: Int) {
  private val iterator = Iterator.iterate(lower)(i => if(i == upper) lower else i + 1)

  def next = iterator.next
  def accepts(id: Int) :Boolean = id >= lower && id <= upper
  def accepts(response: Response) :Boolean = accepts(response.header.responseTo)
}