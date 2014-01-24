/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.core.actors

import akka.actor._
import org.jboss.netty.channel.group._
import reactivemongo.core.errors._
import reactivemongo.core.protocol._
import reactivemongo.utils.LazyLogger
import reactivemongo.core.commands.{ Authenticate => AuthenticateCommand, _ }
import scala.annotation.tailrec
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }
import reactivemongo.core.nodeset._
import java.net.InetSocketAddress
import reactivemongo.api.ReadPreference

// messages

/**
 * A message expecting a response from database.
 * It holds a promise that will be completed by the MongoDBSystem actor.
 * The future can be used to get the error or the successful response.
 */
sealed trait ExpectingResponse {
  private[actors] val promise: Promise[Response] = Promise()
  /** The future response of this request. */
  val future: Future[Response] = promise.future
}

/**
 * A request expecting a response.
 *
 * @param requestMaker The request maker.
 */
case class RequestMakerExpectingResponse(
  requestMaker: RequestMaker) extends ExpectingResponse

/**
 * A checked write request expecting a response.
 *
 * @param checkedWriteRequest The request maker.
 */
case class CheckedWriteRequestExpectingResponse(
  checkedWriteRequest: CheckedWriteRequest) extends ExpectingResponse

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
private[reactivemongo] case class ChannelConnected(channelId: Int)
private[reactivemongo] sealed trait ChannelUnavailable { def channelId: Int }
private[reactivemongo] object ChannelUnavailable { def unapply(cu: ChannelUnavailable): Option[Int] = Some(cu.channelId) }
private[reactivemongo] case class ChannelDisconnected(channelId: Int) extends ChannelUnavailable
private[reactivemongo] case class ChannelClosed(channelId: Int) extends ChannelUnavailable

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
/** MongoDBSystem has been shut down. */
case object Closed

/**
 * Main actor that processes the requests.
 *
 * @param seeds nodes that will be probed to discover the whole replica set (or one standalone node)
 * @param auth list of authenticate messages - all the nodes will be authenticated as soon they are connected.
 * @param nbChannelsPerNode number of open channels by node
 */
class MongoDBSystem(
    seeds: Seq[String],
    initialAuthenticates: Seq[Authenticate],
    nbChannelsPerNode: Int,
    channelFactory: ChannelFactory = new ChannelFactory()) extends Actor {
  import MongoDBSystem._

  private implicit val cFactory = channelFactory

  import scala.concurrent.duration._

  val requestIds = new RequestIds

  private val awaitingResponses = scala.collection.mutable.LinkedHashMap[Int, AwaitingResponse]()

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()
  implicit val ec = context.system.dispatcher
  private val connectAllJob = context.system.scheduler.schedule(MongoDBSystem.DefaultConnectionRetryInterval milliseconds,
    MongoDBSystem.DefaultConnectionRetryInterval milliseconds,
    self,
    ConnectAll)
  // for tests only
  private val refreshAllJob = context.system.scheduler.schedule(MongoDBSystem.DefaultConnectionRetryInterval * 5 milliseconds,
    MongoDBSystem.DefaultConnectionRetryInterval * 5 milliseconds,
    self,
    RefreshAllNodes)

  @tailrec
  final def authenticateConnection(connection: Connection, auths: Seq[Authenticate]): Connection = {
    if (connection.authenticating.isEmpty && !auths.isEmpty) {
      val nextAuth = auths.head
      if (connection.isAuthenticated(nextAuth.db, nextAuth.user))
        authenticateConnection(connection, auths.tail)
      else {
        connection.send(Getnonce(nextAuth.db).maker(requestIds.getNonce.next))
        connection.copy(authenticating = Some(Authenticating(nextAuth.db, nextAuth.user, nextAuth.password, None)))
      }
    } else connection
  }

  final def authenticateNode(node: Node, auths: Seq[Authenticate]): Node = {
    node.copy(connections = node.connections.map {
      case connection if connection.status == ConnectionStatus.Connected => authenticateConnection(connection, auths)
      case connection => connection
    })
  }

  final def authenticateNodeSet(nodeSet: NodeSet): NodeSet = {
    nodeSet.copy(nodes = nodeSet.nodes.map {
      case node @ Node(_, _: QueryableNodeStatus, _, _, _, _) => authenticateNode(node, nodeSet.authenticates.toSeq)
      case node => node
    })
  }

  def updateNodeSetOnDisconnect(channelId: Int): NodeSet =
    updateNodeSet(nodeSet.updateNodeByChannelId(channelId) { node =>
      val connections = node.connections.map { connection =>
        if (connection.channel.getId() == channelId)
          connection.copy(status = ConnectionStatus.Disconnected)
        else connection
      }

      node.copy(
        status = NodeStatus.Unknown,
        connections = connections,
        authenticated = if (connections.isEmpty) Set.empty else node.authenticated)
    })

  val closing: Receive = {
    case req: RequestMaker =>
      logger.error(s"Received a non-expecting response request during closing process: $req")

    case RegisterMonitor => monitors += sender

    case req: ExpectingResponse =>
      logger.debug(s"Received an expecting response request during closing process: $req, completing its promise with a failure")
      req.promise.failure(Exceptions.ClosedException)

    case msg @ ChannelClosed(channelId) =>
      updateNodeSet(nodeSet.updateNodeByChannelId(channelId) { node =>
        val connections = node.connections.filter { connection =>
          connection.channel.getId() != channelId
        }
        node.copy(
          status = NodeStatus.Unknown,
          connections = connections,
          authenticated = if (connections.isEmpty) Set.empty else node.authenticated)
      })
      val remainingConnections = nodeSet.nodes.foldLeft(0) { (open, node) =>
        open + node.connections.size
      }
      if(logger.logger.isDebugEnabled()) {
        val disconnected = nodeSet.nodes.foldLeft(0) { (open, node) =>
          open + node.connections.count(_.status == ConnectionStatus.Disconnected)
        }
        logger.debug(s"(State: Closing) Received $msg, remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
      }
      if(remainingConnections == 0) {
        monitors foreach (_ ! Closed)
        logger.info(s"MongoDBSystem $self is stopping.")
        context.stop(self)
      }

    case msg @ ChannelDisconnected(channelId) =>
      updateNodeSetOnDisconnect(channelId)
      if(logger.logger.isDebugEnabled()) {
        val remainingConnections = nodeSet.nodes.foldLeft(0) { (open, node) =>
          open + node.connections.size
        }
        val disconnected = nodeSet.nodes.foldLeft(0) { (open, node) =>
          open + node.connections.count(_.status == ConnectionStatus.Disconnected)
        }
        logger.debug(s"(State: Closing) Received $msg, remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
      }

    case other =>
      logger.error(s"(State: Closing) UNHANDLED MESSAGE: $other")
  }

  override def receive = {
    case RegisterMonitor => monitors += sender

    case Close =>
      logger.info("Received Close message, going to close connections and moving on stage Closing")

      // cancel all jobs
      connectAllJob.cancel
      refreshAllJob.cancel

      // close all connections
      val listener = new ChannelGroupFutureListener {
        val factory = channelFactory
        val monitorActors = monitors
        def operationComplete(future: ChannelGroupFuture): Unit = {
          logger.debug("Netty says all channels are closed.")
          factory.channelFactory.releaseExternalResources
        }
      }
      allChannelGroup(nodeSet).close.addListener(listener)

      // fail all requests waiting for a response
      awaitingResponses.foreach( _._2.promise.failure(Exceptions.ClosedException) )
      awaitingResponses.empty

      // moving to closing state
      context become closing

    case req: RequestMaker =>

      logger.debug("WARNING received a request")
      val r = req(requestIds.common.next)
      pickChannel(r).map(_._2.send(r))

    case req: RequestMakerExpectingResponse =>
      logger.debug("received a request expecting a response")
      val request = req.requestMaker(requestIds.common.next)
      pickChannel(request) match {
        case Failure(error) =>
          logger.debug("NO CHANNEL, error with promise " + req.promise)
          req.promise.failure(error)
        case Success((node, connection)) =>
          logger.debug("Sending request expecting response " + request + " by connection " + connection + " of node " + node.name)
          if (request.op.expectsResponse) {
            awaitingResponses += request.requestID -> AwaitingResponse(request.requestID, connection.channel.getId(), req.promise, false)
            logger.trace("registering awaiting response for requestID " + request.requestID + ", awaitingResponses: " + awaitingResponses)
          } else logger.trace("NOT registering awaiting response for requestID " + request.requestID)
          connection.send(request)
      }

    case req: CheckedWriteRequestExpectingResponse =>
      logger.debug("received a checked write request")
      val checkedWriteRequest = req.checkedWriteRequest
      val requestId = requestIds.common.next
      val (request, writeConcern) = {
        val tuple = checkedWriteRequest()
        tuple._1(requestId) -> tuple._2(requestId)
      }
      pickChannel(request) match {
        case Failure(error) => req.promise.failure(error)
        case Success((node, connection)) =>
          logger.debug("Sending request expecting response " + request + " by connection " + connection + " of node " + node.name)
          awaitingResponses += requestId -> AwaitingResponse(requestId, connection.channel.getId(), req.promise, true)
          logger.trace("registering writeConcern-awaiting response for requestID " + requestId + ", awaitingResponses: " + awaitingResponses)
          connection.send(request, writeConcern)
      }

    // monitor
    case ConnectAll => {
      updateNodeSet(nodeSet.createNeededChannels(self, nbChannelsPerNode))
      logger.debug("ConnectAll Job running... Status: " + nodeSet.nodes.map(_.toShortString).mkString(" | "))
      connectAll(nodeSet)
    }
    case RefreshAllNodes => {
      nodeSet.nodes.foreach { node =>
        logger.trace("try to refresh " + node.name)
        updateNodeSet(nodeSet.updateAll { node =>
          sendIsMaster(node, requestIds.isMaster.next)
        })
      }
      logger.debug("RefreshAllNodes Job running... Status: " + nodeSet.toShortString)
    }
    case ChannelConnected(channelId) => {
      updateNodeSet(nodeSet.updateByChannelId(channelId) { connection =>
        connection.copy(status = ConnectionStatus.Connected)
      } { node =>
        sendIsMaster(node, requestIds.isMaster.next)
      })
      logger.trace(s"Channel #$channelId connected. NodeSet status: ${nodeSet.toShortString}")
    }
    case channelUnavailable @ ChannelUnavailable(channelId) => {
      logger.debug(s"Channel #$channelId unavailable ($channelUnavailable).")
      val nodeSetWasReachable = nodeSet.isReachable
      val primaryWasAvailable = nodeSet.primary.isDefined
      channelUnavailable match {
        case _: ChannelClosed =>
          updateNodeSet(nodeSet.updateNodeByChannelId(channelId) { node =>
            val connections = node.connections.filter { connection =>
              connection.channel.getId() != channelId
            }
            node.copy(
              status = NodeStatus.Unknown,
              connections = connections,
              authenticated = if (connections.isEmpty) Set.empty else node.authenticated)
          })
        case _: ChannelDisconnected =>
          updateNodeSetOnDisconnect(channelId)
      }
      awaitingResponses.retain { (_, awaitingResponse) =>
        if (awaitingResponse.channelID == channelId) {
          logger.debug("completing promise " + awaitingResponse.promise + " with error='socket disconnected'")
          awaitingResponse.promise.failure(GenericDriverException("socket disconnected"))
          false
        } else true
      }
      if (!nodeSet.isReachable) {
        if(nodeSetWasReachable) {
          logger.error("The entire node set is unreachable, is there a network problem?")
          broadcastMonitors(SetUnavailable)
        }
        else logger.debug("The entire node set is still unreachable, is there a network problem?")
      } else if (!nodeSet.primary.isDefined) {
        if(primaryWasAvailable) {
          logger.error("The primary is unavailable, is there a network problem?")
          broadcastMonitors(PrimaryUnavailable)
        }
        else logger.debug("The primary is still unavailable, is there a network problem?")
      }
      logger.debug(channelId + " is disconnected")
    }
    // isMaster response
    case response: Response if requestIds.isMaster accepts response =>
      val nodeSetWasReachable = nodeSet.isReachable
      val primaryWasAvailable = nodeSet.primary.isDefined
      IsMaster.ResultMaker(response).fold(
        e => {
          logger.error(s"error while processing isMaster response #${response.header.responseTo}", e)
        },
        isMaster => {
          val ns = nodeSet.updateNodeByChannelId(response.info.channelId) { node =>
            val pingInfo =
              if (node.pingInfo.lastIsMasterId == response.header.responseTo)
                node.pingInfo.copy(ping = System.currentTimeMillis() - node.pingInfo.lastIsMasterTime, lastIsMasterTime = 0, lastIsMasterId = -1)
              else node.pingInfo
            val authenticating = isMaster.status match {
              case _: QueryableNodeStatus => authenticateNode(node, nodeSet.authenticates.toSeq)
              case _                      => node
            }
            authenticating.copy(status = isMaster.status, pingInfo = pingInfo, name = isMaster.me.getOrElse(node.name), tags = isMaster.tags)
          }
          updateNodeSet {
            connectAll {
              (if (isMaster.hosts.isDefined) {
                ns.copy(nodes = ns.nodes ++ isMaster.hosts.get.collect {
                  case host if !ns.nodes.exists(_.name == host) => Node(host, NodeStatus.Uninitialized, Vector.empty, Set.empty, None)
                })
              } else {
                ns
              }).createNeededChannels(self, nbChannelsPerNode)
            }
          }
        })
      if(!nodeSetWasReachable && nodeSet.isReachable) {
        broadcastMonitors(SetAvailable)
        logger.info("The node set is now available")
      }
      if(!primaryWasAvailable && nodeSet.primary.isDefined) {
        broadcastMonitors(PrimaryAvailable)
        logger.info("The primary is now available")
      }

    case request @ AuthRequest(authenticate, _) => {
      // TODO warn auth ok
      AuthRequestsManager.addAuthRequest(request)
      updateNodeSet(authenticateNodeSet(nodeSet.copy(authenticates = nodeSet.authenticates + authenticate)))
    }
    // getnonce response
    case response: Response if requestIds.getNonce accepts response =>
      Getnonce.ResultMaker(response).fold(
        e =>
          logger.error(s"error while processing getNonce response #${response.header.responseTo}", e),
        nonce => {
          logger.debug("AUTH: got nonce for channel " + response.info.channelId + ": " + nonce)
          updateNodeSet(nodeSet.updateConnectionByChannelId(response.info.channelId) { connection =>
            connection.authenticating match {
              case Some(authenticating) =>
                connection.send(AuthenticateCommand(authenticating.user, authenticating.password, nonce)(authenticating.db).maker(requestIds.authenticate.next))
                connection.copy(authenticating = Some(authenticating.copy(nonce = Some(nonce))))
              case _ => connection
            }
          })
        })

    // authenticate response
    case response: Response if requestIds.authenticate accepts response => {
      logger.debug("AUTH: got authenticated response! " + response.info.channelId)
      val auth = nodeSet.pickByChannelId(response.info.channelId).flatMap(_._2.authenticating)
      updateNodeSet(auth match {
        case Some(authenticating) =>
          val originalAuthenticate = Authenticate(authenticating.db, authenticating.user, authenticating.password)
          val authenticated = AuthenticateCommand(response) match {
            case Right(successfulAuthentication) =>
              AuthRequestsManager.handleAuthResult(originalAuthenticate, successfulAuthentication)
              Some(Authenticated(authenticating.db, authenticating.user))
            case Left(error) =>
              AuthRequestsManager.handleAuthResult(originalAuthenticate, error)
              None
          }
          val ns = nodeSet.updateByChannelId(response.info.channelId) { connection =>
            authenticateConnection(connection.copy(
              authenticated = authenticated.map(connection.authenticated + _).getOrElse(connection.authenticated),
              authenticating = None), nodeSet.authenticates.toSeq)
          } { node =>
            node.copy(authenticated = authenticated.map(node.authenticated + _).getOrElse(node.authenticated))
          }
          if (!authenticated.isDefined)
            ns.copy(authenticates = ns.authenticates.-(originalAuthenticate))
          else ns
        case _ => nodeSet
      })
    }

    // any other response
    case response: Response if requestIds.common accepts response => {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _, promise, isGetLastError)) => {
          logger.debug("Got a response from " + response.info.channelId + "! Will give back message=" + response + " to promise " + promise)
          awaitingResponses -= response.header.responseTo
          if (response.error.isDefined) {
            logger.debug("{" + response.header.responseTo + "} sending a failure... (" + response.error.get + ")")
            if (response.error.get.isNotAPrimaryError)
              onPrimaryUnavailable()
            promise.failure(response.error.get)
          } else if (isGetLastError) {
            logger.debug("{" + response.header.responseTo + "} it's a getlasterror")
            // todo, for now rewinding buffer at original index
            val ridx = response.documents.readerIndex
            LastError(response).fold(
              e => {
                logger.error(s"Error deserializing LastError message #${response.header.responseTo}", e)
                promise.failure(new RuntimeException(s"Error deserializing LastError message #${response.header.responseTo}", e))
              },
              lastError => {
                if (lastError.inError) {
                  logger.debug("{" + response.header.responseTo + "} sending a failure (lasterror is not ok)")
                  if (lastError.isNotAPrimaryError)
                    onPrimaryUnavailable()
                  promise.failure(lastError)
                } else {
                  logger.trace("{" + response.header.responseTo + "} sending a success (lasterror is ok)")
                  response.documents.readerIndex(ridx)
                  promise.success(response)
                }
              })
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
  var nodeSet: NodeSet = NodeSet(None, None, seeds.map(seed => Node(seed, NodeStatus.Unknown, Vector.empty, Set.empty, None).createNeededChannels(self, 1)).toVector, initialAuthenticates.toSet)
  connectAll(nodeSet)
  // <-- monitor

  def onPrimaryUnavailable() {
    self ! RefreshAllNodes
    updateNodeSet(nodeSet.updateAll(node => if (node.status == NodeStatus.Primary) node.copy(status = NodeStatus.Unknown) else node))
    broadcastMonitors(PrimaryUnavailable)
  }

  def updateNodeSet(nodeSet: NodeSet): NodeSet = {
    this.nodeSet = nodeSet
    nodeSet
  }

  def secondaryOK(message: Request) = !message.op.requiresPrimary && (message.op match {
    case query: Query   => (query.flags & QueryFlags.SlaveOk) != 0
    case _: KillCursors => true
    case _: GetMore     => true
    case _              => false
  })

  def pickChannel(request: Request): Try[(Node, Connection)] = {
    if (request.channelIdHint.isDefined)
      nodeSet.pickByChannelId(request.channelIdHint.get).map(Success(_)).getOrElse(Failure(Exceptions.ChannelNotFound))
    else nodeSet.pick(request.readPreference).map(Success(_)).getOrElse(Failure(Exceptions.PrimaryUnavailableException))
  }

  override def postStop() {
    // COPY OF CLOSING CODE FROM LINE 218

    // cancel all jobs
    connectAllJob.cancel
    refreshAllJob.cancel

    // close all connections
    val listener = new ChannelGroupFutureListener {
      val factory = channelFactory
      val monitorActors = monitors
      def operationComplete(future: ChannelGroupFuture): Unit = {
        logger.debug("Netty says all channels are closed.")
        factory.channelFactory.releaseExternalResources
      }
    }
    allChannelGroup(nodeSet).close.addListener(listener)

    // fail all requests waiting for a response
    awaitingResponses.foreach( _._2.promise.failure(Exceptions.ClosedException) )
    awaitingResponses.empty

    logger.warn(s"MongoDBSystem $self stopped.")
  }

  def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)

  def connectAll(nodeSet: NodeSet) = {
    for {
      node <- nodeSet.nodes
      connection <- node.connections if !connection.channel.isConnected()
    } yield connection.channel.connect(new InetSocketAddress(node.host, node.port))
    nodeSet
  }

  def sendIsMaster(node: Node, id: Int) = {
    node.connected.headOption.map { channel =>
      channel.send(IsMaster().maker(id))
      if (node.pingInfo.lastIsMasterId == -1) {
        node.copy(pingInfo = node.pingInfo.copy(lastIsMasterTime = System.currentTimeMillis(), lastIsMasterId = id))
      } else if (node.pingInfo.lastIsMasterId >= PingInfo.pingTimeout) {
        node.copy(pingInfo = node.pingInfo.copy(lastIsMasterTime = System.currentTimeMillis(), lastIsMasterId = id, ping = Long.MaxValue))
      } else {
        node
      }
    }.getOrElse {
      node
    }
  }

  def allChannelGroup(nodeSet: NodeSet) = {
    val result = new DefaultChannelGroup
    for (node <- nodeSet.nodes) {
      for (connection <- node.connections)
        result.add(connection.channel)
    }
    result
  }

  // Auth Methods
  object AuthRequestsManager {
    private var authRequests: Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = Map.empty
    def addAuthRequest(request: AuthRequest): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      val found = authRequests.get(request.authenticate)
      authRequests = authRequests + (request.authenticate -> (request.promise :: found.getOrElse(Nil)))
      authRequests
    }
    def handleAuthResult(authenticate: Authenticate, result: SuccessfulAuthentication): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      val found = authRequests.get(authenticate)
      if (found.isDefined) {
        found.get.foreach { _.success(result) }
        authRequests = authRequests.-(authenticate)
      }
      authRequests
    }
    def handleAuthResult(authenticate: Authenticate, result: Throwable): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      val found = authRequests.get(authenticate)
      if (found.isDefined) {
        found.get.foreach { _.failure(result) }
        authRequests = authRequests - authenticate
      }
      authRequests
    }
  }

}

case class AuthRequest(authenticate: Authenticate, promise: Promise[SuccessfulAuthentication] = Promise()) {
  def future: Future[SuccessfulAuthentication] = promise.future
}

object MongoDBSystem {
  private[actors] val DefaultConnectionRetryInterval: Int = 2000 // milliseconds
  private val logger = LazyLogger("reactivemongo.core.actors.MongoDBSystem")
}

private[actors] case class AwaitingResponse(
  requestID: Int,
  channelID: Int,
  promise: Promise[Response],
  isGetLastError: Boolean)

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
      if (killed)
        sender ! Failure(new RuntimeException("MongoDBSystem actor shutting down or no longer active"))
      else if (primaryAvailable) {
        logger.debug(sender + " is waiting for a primary... available right now, go!")
        sender ! PrimaryAvailable
      } else {
        logger.debug(sender + " is waiting for a primary...  not available, warning as soon a primary is available.")
        waitingForPrimary += sender
      }
    case Close =>
      logger.debug("Monitor received Close")
      killed = true
      sys ! Close
      waitingForClose += sender
      waitingForPrimary.dequeueAll(_ => true).foreach(_ ! Failure(new RuntimeException("MongoDBSystem actor shutting down or no longer active")))
    case Closed =>
      logger.debug(s"Monitor $self closed, stopping...")
      waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
      context.stop(self)
  }

  override def postStop {
    // COPY OF CLOSING CODE FROM LINE 656
    // I'm not sure this will actually be useful because the close message we send to sys
    // will probably not be received if we are stopping because the Actorsystem is killed.
    // I can't hurt though...
    killed = true
    sys ! Close
    waitingForClose += sender
    waitingForPrimary.dequeueAll(_ => true).foreach(_ ! Failure(new RuntimeException("MongoDBSystem actor shutting down or no longer active")))

    // EXECUTE "CLOSED" CODE too?

    logger.debug(s"Monitor $self stopped.")
  }
}

object MonitorActor {
  private val logger = LazyLogger("reactivemongo.core.actors.MonitorActor")
}

// exceptions
object Exceptions {
  /** An exception thrown when a request needs a non available primary. */
  object PrimaryUnavailableException extends DriverException {
    val message = "No primary node is available!"
  }
  /** An exception thrown when the entire node set is unavailable. The application may not have access to the network anymore. */
  object NodeSetNotReachable extends DriverException {
    val message = "The node set can not be reached! Please check your network connectivity."
  }
  object ChannelNotFound extends DriverException {
    val message = "ChannelNotFound"
  }
  object ClosedException extends DriverException {
    val message = "This MongoConnection is closed"
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
  private val iterator = Iterator.iterate(lower)(i => if (i == upper) lower else i + 1)

  def next = iterator.next
  def accepts(id: Int): Boolean = id >= lower && id <= upper
  def accepts(response: Response): Boolean = accepts(response.header.responseTo)
}
