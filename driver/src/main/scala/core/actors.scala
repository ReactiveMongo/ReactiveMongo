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

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor.Receive
import akka.actor._
import akka.pattern._
import akka.routing.{RoundRobinRoutingLogic, Router}
import reactivemongo.api.{MongoConnection, MongoConnectionOptions}
import reactivemongo.core._
import reactivemongo.core.actors.ConnectionManager.{Remove, Add}
import reactivemongo.core.commands.{Authenticate => AuthenticateCommand, _}
import reactivemongo.core.errors._
import reactivemongo.core.nodeset._
import reactivemongo.core.protocol._

import scala.concurrent.{Future, Promise}

// messages

/**
 * A message expecting a response from database.
 * It holds a promise that will be completed by the MongoDBSystem actor.
 * The future can be used to get the error or the successful response.
 */
sealed trait ExpectingResponse {
  private[core] val promise: Promise[Response] = Promise()
  /** The future response of this request. */
  val future: Future[Response] = promise.future
}

/**
 * A request expecting a response.
 *
 * @param requestMaker The request maker.
 */
case class RequestMakerExpectingResponse(
  requestMaker: RequestMaker, isMongo26WriteOp: Boolean) extends ExpectingResponse

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
case class PrimaryAvailable(metadata: ProtocolMetadata)
/** Message sent when the primary has been lost. */
case object PrimaryUnavailable
// TODO
case class SetAvailable(metadata: ProtocolMetadata)
// TODO
case object SetUnavailable
/** Register a monitor. */
case object RegisterMonitor
/** MongoDBSystem has been shut down. */
case object Closed
case object GetLastMetadata

/**
 * Main actor that processes the requests.
 *
 * @param seeds nodes that will be probed to discover the whole replica set (or one standalone node).
 * @param initialAuthenticates list of authenticate messages - all the nodes will be authenticated as soon they are connected.
 * @param options MongoConnectionOption instance (used for tweaking connection flags and pool size).
 */
class MongoDBSystem(
    seeds: Seq[String],
    initialAuthenticates: Seq[Authenticate],
    options: MongoConnectionOptions,
    system: ActorSystem) {
  import scala.concurrent.ExecutionContext.Implicits.global

  @volatile
  var channels = Map.empty[Int, ActorRef]
  @volatile
  var primaries : Router = Router(RoundRobinRoutingLogic())
  @volatile
  var secondaries : Router = Router(RoundRobinRoutingLogic())
  @volatile
  var mongos : Router = Router(RoundRobinRoutingLogic())

  val nodeSetActor = system.actorOf(Props(new NodeSet()))

  def send(req: RequestMakerExpectingResponse) = primaries.route(req, Actor.noSender)

  def send(req: ExpectingResponse) = primaries.route(req, Actor.noSender)

  def send(req: CheckedWriteRequestExpectingResponse) = primaries.route(req, Actor.noSender)

  def send(req: RequestMaker) = primaries.route(req, Actor.noSender)

  def send(req: AuthRequest) = primaries.route(req, Actor.noSender)

  import scala.concurrent.duration._


  private def addConnection : (ActorRef, ConnectionState) => Unit = (connection, state )  => {
    channels = channels + ((state.channel, connection))
    state.status match {
      case NodeStatus.Primary => primaries = primaries.addRoutee(connection)
      case NodeStatus.Secondary => secondaries = secondaries.addRoutee(connection)
      case _ =>
    }
  }

  private def removeConnection : ActorRef => Unit = (actor: ActorRef) => {

  }

  def connect() : Future[MongoConnection] = {
    system.log.debug("connecting...")
    (nodeSetActor ? NodeSet.ConnectAll(seeds, initialAuthenticates, options.nbChannelsPerNode))(6.seconds)
      .mapTo[ProtocolMetadata].map(MongoConnection(system, this, options, _))
  }

  def secondaryOK(message: Request) = !message.op.requiresPrimary && (message.op match {
    case query: Query   => (query.flags & QueryFlags.SlaveOk) != 0
    case _: KillCursors => true
    case _: GetMore     => true
    case _              => false
  })

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


  class NodeSet extends Actor with ActorLogging {

    var initialAuthenticates: Seq[Authenticate] = Seq.empty
    var connectionsPerNode: Int = 10
    var existingHosts : Set[String] = Set.empty
    var connectingNodes: Vector[ActorRef] = Vector.empty
    var connectedNodes: Vector[ActorRef] = Vector.empty
    var version: Option[Long] = None
    var replyTo: ActorRef = null

    override def receive: Receive = {
      case NodeSet.ConnectAll(hosts, auth, count) => {
        log.info("Connection to initial nodes")
        replyTo = sender()
        this.connectionsPerNode = count
        this.initialAuthenticates = auth
        existingHosts = hosts.toSet
        connectingNodes = hosts.map(address => {
          val node = context.actorOf(Props(classOf[Node], address, initialAuthenticates, connectionsPerNode))
          node ! Node.Connect
          node
        }).toVector
      }
      case Node.Connected(connections, metadata) => {
        log.info("node connected metadata {}", metadata)
        connectingNodes = connectingNodes diff List(sender())
        connectedNodes = sender() +: connectedNodes
        connections.foreach(p => add(p._1, p._2))

        if(connections.exists(p => p._2.isPrimary || p._2.isMongos))
          replyTo ! metadata
      }
      case Node.DiscoveredNodes(hosts) => {
        log.info("nodes descovered")
        val discovered = hosts.filter(!existingHosts.contains(_))
        existingHosts = discovered ++: existingHosts
        connectingNodes = discovered.map(address => {
          val node = context.actorOf(Props(classOf[Node], address, initialAuthenticates, connectionsPerNode))
          node ! Node.Connect
          node
        }) ++: connectingNodes
      }
      case Close => {
        connectedNodes.foreach(_ ! Close)
        context.become(closing)
      }
    }

    private def closing: Receive = {
      case Node.Connected(connections, metadata) => {
        log.info("node connected metadata {} but nodeSet in closing state", metadata)
        connectingNodes = connectingNodes diff List(sender())
        connectedNodes = sender() +: connectedNodes
        sender() ! Close
      }
      case Closed => {
        connectedNodes = connectedNodes diff List(sender())
        if(connectedNodes.isEmpty && connectingNodes.isEmpty) {
          self ! PoisonPill
          log.debug("all connections to a nodeset are closed")
        }
      }
      case Node.DiscoveredNodes(hosts) => {
        log.info("nodes descovered but nodeSet in closing state")
      }
    }

    private def add(connection: ActorRef, state: ConnectionState) = {
      channels = channels + ((state.channel, connection))
      if(state.isMongos)
        mongos = mongos.addRoutee(connection)
      else if(state.isPrimary)
        primaries = primaries.addRoutee(connection)
      else
        secondaries = secondaries.addRoutee(connection)
    }

    private def remove(conn: ActorRef) = {
        channels = channels.filter(_._2 != conn)
        mongos = mongos.removeRoutee(conn)
        primaries = primaries.removeRoutee(conn)
        secondaries = primaries.removeRoutee(conn)
      }
  }

  object  NodeSet {
    case class ConnectAll(hosts: Seq[String], initialAuthenticates: Seq[Authenticate], connectionsPerNode: Int)
  }
}

object ConnectionManager {
  case class Add(connection: ActorRef, state: ConnectionState)
  case class Remove(connection: ActorRef)
}

case class AuthRequest(authenticate: Authenticate, promise: Promise[SuccessfulAuthentication] = Promise()) {
  def future: Future[SuccessfulAuthentication] = promise.future
}

object MongoDBSystem {
  private[actors] val DefaultConnectionRetryInterval: Int = 2000 // milliseconds
}

private[core] case class AwaitingResponse(
  requestID: Int,
  channelID: Int,
  promise: Promise[Response],
  isGetLastError: Boolean,
  isMongo26WriteOp: Boolean)

/** A message to send to a MonitorActor to be warned when a primary has been discovered. */
case object WaitForPrimary

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

private[core] class RequestIds {
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

class RequestId(min: Int = Int.MinValue, max: Int = Int.MaxValue){
  private var value = min
  def next = {
    val result = value
    value = if(value == max) min else value + 1
    result
  }
}
