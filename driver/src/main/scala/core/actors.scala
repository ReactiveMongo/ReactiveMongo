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

import akka.actor._
import akka.pattern._
import akka.routing.{RoundRobinRoutingLogic, Router}
import org.jboss.netty.channel.group._
import reactivemongo.core._
import reactivemongo.core.errors._
import reactivemongo.core.protocol._
import reactivemongo.core.commands.{ Authenticate => AuthenticateCommand, _ }
import scala.annotation.tailrec
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }
import reactivemongo.core.nodeset._
import java.net.InetSocketAddress
import reactivemongo.api.{MongoConnection, MongoConnectionOptions, ReadPreference}

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

  private var channel = new AtomicInteger(0)

  @volatile
  var primaries : Router = Router(RoundRobinRoutingLogic())
  @volatile
  var secondaries : Router = Router(RoundRobinRoutingLogic())
  @volatile
  var mongos : Router = Router(RoundRobinRoutingLogic())
  @volatile
  var primaryPrefered : Router = Router(RoundRobinRoutingLogic())

  private val awaitingResponses = scala.collection.mutable.LinkedHashMap[Int, AwaitingResponse]()

  private val nodeSetActor = system.actorOf(Props(classOf[NodeSet], addConnection.tupled, removeConnection))

  def nextChannel = channel.incrementAndGet()

  def send(req: RequestMakerExpectingResponse) = primaries.route(req, Actor.noSender)

  def send(req: ExpectingResponse) = primaries.route(req, Actor.noSender)

  def send(req: CheckedWriteRequestExpectingResponse) = primaries.route(req, Actor.noSender)

  def send(req: RequestMaker) = primaries.route(req, Actor.noSender)

  def send(req: AuthRequest) = primaries.route(req, Actor.noSender)

  // todo: fix
  // monitor -->
  //val nodeSet = NodeSet(None, seeds.map(Node(_, initialAuthenticates.toSet, options.nbChannelsPerNode)).toVector)




   //context.actorOf(Props(classOf[NodeSet], None, initialAuthenticates))
//    NodeSet(None, None, seeds.map(seed => Node(seed, NodeStatus.Unknown,
//    Vector.empty, Set.empty, None, ProtocolMetadata.Default).createNeededChannels(connectionHandler, connectionManager, 1)).toVector,
//    initialAuthenticates.toSet)
//  connectAll(nodeSet)
  // <-- monitor

  val requestIds = new RequestIds

  import scala.concurrent.duration._


  private def addConnection : (ActorRef, ConnectionState) => Unit = (connection, state )  => {
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

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()


  def secondaryOK(message: Request) = !message.op.requiresPrimary && (message.op match {
    case query: Query   => (query.flags & QueryFlags.SlaveOk) != 0
    case _: KillCursors => true
    case _: GetMore     => true
    case _              => false
  })


  def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)


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
