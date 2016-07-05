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

import java.net.InetSocketAddress

import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

import akka.actor.{ Actor, ActorRef }
import akka.pattern.ask
import akka.util.Timeout

import shaded.netty.channel.{ ChannelFuture, ChannelFutureListener }
import shaded.netty.channel.group.{
  ChannelGroupFuture,
  ChannelGroupFutureListener,
  DefaultChannelGroup
}

import reactivemongo.util.LazyLogger
import reactivemongo.core.ConnectionListener
import reactivemongo.core.errors.{ DriverException, GenericDriverException }
import reactivemongo.core.protocol.{
  CheckedWriteRequest,
  GetMore,
  Query,
  QueryFlags,
  KillCursors,
  MongoWireVersion,
  Request,
  RequestMaker,
  Response
}
import reactivemongo.core.commands.{
  CommandError,
  SuccessfulAuthentication
}
import reactivemongo.core.nodeset.{
  Authenticate,
  Authenticated,
  Authenticating,
  ChannelFactory,
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeSetInfo,
  NodeStatus,
  PingInfo,
  ProtocolMetadata
}
import reactivemongo.api.{ MongoConnectionOptions, ReadPreference }
import reactivemongo.api.commands.LastError

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

object ExpectingResponse {
  def unapply(that: Any): Option[Promise[Response]] = that match {
    case req @ RequestMakerExpectingResponse(_, _) => Some(req.promise)
    case req @ CheckedWriteRequestExpectingResponse(_) => Some(req.promise)
    case _ => None
  }
}

/**
 * A request expecting a response.
 *
 * @param requestMaker the request maker
 * @param isMongo26WriteOp true if the operation is a MongoDB 2.6 write one
 */
case class RequestMakerExpectingResponse(
  requestMaker: RequestMaker,
  isMongo26WriteOp: Boolean) extends ExpectingResponse

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
private[reactivemongo] case object RefreshAll
private[reactivemongo] case class ChannelConnected(channelId: Int)

private[reactivemongo] sealed trait ChannelUnavailable { def channelId: Int }

private[reactivemongo] object ChannelUnavailable {
  def unapply(cu: ChannelUnavailable): Option[Int] = Some(cu.channelId)
}

private[reactivemongo] case class ChannelDisconnected(
  channelId: Int) extends ChannelUnavailable

private[reactivemongo] case class ChannelClosed(
  channelId: Int) extends ChannelUnavailable

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

/** Main actor that processes the requests. */
@deprecated("Internal class: will be made private", "0.11.14")
trait MongoDBSystem extends Actor {
  import scala.concurrent.duration._
  import reactivemongo.bson.BSONDocument
  import MongoDBSystem._

  /** The name of the driver supervisor. */
  def supervisor: String

  /** The name of the connection pool. */
  def name: String

  /**
   * Nodes that will be probed to discover the whole replica set (or one standalone node).
   */
  def seeds: Seq[String]

  /**
   * List of authenticate messages - all the nodes will be authenticated as soon they are connected.
   */
  def initialAuthenticates: Seq[Authenticate]

  /**
   * MongoConnectionOption instance
   * (used for tweaking connection flags and pool size).
   */
  def options: MongoConnectionOptions

  def channelFactory: ChannelFactory

  private val lnm = s"$supervisor/$name" // log naming

  private val listener: Option[ConnectionListener] = {
    val cl = reactivemongo.core.ConnectionListener()
    cl.foreach(_.poolCreated(options, supervisor, name))
    cl
  }

  private implicit val cFactory = channelFactory

  private val awaitingResponses =
    scala.collection.mutable.LinkedHashMap[Int, AwaitingResponse]()

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()

  implicit val ec = context.system.dispatcher

  private val connectAllJob = {
    val ms = options.monitorRefreshMS / 5
    val interval =
      if (ms < 100) 100 milliseconds
      else options.monitorRefreshMS milliseconds

    context.system.scheduler.schedule(interval, interval, self, ConnectAll)
  }

  // for tests only
  private val refreshAllJob = {
    val interval = options.monitorRefreshMS milliseconds

    context.system.scheduler.schedule(interval, interval, self, RefreshAll)
  }

  protected def sendAuthenticate(connection: Connection, authentication: Authenticate): Connection

  @annotation.tailrec
  protected final def authenticateConnection(connection: Connection, auths: Seq[Authenticate]): Connection = {
    if (!connection.authenticating.isEmpty) connection
    else auths.headOption match {
      case Some(nextAuth) =>
        if (connection.isAuthenticated(nextAuth.db, nextAuth.user)) {
          authenticateConnection(connection, auths.tail)
        } else sendAuthenticate(connection, nextAuth)

      case _ => connection
    }
  }

  private final def authenticateNode(node: Node, auths: Seq[Authenticate]): Node = node._copy(connections = node.connections.map {
    case connection if connection.status == ConnectionStatus.Connected =>
      authenticateConnection(connection, auths)

    case connection => connection
  })

  private final def authenticateNodeSet(nodeSet: NodeSet): NodeSet =
    nodeSet.updateAll {
      case node @ Node(_, status, _, _, _, _, _, _) if status.queryable =>
        authenticateNode(node, nodeSet.authenticates.toSeq)

      case node => node
    }

  private def unauthenticate(node: Node, connections: Vector[Connection]): Node = node._copy(
    status = NodeStatus.Unknown,
    connections = connections,
    authenticated = if (connections.isEmpty) Set.empty else node.authenticated)

  private def stopWhenDisconnected[T](state: String, msg: T): Unit = {
    val remainingConnections = _nodeSet.nodes.foldLeft(0) {
      _ + _.connected.size
    }

    if (logger.isDebugEnabled) {
      val disconnected = _nodeSet.nodes.foldLeft(0) { (open, node) =>
        open + node.connections.count(_.status == ConnectionStatus.Disconnected)
      }

      logger.debug(s"[$lnm$$$state] Received $msg remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
    }

    if (remainingConnections == 0) {
      monitors.foreach(_ ! Closed)
      logger.debug(s"[$lnm] Stopping $self")
      context.stop(self)
    }
  }

  def updateNodeSetOnDisconnect(channelId: Int): NodeSet =
    updateNodeSet(_.updateNodeByChannelId(channelId) { node =>
      val connections = node.connections.map { connection =>
        if (connection.channel.getId() != channelId) connection
        else connection.copy(status = ConnectionStatus.Disconnected)
      }

      unauthenticate(node, connections)
    })

  private def lastError(response: Response): Either[Throwable, LastError] = {
    import reactivemongo.api.commands.bson.
      BSONGetLastErrorImplicits.LastErrorReader

    Response.parse(response).next().asTry[LastError] match {
      case Failure(err) => Left(err)
      case Success(err) => Right(err)
    }
  }

  // TODO: Specific option?
  @inline private def requestRetries = options.failoverStrategy.retries

  private def retry(req: AwaitingResponse): Option[AwaitingResponse] =
    req.retriable(requestRetries).flatMap { newReq =>
      foldNodeConnection(req.request)({ error =>
        req.promise.failure(error)
        None
      }, { (node, con) =>
        val reqId = req.requestID
        val awaiting = newReq(con.channel.getId)

        awaitingResponses += reqId -> awaiting

        awaiting.getWriteConcern.fold(con.send(awaiting.request)) { wc =>
          con.send(awaiting.request, wc)
        }

        Some(awaiting)
      })
    }

  protected def authReceive: Receive

  private val processing: Receive = {
    case RegisterMonitor => monitors += sender

    case Close => {
      logger.debug(s"[$lnm] Received Close message, going to close connections and moving on state Closing")

      val ns = close()
      val connectedCon = ns.nodes.foldLeft(0) { _ + _.connected.size }

      // moving to closing state
      context become closing

      if (connectedCon == 0) stopWhenDisconnected("Processing", Close)
    }

    case req @ RequestMaker(_, _, _, _) => {
      logger.trace(s"[$lnm] Transmiting a request: $req")

      val r = req(RequestId.common.next)
      pickChannel(r).map(_._2.send(r))
    }

    case req @ RequestMakerExpectingResponse(maker, _) => {
      val reqId = RequestId.common.next

      logger.trace(
        s"[$lnm] Received a request expecting a response ($reqId): $req")

      val request = maker(reqId)

      foldNodeConnection(request)(req.promise.failure(_), { (node, con) =>
        if (request.op.expectsResponse) {
          awaitingResponses += reqId -> AwaitingResponse(
            request, con.channel.getId, req.promise,
            isGetLastError = false,
            isMongo26WriteOp = req.isMongo26WriteOp)

          logger.trace(s"[$lnm] Registering awaiting response for requestID $reqId, awaitingResponses: $awaitingResponses")
        } else logger.trace(s"[$lnm] NOT registering awaiting response for requestID $reqId")

        con.send(request)
      })
    }

    case req @ CheckedWriteRequestExpectingResponse(_) => {
      logger.debug(s"[$lnm] Received a checked write request")

      val checkedWriteRequest = req.checkedWriteRequest
      val reqId = RequestId.common.next
      val (request, writeConcern) = {
        val tuple = checkedWriteRequest()
        tuple._1(reqId) -> tuple._2(reqId)
      }

      foldNodeConnection(request)(req.promise.failure(_), { (node, con) =>
        awaitingResponses += reqId -> AwaitingResponse(
          request, con.channel.getId(), req.promise,
          isGetLastError = true, isMongo26WriteOp = false).
          withWriteConcern(writeConcern)

        logger.trace(s"[$lnm] Registering awaiting response for requestID $reqId, awaitingResponses: $awaitingResponses")

        con.send(request, writeConcern)
      })
    }

    case ConnectAll => { // monitor
      updateNodeSet(_.createNeededChannels(self, options.nbChannelsPerNode))

      logger.debug(s"[$lnm] ConnectAll Job running... Status: " + _nodeSet.nodes.map(_.toShortString).mkString(" | "))
      connectAll(_nodeSet)
    }

    case RefreshAll => {
      updateNodeSet(_.updateAll { node =>
        logger.trace(s"[$lnm] Try to refresh ${node.name}")

        sendIsMaster(node, RequestId.isMaster.next)
      })

      logger.debug(
        s"[$lnm] RefreshAll Job running... Status: ${_nodeSet.toShortString}")
    }

    case ChannelConnected(channelId) => {
      updateNodeSet(_.updateByChannelId(channelId)(
        _.copy(status = ConnectionStatus.Connected)) { node =>
          sendIsMaster(node, RequestId.isMaster.next)
        })

      logger.trace(s"[$lnm] Channel #$channelId connected. NodeSet status: ${_nodeSet.toShortString}")
    }

    case channelUnavailable @ ChannelUnavailable(channelId) => {
      logger.trace(
        s"[$lnm] Channel #$channelId is unavailable ($channelUnavailable).")

      val nodeSetWasReachable = _nodeSet.isReachable
      val primaryWasAvailable = _nodeSet.primary.isDefined

      channelUnavailable match {
        case ChannelClosed(_) => updateNodeSet(
          _.updateNodeByChannelId(channelId) { node =>
            unauthenticate(node, node.connections.
              filter(_.channel.getId != channelId))
          })

        case ChannelDisconnected(_) => updateNodeSetOnDisconnect(channelId)
      }

      val retried = Map.newBuilder[Int, AwaitingResponse]

      awaitingResponses.retain { (_, awaitingResponse) =>
        if (awaitingResponse.channelID == channelId) {
          retry(awaitingResponse) match {
            case Some(awaiting) => {
              logger.trace(s"[$lnm] Retrying to await response for requestID ${awaiting.requestID}: $awaiting")
              retried += channelId -> awaiting
            }

            case _ => {
              logger.debug(s"[$lnm] Completing promise ${awaitingResponse.promise} with error='socket disconnected'")

              awaitingResponse.promise.
                failure(GenericDriverException(s"Socket disconnected ($lnm)"))

            }
          }

          false
        } else true
      }

      awaitingResponses ++= retried.result()

      if (!_nodeSet.isReachable) {
        if (nodeSetWasReachable) {
          logger.warn(s"[$lnm] The entire node set is unreachable, is there a network problem?")

          broadcastMonitors(SetUnavailable)
        } else logger.debug(s"[$lnm] The entire node set is still unreachable, is there a network problem?")
      } else if (!_nodeSet.primary.isDefined) {
        if (primaryWasAvailable) {
          logger.warn(s"[$lnm] The primary is unavailable, is there a network problem?")
          broadcastMonitors(PrimaryUnavailable)
        } else logger.debug(
          s"[$lnm] The primary is still unavailable, is there a network problem?")
      }

      logger.debug(s"[$lnm] Channel #$channelId is released")
    }

    case response @ Response(_, _, _, _) if ( // isMaster response
      RequestId.isMaster accepts response) => {

      logger.trace(s"[$lnm] IsMaster response: $response")

      import reactivemongo.api.BSONSerializationPack
      import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits
      import reactivemongo.api.commands.Command

      val isMaster = Command.deserialize(BSONSerializationPack, response)(
        BSONIsMasterCommandImplicits.IsMasterResultReader)

      updateNodeSet { nodeSet =>
        val nodeSetWasReachable = nodeSet.isReachable
        val wasPrimary = nodeSet.primary.toSeq.flatMap(_.names)
        @volatile var chanNode = Option.empty[Node]

        val prepared =
          nodeSet.updateNodeByChannelId(response.info.channelId) { node =>
            val pingInfo =
              if (node.pingInfo.lastIsMasterId == response.header.responseTo) {
                node.pingInfo.copy(ping =
                  System.currentTimeMillis() - node.pingInfo.lastIsMasterTime,
                  lastIsMasterTime = 0, lastIsMasterId = -1)

              } else node.pingInfo

            val authenticating =
              if (!isMaster.status.queryable || nodeSet.authenticates.isEmpty) {
                node
              } else authenticateNode(node, nodeSet.authenticates.toSeq)

            val meta = ProtocolMetadata(
              MongoWireVersion(isMaster.minWireVersion),
              MongoWireVersion(isMaster.maxWireVersion),
              isMaster.maxBsonObjectSize,
              isMaster.maxMessageSizeBytes,
              isMaster.maxWriteBatchSize)

            val an = authenticating._copy(
              status = isMaster.status,
              pingInfo = pingInfo,
              tags = isMaster.replicaSet.flatMap(_.tags),
              protocolMetadata = meta,
              isMongos = isMaster.isMongos)

            val n = isMaster.replicaSet.fold(an)(rs => an.withAlias(rs.me))
            chanNode = Some(n)

            n
          }

        val discoveredNodes = isMaster.replicaSet.toSeq.flatMap { rs =>
          rs.hosts.collect {
            case host if (!prepared.nodes.exists(_.names contains host)) =>
              // Prepare node for newly discovered host in the RS
              Node(host, NodeStatus.Uninitialized,
                Vector.empty, Set.empty, None, ProtocolMetadata.Default)
          }
        }

        logger.trace(s"[$lnm] Discovered nodes: $discoveredNodes")

        val upSet = prepared.copy(nodes = prepared.nodes ++ discoveredNodes)

        chanNode.foreach { node =>
          if (!upSet.authenticates.isEmpty && node.authenticated.isEmpty) {
            logger.debug(s"[$lnm] The node set is available (${node.names}); Waiting authentication: ${node.authenticated}")
          } else {
            if (!nodeSetWasReachable && upSet.isReachable) {
              broadcastMonitors(SetAvailable(upSet.protocolMetadata))
              logger.debug(s"[$lnm] The node set is now available")
            }

            if (upSet.primary.exists(n => !wasPrimary.contains(n.name))) {
              broadcastMonitors(PrimaryAvailable(upSet.protocolMetadata))
              logger.debug(s"[$lnm] The primary is now available: ${node.names}")
            }
          }
        }

        upSet
      }

      context.system.scheduler.scheduleOnce(Duration.Zero) {
        updateNodeSet { ns =>
          connectAll(ns.createNeededChannels(self, options.nbChannelsPerNode))
        }
      }
    }
  }

  val closing: Receive = {
    case req @ RequestMaker(_, _, _, _) =>
      logger.error(s"[$lnm] Received a non-expecting response request during closing process: $req")

    case RegisterMonitor => monitors += sender

    case req @ ExpectingResponse(promise) => {
      logger.debug(s"[$lnm] Received an expecting response request during closing process: $req, completing its promise with a failure")
      promise.failure(new Exceptions.ClosedException(supervisor, name))
    }

    case msg @ ChannelClosed(channelId) => {
      updateNodeSet(_.updateNodeByChannelId(channelId) { node =>
        unauthenticate(node, node.connections.
          filter(_.channel.getId != channelId))
      })

      stopWhenDisconnected("Closing", msg)
    }

    case msg @ ChannelDisconnected(channelId) => {
      updateNodeSetOnDisconnect(channelId)

      if (logger.isDebugEnabled) {
        val remainingConnections = _nodeSet.nodes.foldLeft(0) { (open, node) =>
          open + node.connections.size
        }
        val disconnected = _nodeSet.nodes.foldLeft(0) { (open, node) =>
          open + node.connections.count(
            _.status == ConnectionStatus.Disconnected)
        }

        logger.debug(s"[$lnm$$Closing] Received $msg, remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
      }
    }

    case msg @ ChannelConnected(channelId) =>
      logger.warn(s"[$lnm$$Closing] SPURIOUS $msg (ignored, channel closed)")
      updateNodeSetOnDisconnect(channelId)

    case Close =>
      logger.warn(s"[$lnm$$Closing] Already closing ... ignore Close message")

    case other =>
      logger.error(s"[$lnm$$Closing] Unhandled message: $other")
  }

  // any other response
  private val fallback: Receive = {
    case response: Response if RequestId.common accepts response => {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _, promise, isGetLastError, isMongo26WriteOp)) => {
          logger.trace(s"[$lnm] Got a response from ${response.info.channelId} to ${response.header.responseTo}! Will give back message=$response to promise ${System.identityHashCode(promise)}")

          awaitingResponses -= response.header.responseTo

          if (response.error.isDefined) {
            logger.debug(s"[$lnm] {${response.header.responseTo}} sending a failure... (${response.error.get})")

            if (response.error.get.isNotAPrimaryError) onPrimaryUnavailable()
            promise.failure(response.error.get)
          } else if (isGetLastError) {
            logger.debug(s"[$lnm] {${response.header.responseTo}} it's a getlasterror")

            // todo, for now rewinding buffer at original index
            import reactivemongo.api.commands.bson.BSONGetLastErrorImplicits.LastErrorReader

            lastError(response).fold(e => {
              logger.error(s"[$lnm] Error deserializing LastError message #${response.header.responseTo}", e)
              promise.failure(new RuntimeException(s"Error deserializing LastError message #${response.header.responseTo} ($lnm)", e))
            }, { lastError =>
              if (lastError.inError) {
                logger.trace(s"[$lnm] {${response.header.responseTo}} sending a failure (lasterror is not ok)")
                if (lastError.isNotAPrimaryError) onPrimaryUnavailable()
                promise.failure(lastError)
              } else {
                logger.trace(s"[$lnm] {${response.header.responseTo}} sending a success (lasterror is ok)")
                response.documents.readerIndex(response.documents.readerIndex)
                promise.success(response)
              }
            })
          } else if (isMongo26WriteOp) {
            // TODO - logs, bson
            // MongoDB 26 Write Protocol errors
            logger.trace(
              s"[$lnm] Received a response to a MongoDB2.6 Write Op")

            import reactivemongo.bson.lowlevel._
            import reactivemongo.core.netty.ChannelBufferReadableBuffer
            val reader = new LowLevelBsonDocReader(new ChannelBufferReadableBuffer(response.documents))
            val fields = reader.fieldStream
            val okField = fields.find(_.name == "ok")

            logger.trace(s"[$lnm] {${response.header.responseTo}} ok field is: $okField")
            val processedOk = okField.collect {
              case BooleanField(_, v) => v
              case IntField(_, v)     => v != 0
              case DoubleField(_, v)  => v != 0
            }.getOrElse(false)

            if (processedOk) {
              logger.trace(s"[$lnm] {${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
              promise.success(response)
            } else {
              logger.debug(s"[$lnm] {${response.header.responseTo}} [MongoDB26 Write Op response] processedOk is false! sending an error")

              val notAPrimary = fields.find(_.name == "errmsg").exists {
                case errmsg @ LazyField(0x02, _, buf) =>
                  logger.debug(s"[$lnm] {${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg!")
                  buf.readString == "not a primary"
                case errmsg =>
                  logger.debug(s"[$lnm] {${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg but not interesting!")
                  false
              }

              if (notAPrimary) {
                logger.debug(s"[$lnm] {${response.header.responseTo}} [MongoDB26 Write Op response] not a primary error!")
                onPrimaryUnavailable()
              }

              promise.failure(GenericDriverException("not ok"))
            }
          } else {
            logger.trace(s"[$lnm] {${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
            promise.success(response)
          }
        }

        case _ => logger.error(s"[$lnm] Oups. ${response.header.responseTo} not found! complete message is $response")
      }
    }

    case request @ AuthRequest(authenticate, _) => {
      logger.debug(s"[$lnm] New authenticate request $authenticate")

      AuthRequestsManager.addAuthRequest(request)

      updateNodeSet { ns =>
        authenticateNodeSet(ns.
          copy(authenticates = ns.authenticates + authenticate))
      }
    }

    case a => logger.error(s"[$lnm] Not supported: $a")
  }

  override lazy val receive: Receive =
    processing.orElse(authReceive).orElse(fallback)

  private type NodeSetHandler = (NodeSetInfo, NodeSet) => Unit
  private val nodeSetUpdated: NodeSetHandler =
    listener.fold[NodeSetHandler]((_, _) => {}) { l =>
      { (previous: NodeSetInfo, updated: NodeSet) =>
        context.system.scheduler.scheduleOnce(1.second) {
          _setInfo = updated.info
          l.nodeSetUpdated(previous, _setInfo)
        }
      }
    }

  // monitor -->
  private val nodeSetLock = new Object {}
  private var _nodeSet: NodeSet = NodeSet(None, None, seeds.map(seed => Node(seed, NodeStatus.Unknown, Vector.empty, Set.empty, None, ProtocolMetadata.Default).createNeededChannels(self, 1)).toVector, initialAuthenticates.toSet)
  private var _setInfo: NodeSetInfo = _nodeSet.info

  connectAll(_nodeSet, { (node, chan) =>
    chan.addListener(new ChannelFutureListener {
      def operationComplete(chan: ChannelFuture) =
        sendIsMaster(node, RequestId.isMaster.next)
    })

    chan
  })

  nodeSetUpdated(null, _nodeSet)
  // <-- monitor

  def onPrimaryUnavailable() {
    self ! RefreshAll

    updateNodeSet(_.updateAll { node =>
      if (node.status != NodeStatus.Primary) node
      else node._copy(status = NodeStatus.Unknown)
    })

    broadcastMonitors(PrimaryUnavailable)
  }

  private def updateNodeSet(f: NodeSet => NodeSet): NodeSet = {
    var previous: NodeSetInfo = null
    var updated: NodeSet = null

    nodeSetLock.synchronized {
      previous = this._setInfo
      updated = f(this._nodeSet)

      this._nodeSet = updated
    }

    nodeSetUpdated(previous, updated)

    updated
  }

  private def updateAuthenticate(nodeSet: NodeSet, channelId: Int, replyTo: Authenticate, auth: Option[Authenticated]): NodeSet = {
    val ns = nodeSet.updateByChannelId(channelId) { con =>
      val authed = auth.map(con.authenticated + _).getOrElse(con.authenticated)

      authenticateConnection(con.copy(
        authenticated = authed, authenticating = None),
        nodeSet.authenticates.toSeq)

    } { node =>
      node._copy(authenticated = auth.map(node.authenticated + _).
        getOrElse(node.authenticated))
    }

    if (!auth.isDefined) ns.copy(authenticates = ns.authenticates - replyTo)
    else ns
  }

  protected def authenticationResponse(response: Response)(check: Response => Either[CommandError, SuccessfulAuthentication]): NodeSet = {
    updateNodeSet { nodeSet =>
      val auth = nodeSet.pickByChannelId(
        response.info.channelId).flatMap(_._2.authenticating)

      auth match {
        case Some(Authenticating(db, user, pass)) => {
          val originalAuthenticate = Authenticate(db, user, pass)
          val authenticated = check(response) match {
            case Right(successfulAuthentication) => {
              AuthRequestsManager.handleAuthResult(
                originalAuthenticate, successfulAuthentication)

              if (nodeSet.isReachable) {
                broadcastMonitors(SetAvailable(nodeSet.protocolMetadata))
                logger.debug(s"[$lnm] The node set is now authenticated")
              }

              if (nodeSet.primary.isDefined) {
                broadcastMonitors(PrimaryAvailable(nodeSet.protocolMetadata))
                logger.debug(s"[$lnm] The primary is now authenticated")
              } else if (nodeSet.isReachable) {
                logger.warn(s"""[$lnm] The node set is authenticated, but the primary is not available: ${nodeSet.name} -> ${nodeSet.nodes.map(_.names) mkString ", "}""")
              }

              Some(Authenticated(db, user))
            }

            case Left(error) => {
              AuthRequestsManager.handleAuthResult(originalAuthenticate, error)
              None
            }
          }

          updateAuthenticate(nodeSet,
            response.info.channelId, originalAuthenticate, authenticated)
        }

        case res =>
          logger.warn(s"[$lnm] Authentication result: $res")
          nodeSet
      }
    }
  }

  private def secondaryOK(message: Request): Boolean =
    !message.op.requiresPrimary && (message.op match {
      case Query(flags, _, _, _) => (flags & QueryFlags.SlaveOk) != 0
      case KillCursors(_)        => true
      case GetMore(_, _, _)      => true
      case _                     => false
    })

  private def pickChannel(request: Request): Try[(Node, Connection)] = {
    if (request.channelIdHint.isDefined) {
      _nodeSet.pickByChannelId(request.channelIdHint.get).map(Success(_)).
        getOrElse(Failure(Exceptions.ChannelNotFound))

    } else _nodeSet.pick(request.readPreference).map(Success(_)).
      getOrElse(Failure(Exceptions.PrimaryUnavailableException))
  }

  private def foldNodeConnection[T](request: Request)(e: Throwable => T, f: (Node, Connection) => T): T = pickChannel(request) match {
    case Failure(error) => {
      logger.trace(s"[$lnm] No channel for request: $request")
      e(error)
    }

    case Success((node, connection)) => {
      logger.trace(s"[$lnm] Sending request (${request.requestID}) expecting response by connection $connection of node ${node.name}: $request")

      f(node, connection)
    }
  }

  private[actors] def whenAuthenticating(channelId: Int)(f: Tuple2[Connection, Authenticating] => Connection): NodeSet = updateNodeSet(
    _.updateConnectionByChannelId(channelId) { connection =>
      connection.authenticating.fold(connection)(authenticating =>
        f(connection -> authenticating))
    })

  private def close(): NodeSet = {
    logger.debug(s"[$lnm] Closing MongoDBSystem")

    // cancel all jobs
    connectAllJob.cancel
    refreshAllJob.cancel

    // close all connections
    val listener = new ChannelGroupFutureListener {
      val factory = channelFactory
      def operationComplete(future: ChannelGroupFuture): Unit = {
        logger.debug(s"[$lnm] Netty says all channels are closed.")

        try {
          factory.channelFactory.releaseExternalResources()
        } catch {
          case err: Throwable =>
            logger.debug(s"[$lnm] Fails to release channel resources", err)
        }
      }
    }

    val ns = updateNodeSet(_.updateAll { node =>
      node._copy(connections = node.connected)
      // Only keep already connected connection:
      // - prevent to activate other connection
      // - know which connections to be closed
    })

    allChannelGroup(ns).close().addListener(listener)

    // fail all requests waiting for a response
    awaitingResponses.foreach {
      case (_, r) if (!r.promise.isCompleted) =>
        // fail all requests waiting for a response
        r.promise.failure(
          new Exceptions.ClosedException(supervisor, name))

      case _ => ( /* already completed */ )
    }

    awaitingResponses.clear()

    ns
  }

  override def postStop() {
    close()

    logger.info(s"[$lnm] MongoDBSystem $self stopped.")

    listener.foreach(_.poolShutdown(supervisor, name))
  }

  private def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)

  private def connectAll(nodeSet: NodeSet, connected: (Node, ChannelFuture) => ChannelFuture = { (_, chan) => chan }): NodeSet = {
    for {
      node <- nodeSet.nodes
      connection <- node.connections if !connection.channel.isConnected()
    } yield {
      try {
        connected(node, connection.channel.connect(
          new InetSocketAddress(node.host, node.port)))

      } catch {
        case reason: Throwable =>
          logger.warn(s"[$lnm] Fails to connect node: ${node.toShortString}")
      }
    }

    nodeSet
  }

  @deprecated(message = "Will be made private", since = "0.11.10")
  def sendIsMaster(node: Node, id: Int): Node =
    node.connected.headOption.map { channel =>
      import reactivemongo.api.BSONSerializationPack
      import reactivemongo.api.commands.bson.{
        BSONIsMasterCommandImplicits,
        BSONIsMasterCommand
      }, BSONIsMasterCommand.IsMaster
      import reactivemongo.api.commands.Command

      val (isMaster, _) = Command.buildRequestMaker(BSONSerializationPack)(
        IsMaster,
        BSONIsMasterCommandImplicits.IsMasterWriter,
        ReadPreference.primaryPreferred,
        "admin") // only "admin" DB for the admin command

      channel.send(isMaster(id))

      if (node.pingInfo.lastIsMasterId == -1) {
        node._copy(
          pingInfo = node.pingInfo.copy(
            lastIsMasterTime = System.currentTimeMillis(),
            lastIsMasterId = id))

      } else if (node.pingInfo.lastIsMasterId >= PingInfo.pingTimeout) {
        node._copy(
          pingInfo = node.pingInfo.copy(
            lastIsMasterTime = System.currentTimeMillis(),
            lastIsMasterId = id,
            ping = Long.MaxValue))

      } else node
    }.getOrElse(node)

  @deprecated(message = "Will be made private", since = "0.11.10")
  def allChannelGroup(nodeSet: NodeSet): DefaultChannelGroup = {
    val result = new DefaultChannelGroup

    for (node <- nodeSet.nodes) {
      for (connection <- node.connections) result.add(connection.channel)
    }

    result
  }

  // Auth Methods
  private object AuthRequestsManager {
    var authRequests =
      Map.empty[Authenticate, List[Promise[SuccessfulAuthentication]]]

    def addAuthRequest(request: AuthRequest): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      val found = authRequests.get(request.authenticate)
      authRequests = authRequests + (request.authenticate -> (request.promise :: found.getOrElse(Nil)))
      authRequests
    }

    def handleAuthResult(authenticate: Authenticate, result: SuccessfulAuthentication): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      val found = authRequests.get(authenticate)
      if (found.isDefined) {
        found.get.foreach { _.success(result) }
        authRequests = authRequests - authenticate
      }
      authRequests
    }

    def handleAuthResult(authenticate: Authenticate, result: Throwable): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      val found = authRequests.get(authenticate)

      logger.error(s"[$lnm] Authentication failure", result)

      if (found.isDefined) {
        found.get.foreach { _.failure(result) }
        authRequests = authRequests - authenticate
      }
      authRequests
    }
  }
}

@deprecated("Internal class: will be made private", "0.11.14")
final class LegacyDBSystem private[reactivemongo] (
  val supervisor: String,
  val name: String,
  val seeds: Seq[String],
  val initialAuthenticates: Seq[Authenticate],
  val options: MongoConnectionOptions)(
    val channelFactory: ChannelFactory = new ChannelFactory(supervisor, name, options))
    extends MongoDBSystem with MongoCrAuthentication {

  @deprecated("Initialize with an explicit supervisor and connection names", "0.11.14")
  def this(s: Seq[String], a: Seq[Authenticate], opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode opts}", s"unknown-${System identityHashCode opts}", s, a, opts)(new ChannelFactory(opts))

  @deprecated("Initialize with an explicit supervisor and connection names", "0.11.14")
  def this(s: Seq[String], a: Seq[Authenticate], opts: MongoConnectionOptions)(cf: ChannelFactory) = this(s"unknown-${System identityHashCode opts}", s"unknown-${System identityHashCode opts}", s, a, opts)(cf)

}

@deprecated("Internal class: will be made private", "0.11.14")
final class StandardDBSystem private[reactivemongo] (
  val supervisor: String,
  val name: String,
  val seeds: Seq[String],
  val initialAuthenticates: Seq[Authenticate],
  val options: MongoConnectionOptions)(
    val channelFactory: ChannelFactory = new ChannelFactory(supervisor, name, options))
    extends MongoDBSystem with MongoScramSha1Authentication {

  @deprecated("Initialize with an explicit supervisor and connection names", "0.11.14")
  def this(s: Seq[String], a: Seq[Authenticate], opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode opts}", s"unknown-${System identityHashCode opts}", s, a, opts)(new ChannelFactory(opts))

  @deprecated("Initialize with an explicit supervisor and connection names", "0.11.14")
  def this(s: Seq[String], a: Seq[Authenticate], opts: MongoConnectionOptions)(cf: ChannelFactory) = this(s"unknown-${System identityHashCode opts}", s"unknown-${System identityHashCode opts}", s, a, opts)(cf)
}

@deprecated("Internal class: will be made private", "0.11.14")
object MongoDBSystem {
  private[actors] val logger =
    LazyLogger("reactivemongo.core.actors.MongoDBSystem")
}

private[actors] case class AwaitingResponse(
    request: Request,
    channelID: Int,
    promise: Promise[Response],
    isGetLastError: Boolean,
    isMongo26WriteOp: Boolean) {

  @inline def requestID: Int = request.requestID

  private var _retry = 0 // TODO: Refactor as property

  // TODO: Refactor as Property
  var _writeConcern: Option[Request] = None
  def withWriteConcern(wc: Request): AwaitingResponse = {
    _writeConcern = Some(wc)
    this
  }
  def getWriteConcern: Option[Request] = _writeConcern

  def retriable(max: Int): Option[Int => AwaitingResponse] =
    if (_retry >= max) None else Some({ chanId: Int =>
      val req = copy(this.request, channelID = chanId)

      req._retry = _retry + 1
      req._writeConcern = _writeConcern

      req
    })

  def copy(
    request: Request = this.request,
    channelID: Int = this.channelID,
    promise: Promise[Response] = this.promise,
    isGetLastError: Boolean = this.isGetLastError,
    isMongo26WriteOp: Boolean = this.isMongo26WriteOp): AwaitingResponse =
    AwaitingResponse(request, channelID, promise,
      isGetLastError, isMongo26WriteOp)

  @deprecated(message = "Use [[copy]] with `Request`", since = "0.12.0-RC1")
  def copy(
    requestID: Int,
    channelID: Int,
    promise: Promise[Response],
    isGetLastError: Boolean,
    isMongo26WriteOp: Boolean): AwaitingResponse = {
    val req = copy(this.request,
      channelID = channelID,
      promise = promise,
      isGetLastError = isGetLastError,
      isMongo26WriteOp = isMongo26WriteOp)

    req._retry = this._retry
    req._writeConcern = this._writeConcern

    req
  }
}

/**
 * A message to send to a MonitorActor to be warned when a primary has been discovered.
 */
@deprecated(message = "Will be removed", since = "0.11.10")
case object WaitForPrimary

private[actors] object RequestId {
  // all requestIds [0, 1000[ are for isMaster messages
  val isMaster = RequestIdGenerator(0, 999)

  // all requestIds [1000, 2000[ are for getnonce messages
  val getNonce = RequestIdGenerator(1000, 1999) // CR auth

  // all requestIds [2000, 3000[ are for authenticate messages
  val authenticate = RequestIdGenerator(2000, 2999)

  // all requestIds [3000[ are for common messages
  val common = RequestIdGenerator(3000, Int.MaxValue - 1)
}

/**
 * @param lower the lower bound
 * @param upper the upper bound
 */
private[actors] case class RequestIdGenerator(lower: Int, upper: Int) {
  private val iterator = Iterator.iterate(lower)(i => if (i == upper) lower else i + 1)

  def next = iterator.next
  def accepts(id: Int): Boolean = id >= lower && id <= upper
  def accepts(response: Response): Boolean = accepts(response.header.responseTo)
}

// exceptions
object Exceptions {
  /** An exception thrown when a request needs a non available primary. */
  case object PrimaryUnavailableException extends DriverException {
    val message = "No primary node is available!"
  }

  /**
   * An exception thrown when the entire node set is unavailable.
   * The application may not have access to the network anymore.
   */
  sealed class NodeSetNotReachable private (val message: String)
      extends DriverException {

    private[reactivemongo] def this(con: reactivemongo.api.MongoConnection) = this(s"The node set can not be reached! Please check your network connectivity (${con.supervisor}/${con.name})")

    def this() = this("The node set can not be reached! Please check your network connectivity")
  }

  case object NodeSetNotReachable extends NodeSetNotReachable()

  object ChannelNotFound extends DriverException {
    val message = "ChannelNotFound"
  }

  sealed class ClosedException private (val message: String)
      extends DriverException {

    private[reactivemongo] def this(supervisor: String, connection: String) = this(s"This MongoConnection is closed ($supervisor/$connection)")

    def this() = this("This MongoConnection is closed")
  }

  case object ClosedException extends ClosedException()
}
