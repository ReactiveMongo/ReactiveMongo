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
import shaded.netty.channel.group.{
  ChannelGroupFuture,
  ChannelGroupFutureListener,
  DefaultChannelGroup
}
import reactivemongo.util.LazyLogger
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
  QueryableNodeStatus,
  Node,
  NodeSet,
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
private[reactivemongo] case object RefreshAllNodes
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
trait MongoDBSystem extends Actor {
  import scala.concurrent.duration._
  import reactivemongo.bson.BSONDocument
  import MongoDBSystem._

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

    context.system.scheduler.schedule(interval, interval, self, RefreshAllNodes)
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

  private final def authenticateNode(node: Node, auths: Seq[Authenticate]): Node = node.copy(connections = node.connections.map {
    case connection if connection.status == ConnectionStatus.Connected =>
      authenticateConnection(connection, auths)

    case connection => connection
  })

  private final def authenticateNodeSet(nodeSet: NodeSet): NodeSet =
    nodeSet.copy(nodes = nodeSet.nodes.map {
      case node @ Node(_, _: QueryableNodeStatus, _, _, _, _, _, _) =>
        authenticateNode(node, nodeSet.authenticates.toSeq)

      case node => node
    })

  private def unauthenticate(node: Node, connections: Vector[Connection]): Node = node.copy(
    status = NodeStatus.Unknown,
    connections = connections,
    authenticated = if (connections.isEmpty) Set.empty else node.authenticated)

  private def stopWhenDisconnected[T](state: String, msg: T): Unit = {
    val remainingConnections = nodeSet.nodes.foldLeft(0)(
      { (open, node) => open + node.connections.size })

    if (logger.isDebugEnabled) {
      val disconnected = nodeSet.nodes.foldLeft(0) { (open, node) =>
        open + node.connections.count(_.status == ConnectionStatus.Disconnected)
      }

      logger.debug(s"(State: $state) Received $msg remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
    }

    if (remainingConnections == 0) {
      monitors.foreach(_ ! Closed)
      logger.info(s"$self is stopping")
      context.stop(self)
    }
  }

  def updateNodeSetOnDisconnect(channelId: Int): NodeSet =
    updateNodeSet(nodeSet.updateNodeByChannelId(channelId) { node =>
      val connections = node.connections.map { connection =>
        if (connection.channel.getId() == channelId)
          connection.copy(status = ConnectionStatus.Disconnected)
        else connection
      }

      unauthenticate(node, connections)
    })

  protected def authReceive: Receive

  val closing: Receive = {
    case req @ RequestMaker(_, _, _, _) =>
      logger.error(s"Received a non-expecting response request during closing process: $req")

    case RegisterMonitor => monitors += sender

    case req @ ExpectingResponse(promise) => {
      logger.trace(s"Received an expecting response request during closing process: $req, completing its promise with a failure")
      promise.failure(Exceptions.ClosedException)
    }

    case msg @ ChannelClosed(channelId) => {
      updateNodeSet(nodeSet.updateNodeByChannelId(channelId) { node =>
        unauthenticate(node, node.connections.
          filter(_.channel.getId != channelId))
      })

      stopWhenDisconnected("Closing", msg)
    }

    case msg @ ChannelDisconnected(channelId) => {
      updateNodeSetOnDisconnect(channelId)

      if (logger.isDebugEnabled) {
        val remainingConnections = nodeSet.nodes.foldLeft(0) { (open, node) =>
          open + node.connections.size
        }
        val disconnected = nodeSet.nodes.foldLeft(0) { (open, node) =>
          open + node.connections.count(
            _.status == ConnectionStatus.Disconnected)
        }

        logger.debug(s"(State: Closing) Received $msg, remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
      }
    }

    case msg @ ChannelConnected(channelId) =>
      logger.warn(s"(State: Closing) SPURIOUS $msg (ignored, channel closed)")
      updateNodeSetOnDisconnect(channelId)

    case other =>
      logger.error(s"(State: Closing) UNHANDLED MESSAGE: $other")
  }

  private val processing: Receive = {
    case RegisterMonitor => monitors += sender

    case Close => {
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

          try {
            factory.channelFactory.releaseExternalResources
          } catch {
            case err: Throwable =>
              logger.debug("fails to release channel resources", err)
          }
        }
      }
      allChannelGroup(nodeSet).close.addListener(listener)

      // fail all requests waiting for a response
      awaitingResponses.foreach { pair =>
        val promise = pair._2.promise
        if (!promise.isCompleted) {
          promise.failure(Exceptions.ClosedException)
        }
      }
      awaitingResponses.empty

      val connectedCon = nodeSet.nodes.foldLeft(0) { _ + _.connected.size }

      // moving to closing state
      context become closing

      if (connectedCon == 0) stopWhenDisconnected("Processing", Close)
    }

    case req @ RequestMaker(_, _, _, _) => {
      logger.debug(s"Transmiting a request: $req")

      val r = req(RequestId.common.next)
      pickChannel(r).map(_._2.send(r))
    }

    case req @ RequestMakerExpectingResponse(maker, _) => {
      val reqId = RequestId.common.next

      logger.trace(s"Received a request expecting a response ($reqId): $req")

      val request = maker(reqId)

      pickChannel(request) match {
        case Failure(error) => {
          logger.trace(s"No channel, error with promise ${req.promise}")
          req.promise.failure(error)
        }

        case Success((node, connection)) => {
          logger.trace(s"Sending request ($reqId) expecting response by connection $connection of node ${node.name}: $request")

          if (request.op.expectsResponse) {
            awaitingResponses += request.requestID -> AwaitingResponse(
              reqId, connection.channel.getId, req.promise,
              isGetLastError = false,
              isMongo26WriteOp = req.isMongo26WriteOp)

            logger.trace(s"registering awaiting response for requestID $reqId, awaitingResponses: $awaitingResponses")
          } else logger.trace(s"NOT registering awaiting response for requestID $reqId")

          connection.send(request)
        }
      }
    }

    case req @ CheckedWriteRequestExpectingResponse(_) => {
      logger.trace("Received a checked write request")

      val checkedWriteRequest = req.checkedWriteRequest
      val requestId = RequestId.common.next
      val (request, writeConcern) = {
        val tuple = checkedWriteRequest()
        tuple._1(requestId) -> tuple._2(requestId)
      }

      pickChannel(request) match {
        case Failure(error) => req.promise.failure(error)

        case Success((node, connection)) => {
          logger.trace(s"Sending request expecting response $request by connection $connection of node ${node.name}")

          awaitingResponses += requestId -> AwaitingResponse(requestId, connection.channel.getId(), req.promise, isGetLastError = true, isMongo26WriteOp = false)
          logger.trace(s"registering awaiting response for requestID $requestId, awaitingResponses: $awaitingResponses")
          connection.send(request, writeConcern)
        }
      }
    }

    case ConnectAll => { // monitor
      updateNodeSet(nodeSet.createNeededChannels(
        self, options.nbChannelsPerNode))

      logger.trace("ConnectAll Job running... Status: " + nodeSet.nodes.map(_.toShortString).mkString(" | "))
      connectAll(nodeSet)
    }

    case RefreshAllNodes => {
      nodeSet.nodes.foreach { node =>
        logger.trace(s"try to refresh ${node.name}")
        updateNodeSet(nodeSet.updateAll { node =>
          sendIsMaster(node, RequestId.isMaster.next)
        })
      }

      logger.trace(
        s"RefreshAllNodes Job running... Status: ${nodeSet.toShortString}")
    }

    case ChannelConnected(channelId) => {
      updateNodeSet(nodeSet.updateByChannelId(channelId)(
        _.copy(status = ConnectionStatus.Connected)) { node =>
          sendIsMaster(node, RequestId.isMaster.next)
        })

      logger.trace(s"Channel #$channelId connected. NodeSet status: ${nodeSet.toShortString}")
    }

    case channelUnavailable @ ChannelUnavailable(channelId) => {
      logger.trace(s"Channel #$channelId unavailable ($channelUnavailable).")

      val nodeSetWasReachable = nodeSet.isReachable
      val primaryWasAvailable = nodeSet.primary.isDefined

      channelUnavailable match {
        case ChannelClosed(_) => updateNodeSet(
          nodeSet.updateNodeByChannelId(channelId) { node =>
            unauthenticate(node, node.connections.
              filter(_.channel.getId != channelId))
          })

        case ChannelDisconnected(_) => updateNodeSetOnDisconnect(channelId)
      }

      awaitingResponses.retain { (_, awaitingResponse) =>
        if (awaitingResponse.channelID == channelId) {
          logger.debug(s"completing promise ${awaitingResponse.promise} with error='socket disconnected'")

          awaitingResponse.promise.
            failure(GenericDriverException("socket disconnected"))

          false
        } else true
      }

      if (!nodeSet.isReachable) {
        if (nodeSetWasReachable) {
          logger.warn("The entire node set is unreachable, is there a network problem?")
          broadcastMonitors(SetUnavailable)
        } else logger.debug("The entire node set is still unreachable, is there a network problem?")
      } else if (!nodeSet.primary.isDefined) {
        if (primaryWasAvailable) {
          logger.warn("The primary is unavailable, is there a network problem?")
          broadcastMonitors(PrimaryUnavailable)
        } else logger.debug(
          "The primary is still unavailable, is there a network problem?")
      }

      logger.trace(s"Channel #$channelId is released")
    }

    case response @ Response(_, _, _, _) if ( // isMaster response
      RequestId.isMaster accepts response) => {

      val nodeSetWasReachable = nodeSet.isReachable
      val wasPrimary = nodeSet.primary.toSeq.flatMap(_.names)

      import reactivemongo.api.BSONSerializationPack
      import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits
      import reactivemongo.api.commands.Command

      @volatile var chanNode = Option.empty[Node]
      val isMaster = Command.deserialize(BSONSerializationPack, response)(
        BSONIsMasterCommandImplicits.IsMasterResultReader)

      val ns = nodeSet.updateNodeByChannelId(response.info.channelId) { node =>
        chanNode = Some(node)

        val pingInfo =
          if (node.pingInfo.lastIsMasterId == response.header.responseTo) {
            node.pingInfo.copy(ping =
              System.currentTimeMillis() - node.pingInfo.lastIsMasterTime,
              lastIsMasterTime = 0, lastIsMasterId = -1)

          } else node.pingInfo

        val authenticating =
          if (!isMaster.status.queryable || nodeSet.authenticates.isEmpty) node
          else authenticateNode(node, nodeSet.authenticates.toSeq)

        val meta = ProtocolMetadata(
          MongoWireVersion(isMaster.minWireVersion),
          MongoWireVersion(isMaster.maxWireVersion),
          isMaster.maxBsonObjectSize,
          isMaster.maxMessageSizeBytes,
          isMaster.maxWriteBatchSize)

        val an = authenticating.copy(
          status = isMaster.status,
          pingInfo = pingInfo,
          tags = isMaster.replicaSet.flatMap(_.tags),
          protocolMetadata = meta,
          isMongos = isMaster.isMongos)

        isMaster.replicaSet.fold(an)(rs => an.withAlias(rs.me))
      }

      updateNodeSet {
        connectAll {
          val upNodes = isMaster.replicaSet.toSeq.flatMap { rs =>
            rs.hosts.collect {
              case host if !ns.nodes.exists(_.names contains host) =>
                Node(host, NodeStatus.Uninitialized,
                  Vector.empty, Set.empty, None, ProtocolMetadata.Default)
            }
          }

          ns.copy(nodes = ns.nodes ++ upNodes).
            createNeededChannels(self, options.nbChannelsPerNode)
        }
      }

      chanNode.foreach { node =>
        if (!nodeSet.authenticates.isEmpty && node.authenticated.isEmpty) {
          logger.debug(s"The node set is available (${node.names}); Waiting authentication: ${node.authenticated}")
        } else {
          if (!nodeSetWasReachable && nodeSet.isReachable) {
            broadcastMonitors(SetAvailable(nodeSet.protocolMetadata))
            logger.info("The node set is now available")
          }

          if (nodeSet.primary.exists(n => !wasPrimary.contains(n.name))) {
            broadcastMonitors(PrimaryAvailable(nodeSet.protocolMetadata))
            logger.info(s"The primary is now available: ${node.names}")
          }
        }
      }
    }
  }

  private def lastError(response: Response): Either[Throwable, LastError] = {
    import reactivemongo.api.commands.bson.
      BSONGetLastErrorImplicits.LastErrorReader

    Response.parse(response).next().asTry[LastError] match {
      case Failure(err) => Left(err)
      case Success(err) => Right(err)
    }
  }

  private val fallback: Receive = {
    // any other response
    case response: Response if RequestId.common accepts response => {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _, promise, isGetLastError, isMongo26WriteOp)) => {
          logger.trace(s"Got a response from ${response.info.channelId} to ${response.header.responseTo}! Will give back message=$response to promise ${System.identityHashCode(promise)}")
          awaitingResponses -= response.header.responseTo

          if (response.error.isDefined) {
            logger.debug(s"{${response.header.responseTo}} sending a failure... (${response.error.get})")

            if (response.error.get.isNotAPrimaryError) onPrimaryUnavailable()
            promise.failure(response.error.get)
          } else if (isGetLastError) {
            logger.debug(s"{${response.header.responseTo}} it's a getlasterror")

            // todo, for now rewinding buffer at original index
            import reactivemongo.api.commands.bson.BSONGetLastErrorImplicits.LastErrorReader
            lastError(response).fold(e => {
              logger.error(s"Error deserializing LastError message #${response.header.responseTo}", e)
              promise.failure(new RuntimeException(s"Error deserializing LastError message #${response.header.responseTo}", e))
            },
              lastError => {
                if (lastError.inError) {
                  logger.debug(s"{${response.header.responseTo}} sending a failure (lasterror is not ok)")
                  if (lastError.isNotAPrimaryError) onPrimaryUnavailable()
                  promise.failure(lastError)
                } else {
                  logger.trace(s"{${response.header.responseTo}} sending a success (lasterror is ok)")
                  response.documents.readerIndex(response.documents.readerIndex)
                  promise.success(response)
                }
              })
          } else if (isMongo26WriteOp) {
            // TODO - logs, bson
            // MongoDB 26 Write Protocol errors
            logger.trace("Received a response to a MongoDB2.6 Write Op")

            import reactivemongo.bson.lowlevel._
            import reactivemongo.core.netty.ChannelBufferReadableBuffer
            val reader = new LowLevelBsonDocReader(new ChannelBufferReadableBuffer(response.documents))
            val fields = reader.fieldStream
            val okField = fields.find(_.name == "ok")
            logger.trace(s"{${response.header.responseTo}} ok field is: $okField")
            val processedOk = okField.collect {
              case BooleanField(_, v) => v
              case IntField(_, v)     => v != 0
              case DoubleField(_, v)  => v != 0
            }.getOrElse(false)

            if (processedOk) {
              logger.trace(s"{${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
              promise.success(response)
            } else {
              logger.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] processedOk is false! sending an error")

              val notAPrimary = fields.find(_.name == "errmsg").exists {
                case errmsg @ LazyField(0x02, _, buf) =>
                  logger.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg!")
                  buf.readString == "not a primary"
                case errmsg =>
                  logger.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg but not interesting!")
                  false
              }

              if (notAPrimary) {
                logger.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] not a primary error!")
                onPrimaryUnavailable()
              }

              promise.failure(GenericDriverException("not ok"))
            }
          } else {
            logger.trace(s"{${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
            promise.success(response)
          }
        }

        case None => logger.error(s"oups. ${response.header.responseTo} not found! complete message is $response")
      }
    }

    case request @ AuthRequest(authenticate, _) => {
      logger.info(s"New authenticate request $authenticate")
      AuthRequestsManager.addAuthRequest(request)
      updateNodeSet(authenticateNodeSet(nodeSet.
        copy(authenticates = nodeSet.authenticates + authenticate)))
    }

    case a => logger.error(s"not supported: $a")
  }

  override lazy val receive: Receive =
    processing.orElse(authReceive).orElse(fallback)

  // monitor -->
  private var nodeSet: NodeSet = NodeSet(None, None, seeds.map(seed => Node(seed, NodeStatus.Unknown, Vector.empty, Set.empty, None, ProtocolMetadata.Default).createNeededChannels(self, 1)).toVector, initialAuthenticates.toSet)

  connectAll(nodeSet)
  self ! RefreshAllNodes
  // <-- monitor

  def onPrimaryUnavailable() {
    self ! RefreshAllNodes
    updateNodeSet(nodeSet.updateAll(node => if (node.status == NodeStatus.Primary) node.copy(status = NodeStatus.Unknown) else node))
    broadcastMonitors(PrimaryUnavailable)
  }

  private def updateNodeSet(nodeSet: NodeSet): NodeSet = {
    this.nodeSet = nodeSet
    nodeSet
  }

  private def updateAuthenticate(channelId: Int, replyTo: Authenticate, auth: Option[Authenticated]): NodeSet = {
    val ns = nodeSet.updateByChannelId(channelId) { con =>
      val authed = auth.map(con.authenticated + _).getOrElse(con.authenticated)

      authenticateConnection(con.copy(
        authenticated = authed, authenticating = None),
        nodeSet.authenticates.toSeq)

    } { node =>
      node.copy(authenticated = auth.map(node.authenticated + _).
        getOrElse(node.authenticated))
    }

    if (!auth.isDefined) ns.copy(authenticates = ns.authenticates - replyTo)
    else ns
  }

  protected def authenticationResponse(response: Response)(check: Response => Either[CommandError, SuccessfulAuthentication]): NodeSet = {
    val auth = nodeSet.pickByChannelId(
      response.info.channelId).flatMap(_._2.authenticating)

    updateNodeSet(auth match {
      case Some(a @ Authenticating(db, user, pass)) => {
        val originalAuthenticate = Authenticate(db, user, pass)
        val authenticated = check(response) match {
          case Right(successfulAuthentication) => {
            AuthRequestsManager.handleAuthResult(
              originalAuthenticate, successfulAuthentication)

            if (nodeSet.isReachable) {
              broadcastMonitors(SetAvailable(nodeSet.protocolMetadata))
              logger.debug("The node set is now authenticated")
            }

            if (nodeSet.primary.isDefined) {
              broadcastMonitors(PrimaryAvailable(nodeSet.protocolMetadata))
              logger.debug("The primary is now authenticated")
            }

            Some(Authenticated(db, user))
          }

          case Left(error) => {
            AuthRequestsManager.handleAuthResult(originalAuthenticate, error)
            None
          }
        }

        updateAuthenticate(
          response.info.channelId, originalAuthenticate, authenticated)
      }

      case res =>
        logger.warn(s"Authentication result: $res")
        nodeSet
    })
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
      nodeSet.pickByChannelId(request.channelIdHint.get).map(Success(_)).
        getOrElse(Failure(Exceptions.ChannelNotFound))

    } else nodeSet.pick(request.readPreference).map(Success(_)).
      getOrElse(Failure(Exceptions.PrimaryUnavailableException))
  }

  private[actors] def whenAuthenticating(channelId: Int)(f: Tuple2[Connection, Authenticating] => Connection): NodeSet = updateNodeSet(
    nodeSet.updateConnectionByChannelId(channelId) { connection =>
      connection.authenticating.fold(connection)(authenticating =>
        f(connection -> authenticating))
    })

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

    awaitingResponses.foreach {
      case (_, r) if (!r.promise.isCompleted) =>
        // fail all requests waiting for a response
        r.promise.failure(Exceptions.ClosedException)
      case _ => ( /* already completed */ )
    }

    awaitingResponses.empty

    logger.warn(s"MongoDBSystem $self stopped.")
  }

  private def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)

  private def connectAll(nodeSet: NodeSet): NodeSet = {
    for {
      node <- nodeSet.nodes
      connection <- node.connections if !connection.channel.isConnected()
    } yield connection.channel.connect(
      new InetSocketAddress(node.host, node.port))

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
        node.copy(
          pingInfo = node.pingInfo.copy(
            lastIsMasterTime = System.currentTimeMillis(),
            lastIsMasterId = id))

      } else if (node.pingInfo.lastIsMasterId >= PingInfo.pingTimeout) {
        node.copy(
          pingInfo = node.pingInfo.copy(
            lastIsMasterTime = System.currentTimeMillis(),
            lastIsMasterId = id,
            ping = Long.MaxValue))

      } else node
    }.getOrElse(node)

  @deprecated(message = "Will be made private", since = "0.11.10")
  def allChannelGroup(nodeSet: NodeSet) = {
    val result = new DefaultChannelGroup
    for (node <- nodeSet.nodes) {
      for (connection <- node.connections)
        result.add(connection.channel)
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

      logger.error(s"authentication failure", result)

      if (found.isDefined) {
        found.get.foreach { _.failure(result) }
        authRequests = authRequests - authenticate
      }
      authRequests
    }
  }
}

final class LegacyDBSystem(
  val seeds: Seq[String],
  val initialAuthenticates: Seq[Authenticate],
  val options: MongoConnectionOptions)(
    val channelFactory: ChannelFactory = new ChannelFactory(options))
    extends MongoDBSystem with MongoCrAuthentication

final class StandardDBSystem(
  val seeds: Seq[String],
  val initialAuthenticates: Seq[Authenticate],
  val options: MongoConnectionOptions)(
    val channelFactory: ChannelFactory = new ChannelFactory(options))
    extends MongoDBSystem with MongoScramSha1Authentication

object MongoDBSystem {
  private[actors] val logger =
    LazyLogger("reactivemongo.core.actors.MongoDBSystem")
}

private[actors] case class AwaitingResponse(
  requestID: Int,
  channelID: Int,
  promise: Promise[Response],
  isGetLastError: Boolean,
  isMongo26WriteOp: Boolean)

/**
 * A message to send to a MonitorActor to be warned when a primary has been discovered.
 */
@deprecated(message = "Wil be removed", since = "0.11.10")
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

  /** An exception thrown when the entire node set is unavailable. The application may not have access to the network anymore. */
  case object NodeSetNotReachable extends DriverException {
    val message = "The node set can not be reached! Please check your network connectivity."
  }

  object ChannelNotFound extends DriverException {
    val message = "ChannelNotFound"
  }

  object ClosedException extends DriverException {
    val message = "This MongoConnection is closed"
  }
}
