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

import scala.concurrent.Promise
import scala.util.{ Failure, Success, Try }

import akka.actor.{ Actor, ActorRef, Cancellable }

import shaded.netty.channel.{ ChannelFuture, ChannelFutureListener }
import shaded.netty.channel.group.{
  ChannelGroupFuture,
  ChannelGroupFutureListener,
  DefaultChannelGroup
}

import reactivemongo.util.LazyLogger
import reactivemongo.core.errors.GenericDriverException
import reactivemongo.core.protocol.{
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
import external.reactivemongo.ConnectionListener

/** Main actor that processes the requests. */
@deprecated("Internal class: will be made private", "0.11.14")
trait MongoDBSystem extends Actor {
  import scala.concurrent.duration._
  import Exceptions._
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

  /** Returns a fresh channel factory. */
  protected def newChannelFactory(effect: Unit): ChannelFactory

  private var channelFactory: ChannelFactory = newChannelFactory({})

  private val lnm = s"$supervisor/$name" // log naming

  private val listener: Option[ConnectionListener] = {
    val cl = ConnectionListener()
    cl.foreach(_.poolCreated(options, supervisor, name))
    cl
  }

  private val awaitingResponses =
    scala.collection.mutable.LinkedHashMap[Int, AwaitingResponse]()

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()

  implicit val ec = context.system.dispatcher

  // Inner jobs
  private var connectAllJob: Cancellable = NoJob
  private var refreshAllJob: Cancellable = NoJob

  // history
  private val historyMax = 25 // TODO: configurable from options
  private[reactivemongo] var history = shaded.google.common.collect.
    EvictingQueue.create[(Long, String)](historyMax)

  private type NodeSetHandler = (String, NodeSetInfo, NodeSet) => Unit
  private val nodeSetUpdated: NodeSetHandler =
    listener.fold[NodeSetHandler]({ (event: String, _, _) =>
      updateHistory(event)
    }) { l =>
      { (event: String, previous: NodeSetInfo, updated: NodeSet) =>
        updateHistory(event)

        context.system.scheduler.scheduleOnce(1.second) {
          _setInfo = updated.info
          l.nodeSetUpdated(previous, _setInfo)
        }

        ()
      }
    }

  // monitor -->
  private val nodeSetLock = new Object {}
  private[reactivemongo] var _nodeSet: NodeSet = null
  private var _setInfo: NodeSetInfo = null

  initNodeSet()
  nodeSetUpdated(s"Init(${_nodeSet.toShortString})", null, _nodeSet)
  // <-- monitor

  @inline private def updateHistory(event: String): Unit = {
    val time = System.currentTimeMillis()

    @annotation.tailrec
    def go(retry: Int): Unit = try { // compensate EvictionQueue safety
      history.offer(time -> event)
      ()
    } catch {
      case _: Exception if (retry > 0) => go(retry - 1)
      case err: Exception =>
        logger.warn(s"Fails to update history: $event", err)
    }

    go(3)
  }

  private[reactivemongo] def internalState() = new InternalState(
    history.toArray(Array.fill[(Long, String)](historyMax)(null)).
      foldLeft(Array.empty[StackTraceElement]) {
        case (trace, null) => trace

        case (trace, (time, event)) => new StackTraceElement(
          "reactivemongo", event.asInstanceOf[String],
          s"<time:$time>", -1
        ) +: trace
      }
  )

  private[reactivemongo] def getNodeSet = _nodeSet // For test purposes

  /** On start or restart. */
  private def initNodeSet(): NodeSet = {
    val ns = NodeSet(None, None, seeds.map(seed => Node(seed, NodeStatus.Unknown, Vector.empty, Set.empty, None, ProtocolMetadata.Default).createNeededChannels(channelFactory, self, 1)).toVector, initialAuthenticates.toSet)

    _nodeSet = ns
    _setInfo = ns.info

    ns
  }

  private def close(): NodeSet = {
    logger.debug(s"[$lnm] Closing MongoDBSystem")

    // cancel all jobs
    connectAllJob.cancel()
    refreshAllJob.cancel()

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

    val ns = updateNodeSet("Close")(_.updateAll { node =>
      node._copy(connections = node.connected)
      // Only keep already connected connection:
      // - prevent to activate other connection
      // - know which connections to be closed
    })

    allChannelGroup(ns).close().addListener(listener)

    // fail all requests waiting for a response
    val istate = internalState

    awaitingResponses.foreach {
      case (_, r) if (!r.promise.isCompleted) =>
        // fail all requests waiting for a response
        r.promise.failure(new ClosedException(supervisor, name, istate))

      case _ => ( /* already completed */ )
    }

    awaitingResponses.clear()

    ns
  }

  // Akka hooks

  override def preStart(): Unit = {
    logger.info(s"[$lnm] Starting the MongoDBSystem ${self.path}")

    val refreshInterval = options.monitorRefreshMS.milliseconds

    connectAllJob = {
      val ms = options.monitorRefreshMS / 5
      val interval = if (ms < 100) 100.milliseconds else refreshInterval

      context.system.scheduler.schedule(interval, interval, self, ConnectAll)
    }

    refreshAllJob = context.system.scheduler.
      schedule(refreshInterval, refreshInterval, self, RefreshAll)

    _nodeSet = connectAll(_nodeSet, { (node, chan) =>
      val req = requestIsMaster(node)

      chan.addListener(new ChannelFutureListener {
        def operationComplete(chan: ChannelFuture) = { req.send(); () }
      })

      req.node -> chan
    })

    ()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    val details = message.map { m => s": $m" }.getOrElse("")
    val msg = s"Restarting the MongoDBSystem ${self.path}$details"

    logger.info(s"[$lnm] $msg", reason)

    super.preRestart(reason, message)
  }

  override def postStop() {
    logger.info(s"[$lnm] Stopping the MongoDBSystem ${self.path}")

    close()

    ()
  }

  override def postRestart(reason: Throwable): Unit = {
    logger.info(s"[$lnm] The MongoDBSystem is restarted ${self.path} (cause: $reason)")

    // Renew the channel factory, as releaseExternalResources in close@postStop
    channelFactory = newChannelFactory({})

    updateNodeSet(s"Restart(${_nodeSet})")(_ => initNodeSet())

    super.postRestart(reason) // will call preStart()
  }

  // ---

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

  private def collectConnections(node: Node)(collector: PartialFunction[Connection, Connection]): Node = {
    val connections = node.connections.collect(collector)

    if (connections.isEmpty) {
      logger.debug(
        s"[$lnm] No longer connected node; Fallback to Unknown status"
      )

      node._copy(
        status = NodeStatus.Unknown,
        connections = Vector.empty,
        authenticated = Set.empty
      )
    } else node._copy(connections = connections)
  }

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

  def updateNodeSetOnDisconnect(channelId: Int): NodeSet = {
    @inline def event =
      s"ChannelDisconnected($channelId, ${_nodeSet.toShortString})"

    updateNodeSet(event)(_.updateNodeByChannelId(channelId) { node =>
      collectConnections(node) {
        case other if (other.channel.getId != channelId) =>
          other // keep connections for other channels unchanged

        case con =>
          con.copy(status = ConnectionStatus.Disconnected)
      }
    })
  }

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

  private def failureOrLog[T](promise: Promise[T], cause: Throwable)(log: Throwable => Unit): Unit = {
    if (promise.isCompleted) log(cause)
    else { promise.failure(cause); () }
  }

  private def retry(req: AwaitingResponse): Option[AwaitingResponse] = {
    val onError = failureOrLog(req.promise, _: Throwable) { cause =>
      logger.error(s"[$lnm] Fails to retry '${req.request.op}' (channel #${req.channelID})", cause)
    }

    req.retriable(requestRetries).flatMap { newReq =>
      foldNodeConnection(req.request)({ error =>
        onError(error)
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
  }

  val SocketDisconnected = GenericDriverException(s"Socket disconnected ($lnm)")

  protected def authReceive: Receive

  private val processing: Receive = {
    case RegisterMonitor => {
      logger.debug(s"[$lnm] Register monitor $sender")

      monitors += sender

      // In case the NodeSet status has already been updated ...
      // TODO: Test

      val ns = nodeSetLock.synchronized { this._nodeSet }

      if (ns.isReachable) {
        sender ! SetAvailable(ns.protocolMetadata)
        logger.debug(s"[$lnm] The node set is available")
      }

      ns.primary.foreach { prim =>
        if (!ns.authenticates.isEmpty && prim.authenticated.isEmpty) {
          logger.debug(s"[$lnm] The node set is available (${prim.names}); Waiting authentication: ${prim.authenticated}")
        } else {
          sender ! PrimaryAvailable(ns.protocolMetadata)

          logger.debug(s"[$lnm] The primary is available: $prim")
        }
      }

      ()
    }

    case Close => {
      logger.debug(s"[$lnm] Received Close message, going to close connections and moving on state Closing")

      val ns = close()
      val connectedCon = ns.nodes.foldLeft(0) { _ + _.connected.size }

      // moving to closing state
      context become closing

      if (connectedCon == 0) stopWhenDisconnected("Processing", Close)

      listener.foreach(_.poolShutdown(supervisor, name))
    }

    case req @ RequestMaker(_, _, _, _) => {
      logger.trace(s"[$lnm] Transmiting a request: $req")

      val r = req(RequestId.common.next)
      pickChannel(r).map(_._2.send(r))

      ()
    }

    case req @ RequestMakerExpectingResponse(maker, _) => {
      val reqId = RequestId.common.next

      logger.trace(
        s"[$lnm] Received a request expecting a response ($reqId): $req"
      )

      val request = maker(reqId)

      foldNodeConnection(request)(req.promise.failure(_), { (node, con) =>
        if (request.op.expectsResponse) {
          awaitingResponses += reqId -> AwaitingResponse(
            request, con.channel.getId, req.promise,
            isGetLastError = false,
            isMongo26WriteOp = req.isMongo26WriteOp
          )

          logger.trace(s"[$lnm] Registering awaiting response for requestID $reqId, awaitingResponses: $awaitingResponses")
        } else logger.trace(s"[$lnm] NOT registering awaiting response for requestID $reqId")

        con.send(request)
      })

      ()
    }

    case req @ CheckedWriteRequestExpectingResponse(_) => {
      logger.debug(s"[$lnm] Received a checked write request")

      val checkedWriteRequest = req.checkedWriteRequest
      val reqId = RequestId.common.next
      val (request, writeConcern) = {
        val tuple = checkedWriteRequest()
        tuple._1(reqId) -> tuple._2(reqId)
      }
      val onError = failureOrLog(req.promise, _: Throwable) { cause =>
        logger.error(
          s"[$lnm] Fails to register a request: ${request.op}", cause
        )
      }

      foldNodeConnection(request)(onError, { (node, con) =>
        awaitingResponses += reqId -> AwaitingResponse(
          request, con.channel.getId, req.promise,
          isGetLastError = true, isMongo26WriteOp = false
        ).withWriteConcern(writeConcern)

        logger.trace(s"[$lnm] Registering awaiting response for requestID $reqId, awaitingResponses: $awaitingResponses")

        con.send(request, writeConcern)
      })

      ()
    }

    case ConnectAll => { // monitor
      val statusInfo = _nodeSet.toShortString

      updateNodeSet(s"ConnectAll($statusInfo)") { ns =>
        connectAll(ns.createNeededChannels(
          channelFactory, self, options.nbChannelsPerNode
        ))
      }

      logger.debug(s"[$lnm] ConnectAll Job running... Status: $statusInfo")
    }

    case RefreshAll => {
      val statusInfo = _nodeSet.toShortString

      updateNodeSet(s"RefreshAll($statusInfo)")(_.updateAll { node =>
        logger.trace(s"[$lnm] Try to refresh ${node.name}")

        requestIsMaster(node).send()
      })

      logger.debug(s"[$lnm] RefreshAll Job running... Status: $statusInfo")

      context.system.scheduler.scheduleOnce(Duration.Zero) {
        while (history.size > historyMax) { // compensate EvictionQueue safety
          history.poll()
        }
      }

      ()
    }

    case ChannelConnected(channelId) => {
      val statusInfo = _nodeSet.toShortString

      updateNodeSet(s"ChannelConnected($channelId, $statusInfo)")(
        _.updateByChannelId(channelId)(
          _.copy(status = ConnectionStatus.Connected)
        ) { requestIsMaster(_).send() }
      )

      logger.trace(
        s"[$lnm] Channel #$channelId connected. NodeSet status: $statusInfo"
      )
    }

    case channelUnavailable @ ChannelUnavailable(channelId) => {
      logger.trace(
        s"[$lnm] Channel #$channelId is unavailable ($channelUnavailable)."
      )

      val nodeSetWasReachable = _nodeSet.isReachable
      val primaryWasAvailable = _nodeSet.primary.isDefined

      channelUnavailable match {
        case ChannelClosed(_) => {
          @inline def event =
            s"ChannelClosed($channelId, ${_nodeSet.toShortString})"

          updateNodeSet(event)(_.updateNodeByChannelId(channelId) { node =>
            collectConnections(node) {
              case other if (other.channel.getId != channelId) => other
            }
          })
        }

        case ChannelDisconnected(_) => updateNodeSetOnDisconnect(channelId)
      }

      val retried = Map.newBuilder[Int, AwaitingResponse]

      awaitingResponses.retain { (_, awaitingResponse) =>
        if (awaitingResponse.channelID == channelId) {
          retry(awaitingResponse) match {
            case Some(awaiting) => {
              logger.trace(s"[$lnm] Retrying to await response for requestID ${awaiting.requestID}: $awaiting")
              retried += awaiting.requestID -> awaiting
            }

            case _ => {
              logger.debug(s"[$lnm] Completing response for '${awaitingResponse.request.op}' with error='socket disconnected' (channel #$channelId)")

              failureOrLog(awaitingResponse.promise, SocketDisconnected)(
                err => logger.warn(
                  s"[$lnm] Socket disconnected (channel #$channelId)", err
                )
              )
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
          s"[$lnm] The primary is still unavailable, is there a network problem?"
        )
      }

      logger.debug(s"[$lnm] Channel #$channelId is released")
    }

    case IsMasterResponse(response) => onIsMaster(response)
  }

  val closing: Receive = {
    case req @ RequestMaker(_, _, _, _) =>
      logger.error(s"[$lnm] Received a non-expecting response request during closing process: $req")

    case RegisterMonitor => { monitors += sender; () }

    case req @ ExpectingResponse(promise) => {
      logger.debug(s"[$lnm] Received an expecting response request during closing process: $req, completing its promise with a failure")
      promise.failure(new ClosedException(supervisor, name, internalState))
      ()
    }

    case msg @ ChannelClosed(channelId) => {
      updateNodeSet(s"ChannelClosed($channelId)")(
        _.updateNodeByChannelId(channelId) { node =>
          collectConnections(node) {
            case other if (other.channel.getId != channelId) => other
          }
        }
      )

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
            _.status == ConnectionStatus.Disconnected
          )
        }

        logger.debug(s"[$lnm$$Closing] Received $msg, remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
      }
    }

    case msg @ ChannelConnected(channelId) => {
      logger.warn(s"[$lnm$$Closing] SPURIOUS $msg (ignored, channel closed)")

      updateNodeSetOnDisconnect(channelId)
      ()
    }

    case Close =>
      logger.warn(s"[$lnm$$Closing] Already closing... ignore Close message")

    case other =>
      logger.debug(s"[$lnm$$Closing] Unhandled message: $other")
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
            ()
          } else if (isGetLastError) {
            logger.debug(s"[$lnm] {${response.header.responseTo}} it's a getlasterror")

            // todo, for now rewinding buffer at original index
            //import reactivemongo.api.commands.bson.BSONGetLastErrorImplicits.LastErrorReader

            lastError(response).fold({ e =>
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

            ()
          } else if (isMongo26WriteOp) {
            // TODO - logs, bson
            // MongoDB 26 Write Protocol errors
            logger.trace(
              s"[$lnm] Received a response to a MongoDB2.6 Write Op"
            )

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
              ()
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
              ()
            }
          } else {
            logger.trace(s"[$lnm] {${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
            promise.success(response)
            ()
          }
        }

        case _ => logger.error(s"[$lnm] Oups. ${response.header.responseTo} not found! complete message is $response")
      }
    }

    case request @ AuthRequest(authenticate, _) => {
      logger.debug(s"[$lnm] New authenticate request $authenticate")

      AuthRequestsManager.addAuthRequest(request)

      updateNodeSet(authenticate.toString) { ns =>
        authenticateNodeSet(ns.
          copy(authenticates = ns.authenticates + authenticate))
      }
      ()
    }

    case a => logger.error(s"[$lnm] Not supported: $a")
  }

  override lazy val receive: Receive =
    processing.orElse(authReceive).orElse(fallback)

  // ---

  private def onIsMaster(response: Response) {
    import reactivemongo.api.BSONSerializationPack
    import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits
    import reactivemongo.api.commands.Command

    val isMaster = Command.deserialize(BSONSerializationPack, response)(
      BSONIsMasterCommandImplicits.IsMasterResultReader
    )

    logger.trace(s"[$lnm] IsMaster response: $isMaster")

    val updated = {
      val respTo = response.header.responseTo
      @inline def event =
        s"IsMaster(${respTo}, ${_nodeSet.toShortString}"

      updateNodeSet(event) { nodeSet =>
        val nodeSetWasReachable = nodeSet.isReachable
        val wasPrimary: Option[Node] = nodeSet.primary
        @volatile var chanNode = Option.empty[Node]

        // Update the details of the node corresponding to the response chan
        val prepared =
          nodeSet.updateNodeByChannelId(response.info.channelId) { node =>
            val pingInfo =
              if (node.pingInfo.lastIsMasterId == respTo) {
                // No longer waiting response for isMaster,
                // reset the pending state, and update the ping time
                node.pingInfo.copy(
                  ping =
                  System.currentTimeMillis() - node.pingInfo.lastIsMasterTime,
                  lastIsMasterTime = 0, lastIsMasterId = -1
                )
              } else node.pingInfo

            val nodeStatus = isMaster.status
            val authenticating =
              if (!nodeStatus.queryable || nodeSet.authenticates.isEmpty) {
                node
              } else authenticateNode(node, nodeSet.authenticates.toSeq)

            val meta = ProtocolMetadata(
              MongoWireVersion(isMaster.minWireVersion),
              MongoWireVersion(isMaster.maxWireVersion),
              isMaster.maxBsonObjectSize,
              isMaster.maxMessageSizeBytes,
              isMaster.maxWriteBatchSize
            )

            val an = authenticating._copy(
              status = nodeStatus,
              pingInfo = pingInfo,
              tags = isMaster.replicaSet.flatMap(_.tags),
              protocolMetadata = meta,
              isMongos = isMaster.isMongos
            )

            val n = isMaster.replicaSet.fold(an)(rs => an.withAlias(rs.me))
            chanNode = Some(n)

            n
          }

        val discoveredNodes = isMaster.replicaSet.toSeq.flatMap {
          _.hosts.collect {
            case host if (!prepared.nodes.exists(_.names contains host)) =>
              // Prepare node for newly discovered host in the RS
              Node(host, NodeStatus.Uninitialized,
                Vector.empty, Set.empty, None, ProtocolMetadata.Default)
          }
        }

        logger.trace(s"[$lnm] Discovered nodes: $discoveredNodes")

        val upSet = prepared.copy(nodes = prepared.nodes ++ discoveredNodes)

        chanNode.fold(upSet) { node =>
          if (!upSet.authenticates.isEmpty && node.authenticated.isEmpty) {
            logger.debug(s"[$lnm] The node set is available (${node.names}); Waiting authentication: ${node.authenticated}")

          } else {
            if (!nodeSetWasReachable && upSet.isReachable) {
              broadcastMonitors(SetAvailable(upSet.protocolMetadata))
              logger.debug(s"[$lnm] The node set is now available")
            }

            @inline def wasPrimNames: Seq[String] =
              wasPrimary.toSeq.flatMap(_.names)

            if (upSet.primary.exists(n => !wasPrimNames.contains(n.name))) {
              broadcastMonitors(PrimaryAvailable(upSet.protocolMetadata))

              val newPrim = upSet.primary.map(_.name)

              logger.debug(
                s"[$lnm] The primary is now available: ${newPrim.mkString}"
              )
            }
          }

          if (node.status != NodeStatus.Primary) upSet else {
            // Current node is known and primary

            // TODO: Test; 1. an NodeSet with node A & B, with primary A;
            // 2. Receive a isMaster response from B, with primary = B

            upSet.updateAll { n =>
              if (!node.names.contains(n.name) && // the node itself
                n.status == NodeStatus) {
                // invalidate node status on primary status conflict
                n._copy(status = NodeStatus.Unknown)
              } else n
            }
          }
        }
      }
    }

    context.system.scheduler.scheduleOnce(Duration.Zero) {
      @inline def event = s"ConnectAll$$IsMaster(${response.header.responseTo}, ${updated.toShortString})"

      updateNodeSet(event) { ns =>
        connectAll(ns.createNeededChannels(
          channelFactory, self, options.nbChannelsPerNode
        ))
      }

      ()
    }

    ()
  }

  private[reactivemongo] def onPrimaryUnavailable() {
    self ! RefreshAll

    updateNodeSet("PrimaryUnavailable")(_.updateAll { node =>
      if (node.status != NodeStatus.Primary) node
      else node._copy(status = NodeStatus.Unknown)
    })

    broadcastMonitors(PrimaryUnavailable)
  }

  private[reactivemongo] def updateNodeSet(event: String)(f: NodeSet => NodeSet): NodeSet = {
    var previous: NodeSetInfo = null
    var updated: NodeSet = null

    nodeSetLock.synchronized {
      previous = this._setInfo
      updated = f(this._nodeSet)

      this._nodeSet = updated
    }

    nodeSetUpdated(event, previous, updated)

    updated
  }

  private def updateAuthenticate(nodeSet: NodeSet, channelId: Int, replyTo: Authenticate, auth: Option[Authenticated]): NodeSet = {
    val ns = nodeSet.updateByChannelId(channelId) { con =>
      val authed = auth.map(con.authenticated + _).getOrElse(con.authenticated)

      authenticateConnection(
        con.copy(
          authenticated = authed, authenticating = None
        ),
        nodeSet.authenticates.toSeq
      )

    } { node =>
      node._copy(authenticated = auth.map(node.authenticated + _).
        getOrElse(node.authenticated))
    }

    if (!auth.isDefined) ns.copy(authenticates = ns.authenticates - replyTo)
    else ns
  }

  protected def authenticationResponse(response: Response)(check: Response => Either[CommandError, SuccessfulAuthentication]): NodeSet = {
    @inline def event =
      s"Authentication(${response.info.channelId}, ${_nodeSet.toShortString})"

    updateNodeSet(event) { nodeSet =>
      val auth = nodeSet.pickByChannelId(
        response.info.channelId
      ).flatMap(_._2.authenticating)

      auth match {
        case Some(Authenticating(db, user, pass)) => {
          val originalAuthenticate = Authenticate(db, user, pass)
          val authenticated = check(response) match {
            case Right(successfulAuthentication) => {
              AuthRequestsManager.handleAuthResult(
                originalAuthenticate, successfulAuthentication
              )

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

          updateAuthenticate(
            nodeSet,
            response.info.channelId, originalAuthenticate, authenticated
          )
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

  private def nodeInfo(reqAuth: Boolean, node: Node): String = {
    val info = s"connected:${node.connected.size}, channels:${node.connections.size}"

    if (!reqAuth) info else {
      s"authenticated:${node.authenticatedConnections.subject.size}, $info"
    }
  }

  private def pickChannel(request: Request): Try[(Node, Connection)] = {
    val ns = _nodeSet

    request.channelIdHint match {
      case Some(chanId) => ns.pickByChannelId(chanId).map(Success(_)).getOrElse(
        Failure(new ChannelNotFound(s"#chanId", false, internalState))
      )

      case _ => ns.pick(request.readPreference).map(Success(_)).getOrElse {
        val secOk = secondaryOK(request)
        val reqAuth = ns.authenticates.nonEmpty
        val cause: Throwable = if (!secOk) {
          ns.primary match {
            case Some(prim) => new ChannelNotFound(s"Channel not found from the primary node: '${prim.name}' { ${nodeInfo(reqAuth, prim)} } ($supervisor/$name)", true, internalState)

            case _ => new PrimaryUnavailableException(
              supervisor, name, internalState
            )
          }
        } else if (!ns.isReachable) {
          new NodeSetNotReachable(supervisor, name, internalState)
        } else {
          val details = ns.nodes.map { node =>
            s"'${node.name}' [${node.status}] { ${nodeInfo(reqAuth, node)} }"
          }.mkString("; ")

          new ChannelNotFound(s"Channel not found from the nodes: $details ($supervisor/$name); $history", true, internalState)
        }

        Failure(cause)
      }
    }
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

  private[actors] def whenAuthenticating(channelId: Int)(f: Tuple2[Connection, Authenticating] => Connection): NodeSet =
    updateNodeSet(s"Authenticating($channelId)")(
      _.updateConnectionByChannelId(channelId) { connection =>
        connection.authenticating.fold(connection)(authenticating =>
          f(connection -> authenticating))
      }
    )

  // ---

  private def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)

  private[reactivemongo] def connectAll(nodeSet: NodeSet, connecting: (Node, ChannelFuture) => (Node, ChannelFuture) = { _ -> _ }): NodeSet = {
    @annotation.tailrec
    def updateNode(n: Node, before: Vector[Connection], updated: Vector[Connection]): Node = before.headOption match {
      case Some(c) => {
        val con = if (!c.channel.isConnected) c else {
          // Early status normalization,
          // if connectAll take place before ChannelConnected
          c.copy(status = ConnectionStatus.Connected)
        }

        if (con.channel.isConnected ||
          con.status == ConnectionStatus.Connecting) {
          // connected or already trying to connect

          updateNode(n, before.tail, con +: updated)
        } else {
          val upCon = con.copy(status = ConnectionStatus.Connecting)
          val upNode = try {
            connecting(n, upCon.channel.connect(
              new InetSocketAddress(n.host, n.port)
            ))._1
          } catch {
            case reason: Throwable =>
              logger.warn(s"[$lnm] Fails to connect node with channel ${con.channel.getId}: ${n.toShortString}", reason)
              n
          }

          updateNode(upNode, before.tail, upCon +: updated)
        }
      }

      case _ => n._copy(connections = updated.reverse)
    }

    nodeSet.copy(nodes = nodeSet.nodes.map { node =>
      updateNode(node, node.connections, Vector.empty)
    })
  }

  private class IsMasterRequest(
    val node: Node, f: => Unit = ()
  ) { def send() = { f; node } }

  private def requestIsMaster(node: Node): IsMasterRequest =
    node.connected.headOption.fold(new IsMasterRequest(node)) { channel =>
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
        "admin"
      ) // only "admin" DB for the admin command

      val now = System.currentTimeMillis()
      val id = RequestId.isMaster.next

      val updated = if (node.pingInfo.lastIsMasterId == -1) {
        // There is no IsMaster request waiting response for the node:
        // sending a new one
        logger.debug(
          s"[$lnm] Sends a fresh IsMaster request to ${node.toShortString}"
        )

        node._copy(
          pingInfo = node.pingInfo.copy(
            lastIsMasterTime = now,
            lastIsMasterId = id
          )
        )
      } else if ((
        node.pingInfo.lastIsMasterTime + PingInfo.pingTimeout
      ) < now) {

        // The previous IsMaster request is expired
        val msg = s"${node.toShortString} hasn't answered in time to last ping! Please check its connectivity"

        logger.warn(s"[$lnm] $msg")

        // Unregister the pending requests for this node
        val channelIds = node.connections.map(_.channel.getId)
        val wasPrimary = node.status == NodeStatus.Primary
        def error = {
          val cause = new ClosedException(s"$msg ($lnm)")

          if (wasPrimary) {
            new PrimaryUnavailableException(supervisor, name, cause)
          } else cause
        }

        awaitingResponses.retain { (_, awaitingResponse) =>
          if (channelIds contains awaitingResponse.channelID) {
            logger.trace(s"[$lnm] Unregistering the pending request ${awaitingResponse.promise} for ${node.toShortString}'")

            awaitingResponse.promise.failure(error)
            false
          } else true
        }

        // Reset node state
        updateHistory {
          if (wasPrimary) s"PrimaryUnavailable"
          else s"NodeUnavailable($node)"
        }

        node._copy(
          status = NodeStatus.Unknown,
          //connections = Vector.empty,
          authenticated = Set.empty,
          pingInfo = PingInfo(
            lastIsMasterTime = now,
            lastIsMasterId = id
          )
        )
      } else {
        logger.debug(
          s"Do not send isMaster request to already probed ${node.name}"
        )

        node
      }

      new IsMasterRequest(updated, { channel.send(isMaster(id)); () })
    }

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

  private object NoJob extends Cancellable {
    val cancel = false
    val isCancelled = false
  }
}

@deprecated("Internal class: will be made private", "0.11.14")
final class LegacyDBSystem private[reactivemongo] (
    val supervisor: String,
    val name: String,
    val seeds: Seq[String],
    val initialAuthenticates: Seq[Authenticate],
    val options: MongoConnectionOptions
) extends MongoDBSystem with MongoCrAuthentication {

  def newChannelFactory(effect: Unit): ChannelFactory =
    new ChannelFactory(supervisor, name, options)

  @deprecated("Initialize with an explicit supervisor and connection names", "0.11.14")
  def this(s: Seq[String], a: Seq[Authenticate], opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode opts}", s"unknown-${System identityHashCode opts}", s, a, opts)
}

@deprecated("Internal class: will be made private", "0.11.14")
final class StandardDBSystem private[reactivemongo] (
    val supervisor: String,
    val name: String,
    val seeds: Seq[String],
    val initialAuthenticates: Seq[Authenticate],
    val options: MongoConnectionOptions
) extends MongoDBSystem with MongoScramSha1Authentication {

  def newChannelFactory(effect: Unit): ChannelFactory =
    new ChannelFactory(supervisor, name, options)

  @deprecated("Initialize with an explicit supervisor and connection names", "0.11.14")
  def this(s: Seq[String], a: Seq[Authenticate], opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode opts}", s"unknown-${System identityHashCode opts}", s, a, opts)
}

@deprecated("Internal class: will be made private", "0.11.14")
object MongoDBSystem {
  private[actors] val logger =
    LazyLogger("reactivemongo.core.actors.MongoDBSystem")
}

private[reactivemongo] object RequestId {
  // all requestIds [0, 1000[ are for isMaster messages
  val isMaster = new RequestIdGenerator(0, 999)

  // all requestIds [1000, 2000[ are for getnonce messages
  val getNonce = new RequestIdGenerator(1000, 1999) // CR auth

  // all requestIds [2000, 3000[ are for authenticate messages
  val authenticate = new RequestIdGenerator(2000, 2999)

  // all requestIds [3000[ are for common messages
  val common = new RequestIdGenerator(3000, Int.MaxValue - 1)
}
