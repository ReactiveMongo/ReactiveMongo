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

import scala.concurrent.{ Await, Future, Promise }

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import shaded.google.common.collect.{ EvictingQueue, Queues }

import akka.actor.{ Actor, ActorRef, Cancellable }

import shaded.netty.channel.{
  ChannelFuture,
  ChannelFutureListener,
  ChannelId
}
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

  protected final val logger =
    LazyLogger("reactivemongo.core.actors.MongoDBSystem")

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

  private var channelFactory: ChannelFactory = null //newChannelFactory({})
  @volatile private var closingFactory = false

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
  private val history = EvictingQueue.create[(Long, String)](historyMax)
  private[reactivemongo] val syncHistory = Queues.synchronizedQueue(history)

  private type NodeSetHandler = (String, NodeSetInfo, NodeSet) => Unit
  private val nodeSetUpdated: NodeSetHandler =
    listener.fold[NodeSetHandler]({ (event: String, _, _) =>
      updateHistory(event); ()
    }) { l =>
      { (event: String, previous: NodeSetInfo, updated: NodeSet) =>
        updateHistory(event)

        scheduler.scheduleOnce(1.second) {
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

  //initNodeSet()
  ////nodeSetUpdated(s"Init(${_nodeSet.toShortString})", null, _nodeSet)
  // <-- monitor

  @inline private def updateHistory(event: String) =
    syncHistory.offer(System.currentTimeMillis() -> event)

  private[reactivemongo] def internalState() = new InternalState(
    history.toArray(Array.fill[(Long, String)](historyMax)(null)).
      foldLeft(Array.empty[StackTraceElement]) {
        case (trace, null) => trace

        case (trace, (time, event)) => new StackTraceElement(
          "reactivemongo", event.asInstanceOf[String],
          s"<time:$time>", -1) +: trace
      })

  private[reactivemongo] def getNodeSet = _nodeSet // For test purposes

  /** On start or restart. */
  private def initNodeSet(): NodeSet = {
    val ns = NodeSet(None, None, seeds.map(seed => Node(seed, NodeStatus.Unknown, Vector.empty, Set.empty, None, ProtocolMetadata.Default).createNeededChannels(channelFactory, self, 1)).toVector, initialAuthenticates.toSet)

    debug(s"Initial node set: ${ns.toShortString}")

    _nodeSet = ns
    _setInfo = ns.info

    ns
  }

  private def release(): Future[NodeSet] = {
    if (closingFactory) {
      Future.successful(_nodeSet)
    } else {
      closingFactory = true
      val factory = channelFactory

      debug("Releasing the MongoDBSystem resources")

      // cancel all jobs
      connectAllJob.cancel()
      refreshAllJob.cancel()

      val ns = updateNodeSet("Release")(_.updateAll { node =>
        node._copy(connections = node.connected)

        // Only keep already connected connection:
        // - prevent to activate other connection
        // - know which connections to be closed
      })

      // close all connections
      val done = Promise[Unit]()
      val listener = new ChannelGroupFutureListener {
        def operationComplete(future: ChannelGroupFuture): Unit =
          factory.release(done)
      }

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

      done.future.map(_ => ns)
    }
  }

  private class OperationHandler(
    logError: Throwable => Unit,
    logSuccess: ChannelId => Unit)
    extends ChannelFutureListener {

    final def operationComplete(op: ChannelFuture) {
      if (!op.isSuccess) {
        logError(op.cause)
      } else {
        logSuccess(op.channel.id)
      }
    }
  }

  // Akka hooks

  override def preStart(): Unit = {
    info("Starting the MongoDBSystem")

    channelFactory = newChannelFactory({})
    closingFactory = false

    val ns = connectAll(initNodeSet())

    nodeSetUpdated(s"Start(${ns.toShortString})", null, ns)

    //updateNodeSet(s"Start(${_nodeSet})") { ns =>
    //_nodeSet = connectAll(_nodeSet)
    /*, { (node, con) =>
      val req = requestIsMaster(node)

      con.addListener(new ChannelFutureListener {
        def operationComplete(op: ChannelFuture) = {
          //logger.debug
          println(s"[$lnm] Asking whether ${node.name} is master (channel #${op.channel.id})")

          req.send()
        }
      })

      req.node -> con
     })*/

    // Prepare the period jobs
    val refreshInterval = options.monitorRefreshMS.milliseconds

    connectAllJob = {
      val ms = options.monitorRefreshMS / 5
      val interval = if (ms < 100) 100.milliseconds else refreshInterval

      scheduler.schedule(interval, interval, self, ConnectAll)
    }

    refreshAllJob = scheduler.schedule(
      refreshInterval, refreshInterval, self, RefreshAll)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    val details = message.map { m => s": $m" }.getOrElse("")
    val msg = s"Restarting the MongoDBSystem${details}"

    warn(msg, reason)

    super.preRestart(reason, message)

    scheduler.scheduleOnce(Duration.Zero) {
      // ... so the monitors are restored after restart
      debug("Restore monitor registrations after restart")

      monitors.foreach { mon =>
        self.tell(RegisterMonitor, mon)
      }

      ()
    }

    ()
  }

  override def postStop(): Unit = {
    info("Stopping the MongoDBSystem")

    Await.result(release(), options.monitorRefreshMS.milliseconds)

    ()
  }

  override def postRestart(reason: Throwable): Unit = {
    info(s"MongoDBSystem is restarted: $reason")

    nodeSetUpdated("Restart", null, _nodeSet)

    /*
    // Renew the channel factory, as releaseExternalResources in close@postStop
    channelFactory = newChannelFactory({})
    closingFactory = false

    updateNodeSet(s"Restart(${_nodeSet})")(_ => initNodeSet())
 */

    super.postRestart(reason) // will call preStart()
  }

  // ---

  protected def sendAuthenticate(
    connection: Connection,
    authentication: Authenticate): Connection

  @annotation.tailrec
  protected final def authenticateConnection(connection: Connection, auths: Set[Authenticate]): Connection = {
    if (connection.authenticating.nonEmpty) connection
    else auths.headOption match {
      case Some(nextAuth) =>
        if (connection.isAuthenticated(nextAuth.db, nextAuth.user)) {
          authenticateConnection(connection, auths.tail)
        } else sendAuthenticate(connection, nextAuth)

      case _ => connection
    }
  }

  private final def authenticateNode(node: Node, auths: Set[Authenticate]): Node = node._copy(connections = node.connections.map {
    case connection if connection.status == ConnectionStatus.Connected =>
      authenticateConnection(connection, auths)

    case connection => connection
  })

  private def collectConnections(node: Node)(collector: PartialFunction[Connection, Connection]): Node = {
    val connections = node.connections.collect(collector)

    if (connections.isEmpty) {
      debug(s"Node '${node.name}' is no longer connected; Fallback to Unknown status")

      node._copy(
        status = NodeStatus.Unknown,
        connections = Vector.empty,
        authenticated = Set.empty)

    } else {
      node._copy(
        connections = connections,
        authenticated = connections.toSet.flatMap(
          (_: Connection).authenticated))
    }
  }

  private def stopWhenDisconnected[T](state: String, msg: T): Unit = {
    val remainingConnections = _nodeSet.nodes.foldLeft(0) {
      _ + _.connected.size
    }

    if (logger.isDebugEnabled) {
      val disconnected = _nodeSet.nodes.foldLeft(0) { (open, node) =>
        open + node.connections.count(_.status == ConnectionStatus.Disconnected)
      }

      debug(s"Received $msg @ $state; remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}")
    }

    if (remainingConnections == 0) {
      monitors.foreach(_ ! Closed)

      debug("Stopping on disconnection")

      if (context == null) {
        warn("Do not stop context as already released")
      } else {
        context.stop(self)
      }
    }
  }

  private def updateNodeSetOnDisconnect(channelId: ChannelId): (Boolean, NodeSet) = {
    @inline def event =
      s"ChannelDisconnected($channelId, ${_nodeSet.toShortString})"

    @volatile var updated = false

    val ns = updateNodeSet(event)(_.updateNodeByChannelId(channelId) {
      collectConnections(_) {
        case other if (other.channel.id != channelId) =>
          other // keep connections for other channels unchanged

        case con =>
          updated = true
          con.copy(status = ConnectionStatus.Disconnected)
      }
    })

    updated -> ns
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
      error(s"Fails to retry '${req.request.op}' (channel #${req.channelID})", cause)
    }

    req.retriable(requestRetries).flatMap { newReq =>
      foldNodeConnection(req.request)({ error =>
        onError(error)
        None
      }, { (node, con) =>
        val reqId = req.requestID
        val awaiting = newReq(con.channel.id)

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
      debug(s"Register monitor $sender")

      monitors += sender

      // In case the NodeSet status has already been updated ...
      // TODO: Test

      val ns = nodeSetLock.synchronized { this._nodeSet }

      if (ns.isReachable) {
        sender ! SetAvailable(ns.protocolMetadata)
        debug("The node set is available")
      }

      ns.primary.foreach { prim =>
        if (ns.authenticates.nonEmpty && prim.authenticated.isEmpty) {
          debug(s"The node set is available (${prim.names}); Waiting authentication: ${prim.authenticated}")
        } else {
          sender ! PrimaryAvailable(ns.protocolMetadata)

          debug(s"The primary is available: $prim")
        }
      }

      ()
    }

    case Close(src) => {
      debug(s"Received Close request from $src, closing connections")

      // moving to closing state
      context become closing

      release().onComplete {
        case Success(ns) => {
          val connectedCon = ns.nodes.foldLeft(0) { _ + _.connected.size }

          if (connectedCon == 0) stopWhenDisconnected("Processing", Close)

          listener.foreach(_.poolShutdown(supervisor, name))
        }

        case Failure(cause) => warn("Fails to Close", cause)
      }
    }

    case req @ RequestMaker(_, _, _, _) => {
      trace(s"Transmiting a request: $req")

      val r = req(RequestId.common.next)
      pickChannel(r).map(_._2.send(r))

      ()
    }

    case req @ RequestMakerExpectingResponse(maker, _) => {
      val reqId = RequestId.common.next

      trace(s"Received a request expecting a response ($reqId): $req")

      val request = maker(reqId)

      foldNodeConnection(request)(req.promise.failure(_), { (node, con) =>
        if (request.op.expectsResponse) {
          awaitingResponses += reqId -> AwaitingResponse(
            request, con.channel.id, req.promise,
            isGetLastError = false,
            isMongo26WriteOp = req.isMongo26WriteOp)

          trace(s"Registering awaiting response for requestID $reqId, awaitingResponses: $awaitingResponses")
        } else {
          trace(s"NOT registering awaiting response for requestID $reqId")
        }

        con.send(request).addListener(new OperationHandler(
          error(s"Fails to send request expecting response $reqId", _),
          { chanId =>
            trace(s"Request $reqId successful on channel #${chanId}")
          }))
      })

      ()
    }

    case req @ CheckedWriteRequestExpectingResponse(_) => {
      debug("Received a checked write request")

      val checkedWriteRequest = req.checkedWriteRequest
      val reqId = RequestId.common.next
      val (request, writeConcern) = {
        val tuple = checkedWriteRequest()
        tuple._1(reqId) -> tuple._2(reqId)
      }
      val onError = failureOrLog(req.promise, _: Throwable) { cause =>
        error(s"Fails to register a request: ${request.op}", cause)
      }

      foldNodeConnection(request)(onError, { (node, con) =>
        awaitingResponses += reqId -> AwaitingResponse(
          request, con.channel.id, req.promise,
          isGetLastError = true, isMongo26WriteOp = false).withWriteConcern(writeConcern)

        trace(s"Registering awaiting response for requestID $reqId, awaitingResponses: $awaitingResponses")

        con.send(request, writeConcern).addListener(new OperationHandler(
          error(s"Fails to send checked write request $reqId", _),
          { chanId =>
            trace(s"Request $reqId successful on channel #${chanId}")
          }))
      })

      ()
    }

    case ConnectAll => { // monitor
      val statusInfo = _nodeSet.toShortString

      updateNodeSet(s"ConnectAll($statusInfo)") { ns =>
        connectAll(ns.createNeededChannels(
          channelFactory, self, options.nbChannelsPerNode))
      }

      debug(s"ConnectAll Job running... Status: $statusInfo")
    }

    case RefreshAll => {
      val statusInfo = _nodeSet.toShortString

      updateNodeSet(s"RefreshAll($statusInfo)")(_.updateAll { node =>
        trace(s"Try to refresh ${node.name}")

        requestIsMaster(node).send()
      })

      debug(s"RefreshAll Job running... Status: $statusInfo")

      ()
    }

    case ChannelConnected(channelId) => {
      val statusInfo = _nodeSet.toShortString

      trace(s"Channel #$channelId is connected; NodeSet status: $statusInfo")

      updateNodeSet(s"ChannelConnected($channelId, $statusInfo)") { ns =>
        //println(s"_connected_ns: ${ns.nodes.flatMap(_.connections).map(_.channel)}")

        ns.updateByChannelId(channelId)(
          _.copy(status = ConnectionStatus.Connected)) { n =>
            //println(s"_connected_node: ${n.name}")
            requestIsMaster(n).send()
          }
      }

      ()
    }

    case ChannelDisconnected(channelId) =>
      updateNodeSetOnDisconnect(channelId) match {
        case (false, _) => {
          debug(s"Unavailable channel #${channelId} is already unattached")
        }

        case (_, ns) => {
          trace(s"Channel #$channelId is unavailable")

          val nodeSetWasReachable = _nodeSet.isReachable
          val primaryWasAvailable = _nodeSet.primary.isDefined

          val retried = Map.newBuilder[Int, AwaitingResponse]

          // TODO: Retry isMaster?

          awaitingResponses.retain { (_, awaitingResponse) =>
            if (awaitingResponse.channelID == channelId) {
              retry(awaitingResponse) match {
                case Some(awaiting) => {
                  trace(s"Retrying to await response for requestID ${awaiting.requestID}: $awaiting")
                  retried += awaiting.requestID -> awaiting
                }

                case _ => {
                  debug(s"Completing response for '${awaitingResponse.request.op}' with error='socket disconnected' (channel #$channelId)")

                  failureOrLog(awaitingResponse.promise, SocketDisconnected)(
                    err => warn(
                      s"Socket disconnected (channel #$channelId)", err))
                }
              }

              false
            } else true
          }

          awaitingResponses ++= retried.result()

          if (!ns.isReachable) {
            if (nodeSetWasReachable) {
              warn("The entire node set is unreachable, is there a network problem?")

              broadcastMonitors(SetUnavailable)
            } else {
              debug("The entire node set is still unreachable, is there a network problem?")
            }
          } else if (!ns.primary.isDefined) {
            if (primaryWasAvailable) {
              warn("The primary is unavailable, is there a network problem?")

              broadcastMonitors(PrimaryUnavailable)
            } else {
              debug("The primary is still unavailable, is there a network problem?")
            }
          }

          trace(s"Channel #$channelId is released")
        }
      }

    case IsMasterResponse(response) => onIsMaster(response)
  }

  val closing: Receive = {
    case req @ RequestMaker(_, _, _, _) =>
      error(s"Received a non-expecting response request during closing process: $req")

    case RegisterMonitor => { monitors += sender; () }

    case req @ ExpectingResponse(promise) => {
      debug(s"Received an expecting response request during closing process: $req, completing its promise with a failure")

      promise.failure(new ClosedException(supervisor, name, internalState))
      ()
    }

    case msg @ ChannelDisconnected(channelId) => {
      updateNodeSetOnDisconnect(channelId)

      stopWhenDisconnected("Closing$$ChannelDisconnected", msg)
    }

    case msg @ ChannelConnected(channelId) => {
      warn(s"SPURIOUS $msg (ignored, channel closed)")

      updateNodeSetOnDisconnect(channelId)
      ()
    }

    case Close(src) =>
      warn(s"Already closing... ignore Close request from $src")

    case other =>
      debug(s"Unhandled message: $other")
  }

  // any other response
  private val fallback: Receive = {
    case response: Response if RequestId.common accepts response => {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _, promise, isGetLastError, isMongo26WriteOp)) => {
          trace(s"Got a response from ${response.info._channelId} to ${response.header.responseTo}! Will give back message=$response to promise ${System.identityHashCode(promise)}")

          awaitingResponses -= response.header.responseTo

          response match {
            case cmderr: Response.CommandError =>
              promise.success(cmderr); ()

            case _ => {
          if (response.error.isDefined) {
            debug(s"{${response.header.responseTo}} sending a failure... (${response.error.get})")

            case _ => {
              if (response.error.isDefined) { // TODO: Option pattern matching
                debug(s"{${response.header.responseTo}} sending a failure... (${response.error.get})")

                if (response.error.get.isNotAPrimaryError) {
                  onPrimaryUnavailable()
                }

                promise.failure(response.error.get)
                ()
              } else if (isGetLastError) {
                debug(s"{${response.header.responseTo}} it's a getlasterror")

                // todo, for now rewinding buffer at original index
                //import reactivemongo.api.commands.bson.BSONGetLastErrorImplicits.LastErrorReader

                lastError(response).fold({ e =>
                  error(s"Error deserializing LastError message #${response.header.responseTo}", e)
                  promise.failure(new RuntimeException(s"Error deserializing LastError message #${response.header.responseTo} ($lnm)", e))
                }, { lastError =>
                  if (lastError.inError) {
                    trace(s"{${response.header.responseTo}} sending a failure (lasterror is not ok)")

                    if (lastError.isNotAPrimaryError) onPrimaryUnavailable()

                    promise.failure(lastError)
                  } else {
                    trace(s"{${response.header.responseTo}} sending a success (lasterror is ok)")

                    response.documents.readerIndex(response.documents.readerIndex)

                    promise.success(response)
                  }
                })

                ()
              } else if (isMongo26WriteOp) {
                // TODO - logs, bson
                // MongoDB 26 Write Protocol errors
                logger.trace(
                  s"[$lnm] Received a response to a MongoDB2.6 Write Op")

            trace(s"{${response.header.responseTo}} ok field is: $okField")
            val processedOk = okField.collect {
              case BooleanField(_, v) => v
              case IntField(_, v)     => v != 0
              case DoubleField(_, v)  => v != 0
            }.getOrElse(false)

            if (processedOk) {
              trace(s"{${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
              promise.success(response)
              ()
            } else {
              debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] processedOk is false! sending an error")

              val notAPrimary = fields.find(_.name == "errmsg").exists {
                case errmsg @ LazyField(0x02, _, buf) =>
                  debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg!")
                  buf.readString == "not a primary"
                case errmsg =>
                  debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg but not interesting!")
                  false
              }

              if (notAPrimary) {
                debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] not a primary error!")
                onPrimaryUnavailable()
              }

              promise.failure(GenericDriverException("not ok"))
              ()
            }
          } else {
            trace(s"{${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
            promise.success(response)
            ()
          }
        }

        case _ => error(s"Oups. ${response.header.responseTo} not found! complete message is $response")
      }
    }

    case request @ AuthRequest(authenticate, _) => {
      debug(s"New authenticate request $authenticate")
      // For [[MongoConnection.authenticate]]

      AuthRequestsManager.addAuthRequest(request)

      updateNodeSet(authenticate.toString) { ns =>
        // Authenticate the entire NodeSet
        val authenticates = ns.authenticates + authenticate

        ns.copy(authenticates = authenticates).updateAll {
          case node @ Node(_, status, _, _, _, _, _, _) if status.queryable =>
            authenticateNode(node, authenticates)

          case node => node
        }
      }

      ()
    }

    case a => error(s"Not supported: $a")
  }

  override lazy val receive: Receive =
    processing.orElse(authReceive).orElse(fallback)

  // ---

  private def onIsMaster(response: Response) {
    import reactivemongo.api.BSONSerializationPack
    import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits
    import reactivemongo.api.commands.Command

    val isMaster = Command.deserialize(BSONSerializationPack, response)(
      BSONIsMasterCommandImplicits.IsMasterResultReader)

    trace(s"IsMaster response: $isMaster")

    val updated = {
      val respTo = response.header.responseTo
      @inline def event =
        s"IsMaster(${respTo}, ${_nodeSet.toShortString})"

      updateNodeSet(event) { nodeSet =>
        val nodeSetWasReachable = nodeSet.isReachable
        val wasPrimary: Option[Node] = nodeSet.primary
        @volatile var chanNode = Option.empty[Node]

        // Update the details of the node corresponding to the response chan
        val prepared =
          nodeSet.updateNodeByChannelId(response.info._channelId) { node =>
            val pingInfo =
              if (node.pingInfo.lastIsMasterId == respTo) {
                // No longer waiting response for isMaster,
                // reset the pending state, and update the ping time
                node.pingInfo.copy(
                  ping =
                    System.currentTimeMillis() - node.pingInfo.lastIsMasterTime,
                  lastIsMasterTime = 0, lastIsMasterId = -1)
              } else node.pingInfo

            val nodeStatus = isMaster.status

            val authenticating =
              if (!nodeStatus.queryable || nodeSet.authenticates.isEmpty) {
                node
              } else authenticateNode(node, nodeSet.authenticates)

            val meta = ProtocolMetadata(
              MongoWireVersion(isMaster.minWireVersion),
              MongoWireVersion(isMaster.maxWireVersion),
              isMaster.maxBsonObjectSize,
              isMaster.maxMessageSizeBytes,
              isMaster.maxWriteBatchSize)

            val an = authenticating._copy(
              status = nodeStatus,
              pingInfo = pingInfo,
              tags = isMaster.replicaSet.flatMap(_.tags),
              protocolMetadata = meta,
              isMongos = isMaster.isMongos)

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

        trace(s"Discovered nodes: $discoveredNodes")

        val upSet = prepared.copy(nodes = prepared.nodes ++ discoveredNodes)

        chanNode.fold(upSet) { node =>
          if (upSet.authenticates.nonEmpty && node.authenticated.isEmpty) {
            debug(s"The node set is available (${node.names}); Waiting authentication: ${node.authenticated}")

          } else {
            if (!nodeSetWasReachable && upSet.isReachable) {
              debug("The node set is now available")

              broadcastMonitors(SetAvailable(upSet.protocolMetadata))

              updateHistory(s"SetAvailable(${nodeSet.toShortString})")
            }

            @inline def wasPrimNames: Seq[String] =
              wasPrimary.toSeq.flatMap(_.names)

            if (upSet.primary.exists(n => !wasPrimNames.contains(n.name))) {
              val newPrim = upSet.primary.map(_.name)

              debug(s"The primary is now available: ${newPrim.mkString}")

              broadcastMonitors(PrimaryAvailable(upSet.protocolMetadata))

              updateHistory(s"PrimaryAvailable(${nodeSet.toShortString})")
            }
          }

          if (node.status != NodeStatus.Primary) upSet else {
            // Current node is known and primary

            // TODO: Test; 1. an NodeSet with node A & B, with primary A;
            // 2. Receive a isMaster response from B, with primary = B

            upSet.updateAll { n =>
              if (node.names.contains(n.name) && // the node itself
                n.status != node.status) {
                // invalidate node status on status conflict
                warn(s"Invalid node status for ${node.name}; Fallback to Unknown status")

                n._copy(status = NodeStatus.Unknown)
              } else n
            }
          }
        }
      }
    }

    scheduler.scheduleOnce(Duration.Zero) {
      @inline def event = s"ConnectAll$$IsMaster(${response.header.responseTo}, ${updated.toShortString})"

      updateNodeSet(event) { ns =>
        connectAll(ns.createNeededChannels(
          channelFactory, self, options.nbChannelsPerNode))
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

  private def updateAuthenticate(nodeSet: NodeSet, channelId: ChannelId, replyTo: Authenticate, auth: Option[Authenticated]): NodeSet = {
    val ns = nodeSet.updateByChannelId(channelId) { con =>
      val authed = auth.map(con.authenticated + _).getOrElse(con.authenticated)

      authenticateConnection(
        con.copy(authenticated = authed, authenticating = None),
        nodeSet.authenticates)

    } { node =>
      node._copy(authenticated = auth.map(node.authenticated + _).
        getOrElse(node.authenticated))
    }

    if (!auth.isDefined) ns.copy(authenticates = ns.authenticates - replyTo)
    else ns
  }

  protected def authenticationResponse(response: Response)(check: Response => Either[CommandError, SuccessfulAuthentication]): NodeSet = {
    @inline def event =
      s"Authentication(${response.info._channelId}, ${_nodeSet.toShortString})"

    updateNodeSet(event) { nodeSet =>
      val auth = nodeSet.pickByChannelId(
        response.info._channelId).flatMap(_._2.authenticating)

      auth match {
        case Some(Authenticating(db, user, pass)) => {
          val originalAuthenticate = Authenticate(db, user, pass)
          val authenticated = check(response) match {
            case Right(successfulAuthentication) => {
              AuthRequestsManager.handleAuthResult(
                originalAuthenticate, successfulAuthentication)

              if (nodeSet.isReachable) {
                debug("The node set is now authenticated")

                broadcastMonitors(SetAvailable(nodeSet.protocolMetadata))

                updateHistory(s"SetAvailable(${nodeSet.toShortString})")
              }

              if (nodeSet.primary.isDefined) {
                debug("The primary is now authenticated")

                broadcastMonitors(PrimaryAvailable(nodeSet.protocolMetadata))

                updateHistory(s"PrimaryAvailable(${nodeSet.toShortString})")
              } else if (nodeSet.isReachable) {
                warn(s"""The node set is authenticated, but the primary is not available: ${nodeSet.name} -> ${nodeSet.nodes.map(_.names) mkString ", "}""")
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
            response.info._channelId, originalAuthenticate, authenticated)
        }

        case res => {
          warn(s"Authentication result: $res")
          nodeSet
        }
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
        Failure(new ChannelNotFound(s"#chanId", false, internalState)))

      case _ => ns.pick(request.readPreference).map(Success(_)).getOrElse {
        val secOk = secondaryOK(request)
        lazy val reqAuth = ns.authenticates.nonEmpty
        val cause: Throwable = if (!secOk) {
          ns.primary match {
            case Some(prim) => {
              val details = s"'${prim.name}' { ${nodeInfo(reqAuth, prim)} } ($supervisor/$name)"

              if (!reqAuth || prim.authenticated.nonEmpty) {
                new ChannelNotFound(s"No active channel can be found to the primary node: $details", true, internalState)
              } else {
                new NotAuthenticatedException(
                  s"No authenticated channel for the primary node: $details")
              }
            }

            case _ => new PrimaryUnavailableException(
              supervisor, name, internalState)
          }
        } else if (!ns.isReachable) {
          new NodeSetNotReachable(supervisor, name, internalState)
        } else {
          // Node set is reachable, secondary is ok, but no channel found
          val (authed, msgs) = ns.nodes.foldLeft(
            false -> Seq.empty[String]) {
              case ((authed, msgs), node) =>
                val msg = s"'${node.name}' [${node.status}] { ${nodeInfo(reqAuth, node)} }"

                (authed || node.authenticated.nonEmpty) -> (msg +: msgs)
            }
          val details = s"""${msgs.mkString(", ")} ($supervisor/$name)"""

          if (!reqAuth || authed) {
            new ChannelNotFound(s"No active channel with '${request.readPreference}' found for the nodes: $details", true, internalState)
          } else {
            new NotAuthenticatedException(s"No authenticated channel: $details")
          }
        }

        Failure(cause)
      }
    }
  }

  private def foldNodeConnection[T](request: Request)(e: Throwable => T, f: (Node, Connection) => T): T = pickChannel(request) match {
    case Failure(error) => {
      trace(s"No channel for request: $request")
      e(error)
    }

    case Success((node, connection)) => {
      trace(s"Sending request (${request.requestID}) expecting response by connection $connection of node ${node.name}: $request")

      f(node, connection)
    }
  }

  private[actors] def whenAuthenticating(channelId: ChannelId)(f: Tuple2[Connection, Authenticating] => Connection): NodeSet =
    updateNodeSet(s"Authenticating($channelId)")(
      _.updateConnectionByChannelId(channelId) { connection =>
        connection.authenticating.fold(connection)(authenticating =>
          f(connection -> authenticating))
      })

  // ---

  private def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)

  private lazy val connectTimeout = options.failoverStrategy.initialDelay

  private[reactivemongo] def connectAll(nodeSet: NodeSet): NodeSet = {
    @annotation.tailrec
    def updateNode(n: Node, before: Vector[Connection], updated: Vector[Connection]): Node = before.headOption match {
      case Some(c) => {
        //println(s"-> c.status = ${c.status} ? ${c.channel.isActive}")

        if (c.status == ConnectionStatus.Connecting ||
          c.status == ConnectionStatus.Connected) {

          if (c.status == ConnectionStatus.Connecting && c.channel.isActive) {
            // #cch1: Can happen for the first channel become active so quick,
            // it happens even before the ChannelFactory.initChannel ends.

            scheduler.scheduleOnce(Duration.Zero) {
              // Schedule/deferred notification than immediate one,
              // to avoid synchronization issue around NodeSet update

              //println("_cch1")
              self ! ChannelConnected(c.channel.id)
            }
          }

          updateNode(n, before.tail, c +: updated)
        } else {
          val upStatus: ConnectionStatus = {
            if (c.channel.isActive) {
              ConnectionStatus.Connected
            } else {
              try {
                val asyncSt = Promise[ConnectionStatus]()
                val nodeAddr = new InetSocketAddress(n.host, n.port)

                c.channel.connect(nodeAddr).addListener(new OperationHandler(
                  { cause =>
                    error(s"Fails to connect channel #${c.channel.id}", cause)

                    asyncSt.success(ConnectionStatus.Disconnected)

                    ()
                  },
                  { _ =>
                    asyncSt.success(ConnectionStatus.Connecting)

                    ()
                  }))

                Await.result(asyncSt.future, connectTimeout)
              } catch {
                case NonFatal(reason) =>
                  warn(s"Fails to connect node with channel #${c.channel.id}: ${n.toShortString}", reason)

                  c.status
              }
            }
          }

          val upCon = c.copy(status = upStatus)
          val upNode = {
            if (c.status == upStatus) n
            else n.updateByChannelId(c.channel.id)(_ => upCon)(identity)
          }

          updateNode(upNode, before.tail, upCon +: updated)
        }
      }

      case _ => {
        val cons = updated.reverse

        n._copy(
          connections = cons,
          authenticated = cons.toSet.flatMap((_: Connection).authenticated))
      }
    }

    //println(s"_connect_all: ${nodeSet.nodes}")

    nodeSet.copy(nodes = nodeSet.nodes.map { node =>
      //println(s"_connect_node = $node")

      updateNode(node, node.connections, Vector.empty)
    })
  }

  private class IsMasterRequest(val node: Node, f: => Unit = ()) {
    def send() = { f; node }
  }

  private def requestIsMaster(node: Node): IsMasterRequest =
    node.connected.headOption.fold(new IsMasterRequest(node)) { con =>
      import reactivemongo.api.BSONSerializationPack
      import reactivemongo.api.commands.bson.{
        BSONIsMasterCommandImplicits,
        BSONIsMasterCommand
      }, BSONIsMasterCommand.IsMaster
      import reactivemongo.api.commands.Command

      val id = RequestId.isMaster.next
      val (isMaster, _) = Command.buildRequestMaker(BSONSerializationPack)(
        IsMaster(id.toString),
        BSONIsMasterCommandImplicits.IsMasterWriter,
        ReadPreference.primaryPreferred,
        "admin") // only "admin" DB for the admin command

      val now = System.currentTimeMillis()

      // 724#TODO2: ? Add chanId on PingInfo to be able to retry IsMaster
      // if sent with a chan detected then as ChannelDisconnected
      // as for the awaitingResponses

      val updated = if (node.pingInfo.lastIsMasterId == -1) {
        // There is no IsMaster request waiting response for the node:
        // sending a new one
        debug(s"Prepares a fresh IsMaster request to ${node.toShortString} (channel #${con.channel.id})")

        node._copy(
          pingInfo = node.pingInfo.copy(
            lastIsMasterTime = now,
            lastIsMasterId = id))

      } else if ((
        node.pingInfo.lastIsMasterTime + PingInfo.pingTimeout) < now) {

        // The previous IsMaster request is expired
        val msg = s"${node.toShortString} hasn't answered in time to last ping! Please check its connectivity"

        warn(msg)

        // Unregister the pending requests for this node
        val channelIds = node.connections.map(_.channel.id)
        val wasPrimary = node.status == NodeStatus.Primary
        def error = {
          val cause = new ClosedException(s"$msg ($lnm)")

          if (wasPrimary) {
            new PrimaryUnavailableException(supervisor, name, cause)
          } else cause
        }

        awaitingResponses.retain { (_, awaitingResponse) =>
          if (channelIds contains awaitingResponse.channelID) {
            trace(s"Unregistering the pending request ${awaitingResponse.promise} for ${node.toShortString}'")

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
            lastIsMasterId = id))

      } else {
        debug(s"Do not prepare a isMaster request to already probed ${node.name}")

        node
      }

      new IsMasterRequest(updated, {
        con.send(isMaster(id)).addListener(new OperationHandler(
          error(s"Fails to send a isMaster request to ${node.name} (channel #${con.channel.id})", _),
          { chanId =>
            trace(s"isMaster request successful on channel #${chanId}")
          }))
        ()
      })
    }

  @inline private def scheduler = context.system.scheduler

  @deprecated(message = "Will be made private", since = "0.11.10")
  def allChannelGroup(nodeSet: NodeSet): DefaultChannelGroup = {
    val result = new DefaultChannelGroup(
      shaded.netty.util.concurrent.GlobalEventExecutor.INSTANCE)

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

      error("Authentication failure", result)

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

  // --- Logging ---

  protected val lnm = s"$supervisor/$name" // log naming

  @inline protected def debug(msg: => String) = logger.debug(s"[$lnm] $msg")

  @inline protected def debug(msg: => String, cause: Throwable) =
    logger.debug(s"[$lnm] $msg", cause)

  @inline protected def info(msg: => String) = logger.info(s"[$lnm] $msg")

  @inline protected def trace(msg: => String) = logger.trace(s"[$lnm] $msg")

  @inline protected def warn(msg: => String) = logger.warn(s"[$lnm] $msg")

  @inline protected def warn(msg: => String, cause: Throwable) =
    logger.warn(s"[$lnm] $msg", cause)

  @inline protected def error(msg: => String) = logger.error(s"[$lnm] $msg")

  @inline protected def error(msg: => String, cause: Throwable) =
    logger.error(s"[$lnm] $msg", cause)
}

@deprecated("Internal class: will be made private", "0.11.14")
final class LegacyDBSystem private[reactivemongo] (
  val supervisor: String,
  val name: String,
  val seeds: Seq[String],
  val initialAuthenticates: Seq[Authenticate],
  val options: MongoConnectionOptions) extends MongoDBSystem with MongoCrAuthentication {

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
  val options: MongoConnectionOptions) extends MongoDBSystem with MongoScramSha1Authentication {

  def newChannelFactory(effect: Unit): ChannelFactory =
    new ChannelFactory(supervisor, name, options)

  @deprecated("Initialize with an explicit supervisor and connection names", "0.11.14")
  def this(s: Seq[String], a: Seq[Authenticate], opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode opts}", s"unknown-${System identityHashCode opts}", s, a, opts)
}

@deprecated("Internal class: will be made private", "0.11.14")
object MongoDBSystem {
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

case class AuthRequest(
  authenticate: Authenticate,
  promise: Promise[SuccessfulAuthentication] = Promise()) {
  def future: Future[SuccessfulAuthentication] = promise.future
}
