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

import akka.actor.{ Actor, ActorRef, Cancellable }

import reactivemongo.io.netty.channel.{
  ChannelFuture,
  ChannelFutureListener,
  ChannelId
}
import reactivemongo.io.netty.channel.group.{
  ChannelGroupFuture,
  ChannelGroupFutureListener,
  DefaultChannelGroup
}

import reactivemongo.util.{ LazyLogger, SimpleRing }

import reactivemongo.core.errors.GenericDriverException
import reactivemongo.core.protocol.{
  GetMore,
  Query,
  QueryFlags,
  KillCursors,
  MongoWireVersion,
  Request,
  Response
}
import reactivemongo.core.commands.{
  CommandError,
  FailedAuthentication,
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

  private val requestTracker = new RequestTracker

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()

  @inline implicit def ec = context.system.dispatcher

  // Inner jobs
  private var connectAllJob: Cancellable = NoJob
  private var refreshAllJob: Cancellable = NoJob

  private[reactivemongo] lazy val history =
    new SimpleRing[(Long, String)](options.maxHistorySize)

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
  // <-- monitor

  @inline private def updateHistory(event: String) = {
    val now = System.currentTimeMillis()
    // TODO: nanoTime

    self ! (now -> event)
  }

  private[reactivemongo] def internalState() = {
    val snap = history.synchronized {
      history.toArray()
    }

    new InternalState(snap.foldLeft(Array.empty[StackTraceElement]) {
      case (trace, null) => trace

      case (trace, (time, event)) => new StackTraceElement(
        "reactivemongo", event.asInstanceOf[String],
        s"<time:$time>", -1) +: trace
    })
  }

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

      requestTracker.withAwaiting { (resps, chans) =>
        resps.foreach {
          case (_, r) if (!r.promise.isCompleted) =>
            // fail all requests waiting for a response
            r.promise.failure(new ClosedException(supervisor, name, istate))

          case _ => ( /* already completed */ )
        }

        resps.clear()
        chans.clear()
      }

      done.future.map(_ => ns)
    }
  }

  protected final class OperationHandler(
    logError: Throwable => Unit,
    logSuccess: ChannelId => Unit)
    extends ChannelFutureListener {

    final def operationComplete(op: ChannelFuture): Unit = {
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
    if (connection.authenticating.nonEmpty) {
      //_println(s"Already authenticating $connection")

      connection
    } else auths.headOption match {
      case Some(nextAuth) =>
        if (connection.isAuthenticated(nextAuth.db, nextAuth.user)) {
          authenticateConnection(connection, auths.tail)
        } else {
          sendAuthenticate(connection, nextAuth)
        }

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

  private def updateNodeSetOnDisconnect(channelId: ChannelId)(
    f: (Boolean, NodeSet) => NodeSet): NodeSet = {
    @inline def event =
      s"ChannelDisconnected($channelId, ${_nodeSet.toShortString})"

    @volatile var updated: Int = 0

    val res = updateNodeSet(event) { ns =>
      val updSet = ns.updateNodeByChannelId(channelId) { n =>
        collectConnections(n) {
          case other if (other.channel.id != channelId) =>
            other // keep connections for other channels unchanged

          case con => {
            if (con.channel.isOpen) { // can still be reused
              updated = 1
              con.copy(status = ConnectionStatus.Disconnected)
            } else {
              updated = 2 // delayed
              con.copy(status = ConnectionStatus.Connecting)
            }
          }
        }
      }

      f(updated > 0, updSet)
    }

    if (updated == 2) {
      scheduler.scheduleOnce(options.reconnectDelayMS.milliseconds) {
        val reEvent =
          s"ChannelReconnecting($channelId, ${_nodeSet.toShortString})"

        updateNodeSet(reEvent) {
          _.updateNodeByChannelId(channelId) { n =>
            collectConnections(n) {
              case other if (other.channel.id != channelId) =>
                other // keep connections for other channels unchanged

              case _ =>
                n.createConnection(channelFactory, self)
            }
          }
        }

        ()
      }
    }

    res
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

  /* !! Should not use `requestTracker.withAwaiting` */
  private def retry(req: AwaitingResponse): Option[AwaitingResponse] = {
    val onError = failureOrLog(req.promise, _: Throwable) { cause =>
      error(s"Fails to retry '${req.request.op}' (channel #${req.channelID})", cause)
    }

    req.retriable(requestRetries).flatMap { renew =>
      foldNodeConnection(req.request)({ error =>
        onError(error)
        None
      }, { (node, con) =>
        val awaiting = renew(con.channel.id)

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
    case (time: Long, event: String) =>
      history.synchronized {
        history.enqueue(time -> event)
      }

    case RegisterMonitor => {
      debug(s"Register monitor $sender")

      monitors += sender

      // In case the NodeSet status has already been updated ...
      // TODO: Test

      val ns = nodeSetLock.synchronized { this._nodeSet }

      if (ns.isReachable) {
        sender ! new SetAvailable(ns.protocolMetadata, ns.name)
        debug("The node set is available")
      }

      ns.primary.foreach { prim =>
        if (ns.authenticates.nonEmpty && prim.authenticated.isEmpty) {
          debug(s"The node set is available (${prim.names}); Waiting authentication: ${prim.authenticated}")
        } else {
          sender ! new PrimaryAvailable(ns.protocolMetadata, ns.name)

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

          if (connectedCon == 0) {
            stopWhenDisconnected("Processing", Close)
          }

          listener.foreach(_.poolShutdown(supervisor, name))
        }

        case Failure(cause) => warn("Fails to Close", cause)
      }
    }

    case req @ RequestMakerExpectingResponse(maker, _) => {
      val reqId = RequestId.common.next

      debug(s"Received a request expecting a response ($reqId): $req")

      val request = maker(reqId)

      foldNodeConnection(request)(req.promise.failure(_), { (node, con) =>
        if (request.op.expectsResponse) {
          requestTracker.withAwaiting { (resps, chans) =>
            resps += reqId -> AwaitingResponse(
              request, con.channel.id, req.promise,
              isGetLastError = false,
              isMongo26WriteOp = req.isMongo26WriteOp)

            chans += con.channel.id

            //println(s"_reserve: ${con.channel.id}")

            trace(s"Registering awaiting response for requestID $reqId on channel #${con.channel.id}, awaitingResponses: $resps")
          }
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
    }

    case ChannelConnected(channelId) => {
      val statusInfo = _nodeSet.toShortString

      trace(s"Channel #$channelId is connected; NodeSet status: $statusInfo")

      updateNodeSet(s"ChannelConnected($channelId, $statusInfo)") { ns =>
        ns.updateByChannelId(channelId)(
          _.copy(status = ConnectionStatus.Connected)) { node =>
            requestIsMaster(node).send()
          }
      }

      ()
    }

    case ChannelDisconnected(chanId) => {
      updateNodeSetOnDisconnect(chanId) {
        case (false, onDisconnect) => {
          debug(s"Unavailable channel #${chanId} is already unattached")
          onDisconnect
        }

        case (_, onDisconnect) => {
          trace(s"Channel #$chanId is unavailable")

          val nodeSetWasReachable = onDisconnect.isReachable
          val primaryWasAvailable = onDisconnect.primary.isDefined

          val ns = onDisconnect.updateNodeByChannelId(chanId) { n =>
            val node = {
              if (n.pingInfo.channelId.exists(_ == chanId)) {
                // #cch2: The channel used to send isMaster/ping request is
                // the one disconnected there

                debug(s"Discard pending isMaster ping: used channel #${chanId} is disconnected")

                n._copy(pingInfo = PingInfo())
              } else {
                n
              }
            }

            if (node.connected.isEmpty) {
              // If there no longer established connection
              trace(s"Unset the node status on disconnect (#${chanId})")

              node._copy(status = NodeStatus.Unknown)
            } else {
              node
            }
          }

          val retried = Map.newBuilder[Int, AwaitingResponse]

          // TODO: Retry isMaster?

          requestTracker.withAwaiting { (resps, chans) =>
            chans -= chanId

            val retriedChans = Set.newBuilder[ChannelId]

            resps.retain { (_, awaitingResponse) =>
              if (awaitingResponse.channelID == chanId) {
                retry(awaitingResponse) match {
                  case Some(awaiting) => {
                    trace(s"Retrying to await response for requestID ${awaiting.requestID} on channel #${awaiting.channelID}: $awaiting")

                    retried += awaiting.requestID -> awaiting
                    retriedChans += awaiting.channelID
                  }

                  case _ => {
                    debug(s"Completing response for '${awaitingResponse.request.op}' with error='socket disconnected' (channel #$chanId)")

                    failureOrLog(awaitingResponse.promise, SocketDisconnected)(
                      err => warn(
                        s"Socket disconnected (channel #$chanId)", err))
                  }
                }

                false
              } else true
            }

            chans ++= retriedChans.result()
            resps ++= retried.result()
          }

          if (!ns.isReachable) {
            if (nodeSetWasReachable) {
              warn("The entire node set is unreachable, is there a network problem?")

              broadcastMonitors(PrimaryUnavailable)
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

          trace(s"Channel #$chanId is released")

          ns
        }
      }

      ()
    }

    case IsMasterResponse(response) => onIsMaster(response)

    case err @ Response.CommandError(_, _, _, cause) if (
      RequestId.authenticate accepts err) => {
      // Successful authenticate responses go to `authReceive` then

      warn("Fails to authenticate", cause)

      updateNodeSet(s"AuthenticationFailure(${err.info._channelId})") { ns =>
        handleAuthResponse(ns, err)(
          Left(FailedAuthentication(cause.getMessage)))
      }

      ()
    }
  }

  val closing: Receive = {
    case (time: Long, event: String) =>
      history.synchronized {
        history.enqueue(time -> event)
      }

    case RegisterMonitor => { monitors += sender; () }

    case req @ ExpectingResponse(promise) => {
      debug(s"Received an expecting response request during closing process: $req, completing its promise with a failure")

      promise.failure(new ClosedException(supervisor, name, internalState))
      ()
    }

    case msg @ ChannelDisconnected(channelId) => {
      updateNodeSetOnDisconnect(channelId) { (_, ns) => ns }

      stopWhenDisconnected("Closing$$ChannelDisconnected", msg)
    }

    case msg @ ChannelConnected(channelId) => {
      warn(s"SPURIOUS $msg (ignored, channel closed)")

      // updateNodeSetOnDisconnect(channelId)

      updateNodeSet("Closing$$ChannelConnected") {
        _.updateNodeByChannelId(channelId) {
          collectConnections(_) {
            case other if (other.channel.id != channelId) =>
              other

            case con => {
              con.channel.close()
              con
            }
          }
        }
      }

      ()
    }

    case Close(src) =>
      warn(s"Already closing... ignore Close request from $src")

    case other =>
      debug(s"Unhandled message: $other")
  }

  // any other response
  private val fallback: Receive = {
    case response: Response if RequestId.common accepts response =>
      requestTracker.withAwaiting { (resps, chans) =>
        resps.get(response.header.responseTo) match {
          case Some(AwaitingResponse(
            _, chanId, promise, isGetLastError, isMongo26WriteOp)) => {

            trace(s"Got a response from ${response.info._channelId} to ${response.header.responseTo}! Will give back message=$response to promise ${System.identityHashCode(promise)}")

            resps -= response.header.responseTo
            chans -= chanId

            //println(s"_release: $chanId")

            response match {
              case cmderr: Response.CommandError =>
                promise.success(cmderr); ()

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
                  trace(s"Received a response to a MongoDB2.6 Write Op")

                  import reactivemongo.bson.lowlevel._
                  import reactivemongo.core.netty.ChannelBufferReadableBuffer

                  val fields = {
                    val reader = new LowLevelBsonDocReader(
                      new ChannelBufferReadableBuffer(response.documents))

                    reader.fieldStream
                  }
                  val okField = fields.find(_.name == "ok")

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

  private def onIsMaster(response: Response): Unit = {
    import reactivemongo.api.BSONSerializationPack
    import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits

    val isMaster = BSONSerializationPack.readAndDeserialize(
      response, BSONIsMasterCommandImplicits.IsMasterResultReader)

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
                val pingTime =
                  System.currentTimeMillis() - node.pingInfo.lastIsMasterTime

                node.pingInfo.copy(
                  ping = pingTime,
                  lastIsMasterTime = 0,
                  lastIsMasterId = -1,
                  channelId = None)

              } else node.pingInfo

            val nodeStatus = isMaster.status

            val authenticating =
              if (!nodeStatus.queryable || nodeSet.authenticates.isEmpty) {
                node
              } else {
                authenticateNode(node, nodeSet.authenticates)
              }

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

            trace(s"Node refreshed from isMaster: ${node.toShortString}")

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

        trace(s"Discovered ${discoveredNodes.size} nodes: ${discoveredNodes mkString ", "}")

        val upSet = prepared.copy(
          name = isMaster.replicaSet.map(_.setName),
          version = isMaster.replicaSet.map { _.setVersion.toLong },
          nodes = prepared.nodes ++ discoveredNodes)

        chanNode.fold(upSet) { node =>
          if (upSet.authenticates.nonEmpty && node.authenticated.isEmpty) {
            debug(s"The node set is available (${node.names}); Waiting authentication: ${node.authenticated}")

          } else {
            if (!nodeSetWasReachable && upSet.isReachable) {
              debug("The node set is now available")

              broadcastMonitors(
                new SetAvailable(upSet.protocolMetadata, upSet.name))

              updateHistory(s"SetAvailable(${nodeSet.toShortString})")
            }

            @inline def wasPrimNames: Seq[String] =
              wasPrimary.toSeq.flatMap(_.names)

            if (upSet.primary.exists(n => !wasPrimNames.contains(n.name))) {
              val newPrim = upSet.primary.map(_.name)

              debug(s"The primary is now available: ${newPrim.mkString}")

              broadcastMonitors(new PrimaryAvailable(
                upSet.protocolMetadata, upSet.name))

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

  private[reactivemongo] def onPrimaryUnavailable(): Unit = {
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

  protected final def handleAuthResponse(nodeSet: NodeSet, resp: Response)(
    check: => Either[CommandError, SuccessfulAuthentication]): NodeSet = {

    val chanId = resp.info._channelId
    val authed = check
    val authenticate = Promise[Authenticate]()

    val updSet = nodeSet.updateNodeByChannelId(chanId) { node =>
      node.pickConnectionByChannelId(chanId).flatMap(_.authenticating) match {
        case Some(Authenticating(db, user, pass)) => {
          val originalAuthenticate = Authenticate(db, user, pass)

          authenticate.success(originalAuthenticate)

          val authenticated: Option[Authenticated] = authed match {
            case Right(successfulAuthentication) => {
              AuthRequestsManager.handleAuthResult(
                originalAuthenticate, successfulAuthentication)

              if (nodeSet.isReachable) {
                debug("The node set is now authenticated")

                broadcastMonitors(
                  new SetAvailable(nodeSet.protocolMetadata, nodeSet.name))

                updateHistory(s"SetAvailable(${nodeSet.toShortString})")
              }

              if (nodeSet.primary.isDefined) {
                debug("The primary is now authenticated")

                broadcastMonitors(
                  new PrimaryAvailable(nodeSet.protocolMetadata, nodeSet.name))

                updateHistory(s"PrimaryAvailable(${nodeSet.toShortString})")
              } else if (nodeSet.isReachable) {
                warn(s"""The node set is authenticated, but the primary is not available: ${nodeSet.name} -> ${nodeSet.nodes.map(_.names) mkString ", "}""")
              }

              Some(Authenticated(db, user))
            }

            case Left(error) => {
              AuthRequestsManager.handleAuthResult(originalAuthenticate, error)

              Option.empty[Authenticated]
            }
          }

          authenticated match {
            case Some(auth) => node.updateByChannelId(chanId)(
              { con =>
                authenticateConnection(
                  con.copy(
                    authenticating = None,
                    authenticated = con.authenticated + auth),
                  nodeSet.authenticates)
              }) {
                _._copy(authenticated = node.authenticated + auth)
              }

            case _ => node.updateByChannelId(chanId)(
              _.copy(authenticating = None))(identity /* unchanged */ )
          }
        }

        case _ => {
          warn(s"No pending authentication matching the response channel #${chanId}: $resp")

          node
        }
      }
    }

    if (authed.isLeft) { // there is an auth error
      authenticate.future.value match {
        case Some(Success(replyTo)) => { // original requested auth
          updSet.copy(authenticates = updSet.authenticates - replyTo)
        }

        case r => {
          warn(s"Original authenticate not resolved: $r")
          updSet
        }
      }
    } else updSet
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
      s"authenticated:${node.authenticatedConnections.subject.size}, authenticating: ${node.connected.filter(_.authenticating.isDefined).size}, $info"
    }
  }

  private def pickChannel(request: Request): Try[(Node, Connection)] = {
    val ns = _nodeSet

    request.channelIdHint match {
      case Some(chanId) => ns.pickByChannelId(chanId).map(Success(_)).getOrElse(
        Failure(new ChannelNotFound(s"#${chanId}", false, internalState)))

      case _ => requestTracker.withAwaiting { (_, chans) =>
        val accept = { c: Connection => !chans.contains(c.channel.id) }

        ns.pick(request.readPreference, accept).map(Success(_)).getOrElse {
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

  // ---

  private def broadcastMonitors(message: AnyRef) = monitors.foreach(_ ! message)

  private lazy val connectTimeout = options.failoverStrategy.initialDelay

  private[reactivemongo] def connectAll(nodeSet: NodeSet): NodeSet = {
    @annotation.tailrec
    def updateNode(n: Node, before: Vector[Connection], updated: Vector[Connection]): Node = before.headOption match {
      case Some(c) => {
        //println(s"${n.name} -> ${c.channel} = ${c.status} ? ${c.channel.isActive}")

        if (c.status == ConnectionStatus.Connecting ||
          c.status == ConnectionStatus.Connected) {

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
      updateNode(node, node.connections, Vector.empty)
    })
  }

  private class IsMasterRequest(val node: Node, f: => Unit = ()) {
    def send() = { f; node }
  }

  private def requestIsMaster(node: Node): IsMasterRequest = {
    /*_println(s"requestIsMaster? ${node.connected.headOption.mkString}"); */ node.connected.headOption.fold(new IsMasterRequest(node)) { con =>
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

      val updated = if (node.pingInfo.lastIsMasterId == -1) {
        // There is no IsMaster request waiting response for the node:
        // sending a new one
        debug(s"Prepares a fresh IsMaster request to ${node.toShortString} (channel #${con.channel.id})")

        node._copy(
          pingInfo = node.pingInfo.copy(
            lastIsMasterTime = now,
            lastIsMasterId = id,
            channelId = Some(con.channel.id)))

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

          if (!wasPrimary) cause
          else new PrimaryUnavailableException(supervisor, name, cause)
        }

        requestTracker.withAwaiting { (resps, chans) =>
          resps.retain { (_, awaitingResponse) =>
            chans -= awaitingResponse.channelID

            //println(s"_release: ${awaitingResponse.channelID}")

            if (channelIds contains awaitingResponse.channelID) {
              trace(s"Unregistering the pending request ${awaitingResponse.promise} for ${node.toShortString}'")

              awaitingResponse.promise.failure(error)
              false
            } else true
          }
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
            ping = Long.MaxValue,
            lastIsMasterTime = now,
            lastIsMasterId = id,
            channelId = con.channel.id))

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
  }

  @inline private def scheduler = context.system.scheduler

  @deprecated(message = "Will be made private", since = "0.11.10")
  def allChannelGroup(nodeSet: NodeSet): DefaultChannelGroup = {
    val result = new DefaultChannelGroup(
      reactivemongo.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE)

    for (node <- nodeSet.nodes) {
      for (connection <- node.connections) result.add(connection.channel)
    }

    result
  }

  // Manage authenticate request other than the initial ones
  private object AuthRequestsManager {
    private var authRequests =
      Map.empty[Authenticate, List[Promise[SuccessfulAuthentication]]]

    def addAuthRequest(request: AuthRequest): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      val found = authRequests.get(request.authenticate)

      authRequests = authRequests + (request.authenticate -> (
        request.promise :: found.getOrElse(Nil)))

      authRequests
    }

    private def withRequest[T](authenticate: Authenticate)(
      f: Promise[SuccessfulAuthentication] => T): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] = {
      // TODO: synchronized

      authRequests.get(authenticate) match {
        case Some(found) => {
          authRequests = authRequests - authenticate

          found.foreach(f)
        }

        case _ => {
          debug(s"No pending authentication is matching response: $authenticate")

          authRequests
        }
      }

      authRequests
    }

    def handleAuthResult(
      authenticate: Authenticate,
      result: SuccessfulAuthentication) =
      withRequest(authenticate)(_.success(result))

    def handleAuthResult(authenticate: Authenticate, result: Throwable) =
      withRequest(authenticate)(_.failure(result))
  }

  private object NoJob extends Cancellable {
    val cancel = false
    val isCancelled = false
  }

  // --- Logging ---

  protected final lazy val lnm = s"$supervisor/$name" // log naming

  @inline protected def _println(msg: => String) = println(s"[$lnm] $msg")

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

final class StandardDBSystemWithX509 private[reactivemongo] (
  val supervisor: String,
  val name: String,
  val seeds: Seq[String],
  val initialAuthenticates: Seq[Authenticate],
  val options: MongoConnectionOptions) extends MongoDBSystem with MongoX509Authentication {

  def newChannelFactory(effect: Unit): ChannelFactory =
    new ChannelFactory(supervisor, name, options)
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

private[actors] final class RequestTracker {
  import scala.collection.mutable.{ LinkedHashMap, Set }

  private val awaitingResponses = LinkedHashMap.empty[Int, AwaitingResponse]

  private val awaitingChannels = Set.empty[ChannelId]

  private[actors] def withAwaiting[T](f: Function2[LinkedHashMap[Int, AwaitingResponse], Set[ChannelId], T]): T =
    awaitingResponses.synchronized {
      f(awaitingResponses, awaitingChannels)
    }
}
