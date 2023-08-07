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

import java.nio.channels.ClosedChannelException

import java.util.concurrent.TimeoutException

import java.net.InetSocketAddress

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import scala.collection.{ Map => IMap }
import scala.collection.immutable.ListSet
import scala.collection.mutable.{ Map => MMap }

import scala.concurrent.{ Await, ExecutionContext, Future, Promise }

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

import reactivemongo.core.ClientMetadata
import reactivemongo.core.errors.{ CommandException, GenericDriverException }
import reactivemongo.core.netty.ChannelFactory
import reactivemongo.core.nodeset.{
  Authenticate,
  Authenticated,
  Authenticating,
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeSetInfo,
  NodeStatus,
  PingInfo
}
import reactivemongo.core.protocol.{
  GetMore,
  KillCursors,
  Message => OpMsg,
  MongoWireVersion,
  ProtocolMetadata,
  Query,
  QueryFlags,
  Request,
  Response
}

import reactivemongo.api.{
  MongoConnectionOptions,
  PackSupport,
  ReadPreference,
  Serialization,
  WriteConcern
}
import reactivemongo.api.bson.BSONDocumentReader
import reactivemongo.api.bson.collection.BSONSerializationPack
import reactivemongo.api.commands.{
  CommandKind,
  FailedAuthentication,
  LastErrorFactory,
  SuccessfulAuthentication,
  UpsertedFactory
}

import com.github.ghik.silencer.silent
import external.reactivemongo.ConnectionListener
import reactivemongo.actors.actor.{ Actor, ActorRef, Cancellable }
import reactivemongo.util.{ LazyLogger, SimpleRing }

/** Main actor that processes the requests. */
@SuppressWarnings(Array("NullAssignment"))
private[reactivemongo] trait MongoDBSystem extends Actor { selfSystem =>
  import scala.concurrent.duration._
  import Exceptions._
  import Commands.{ LastError, Upserted }

  protected type Pack = Serialization.Pack

  protected val pack: Pack = Serialization.internalSerializationPack

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

  private[reactivemongo] var channelFactory: ChannelFactory =
    null // newChannelFactory({})

  @volatile private var closingFactory = false

  private[reactivemongo] lazy val clientMetadata: ClientMetadata =
    new ClientMetadata(options.appName getOrElse lnm)

  private val listener: Option[ConnectionListener] = {
    val cl = ConnectionListener()
    cl.foreach(_.poolCreated(options, supervisor, name))
    cl
  }

  private val requestTracker = new RequestTracker

  private val monitors = scala.collection.mutable.ListBuffer[ActorRef]()

  @inline implicit def ec: ExecutionContext = context.system.dispatcher

  // Inner jobs
  private var connectAllJob: Cancellable = NoJob
  private var refreshAllJob: Cancellable = NoJob

  private[reactivemongo] lazy val history =
    new SimpleRing[(Long, String)](options.maxHistorySize)

  private type NodeSetHandler = (String, NodeSetInfo, NodeSet) => Unit

  @SuppressWarnings(Array("NullParameter"))
  private val nodeSetUpdated: NodeSetHandler =
    listener.fold[NodeSetHandler]({ (event: String, _, _) =>
      updateHistory(event); ()
    }) { l =>
      { (event: String, previous: NodeSetInfo, updated: NodeSet) =>
        updateHistory(event)

        if (
          context != null && context.system != null /* Akka workaround */
        ) {
          scheduler.scheduleOnce(1.second) {
            requestTracker.withAwaiting { (responses, channels) =>
              @SuppressWarnings(Array("UnsafeTraversableMethods"))
              def maxAwaitingPerChannel = {
                if (channels.isEmpty) 0
                else channels.map(_._2).max
              }

              _setInfo = updated.info
                .withAwaitingRequests(responses.size, maxAwaitingPerChannel)
            }

            l.nodeSetUpdated(supervisor, name, previous, _setInfo)
          }
        }

        ()
      }
    }

  @inline private def userConnectionsPerNode = options.nbChannelsPerNode

  // monitor -->
  private lazy val heartbeatFrequency =
    options.heartbeatFrequencyMS.milliseconds

  private val pingTimeout: Long = options.heartbeatFrequencyMS * 1000000L

  private val maxNonQueryableTime: Long = // in nanos
    options.maxNonQueryableHeartbeats * pingTimeout

  @inline private def formatNanos(ns: Long): String = {
    if (ns < 1000L) s"${ns.toString}ns"
    else if (ns < 100000000L) s"${(ns / 1000000L).toString}ms"
    else s"${(ns / 1000000000L).toString}s"
  }

  private lazy val maxNonQueryableTimeRepr: String =
    formatNanos(maxNonQueryableTime)

  private val nodeSetLock = new Object {}

  private[reactivemongo] var _nodeSet: NodeSet = null
  private var _setInfo: NodeSetInfo = null
  // <-- monitor

  @inline private def updateHistory(event: String): Unit =
    self ! (System.nanoTime() -> event)

  private[reactivemongo] def internalState() = {
    val snap = history.synchronized {
      history.toArray()
    }

    new InternalState(snap.foldLeft(Array.empty[StackTraceElement]) {
      case (trace, (time, event: String)) =>
        new StackTraceElement(
          "reactivemongo",
          event,
          s"<time:$time>",
          -1
        ) +: trace
    })
  }

  private[reactivemongo] def getNodeSet = _nodeSet // For test purposes

  private lazy val signalingTimeoutMS: Int = {
    if (options.heartbeatFrequencyMS <= 0) {
      0
    } else {
      val v = options.heartbeatFrequencyMS.toLong * 1.5D

      if (v <= Int.MaxValue) v.toInt else 0
    }
  }

  /** On start or restart. */
  private def initNodeSet(): Try[NodeSet] = {
    val nanow = System.nanoTime()
    val seedNodeSet = new NodeSet(
      None,
      None,
      seeds
        .map(seed =>
          new Node(
            seed,
            Set.empty,
            NodeStatus.Unknown,
            Vector.empty,
            Set.empty,
            tags = Map.empty[String, String],
            ProtocolMetadata.Default,
            PingInfo(),
            false,
            nanow
          )
        )
        .toVector,
      initialAuthenticates.toSet,
      ListSet.empty
    )

    debug(s"Initial node set: ${seedNodeSet.toShortString}")

    seedNodeSet.updateAll { n =>
      n.createSignalingConnection(
        channelFactory,
        signalingTimeoutMS,
        self
      ) match {
        case Success(node) => node

        case Failure(cause) => {
          warn(
            s"Fails to create the signaling channel: ${n.toShortString}",
            cause
          )

          n
        }
      }
    }.createUserConnections(
      channelFactory,
      options.maxIdleTimeMS,
      self,
      1
    ) match {
      case inited @ Success(ns) => {
        _nodeSet = ns
        _setInfo = ns.info

        inited
      }

      case result @ Failure(cause) => {
        error("Fails to init the NodeSet", cause)

        _nodeSet = seedNodeSet // anyway
        _setInfo = _nodeSet.info

        result
      }
    }
  }

  private def release(
      parentEvent: String,
      timeout: FiniteDuration
    ): Future[NodeSet] = {

    if (closingFactory) {
      Future.successful(_nodeSet)
    } else {
      closingFactory = true
      val factory = channelFactory

      debug("Releasing the MongoDBSystem resources")

      // cancel all jobs
      connectAllJob.cancel()
      refreshAllJob.cancel()

      val ns = updateNodeSet(s"${parentEvent}$$Release")(_.updateAll { node =>
        node.copy(connections = node.connected)

        // Only keep already connected connection:
        // - prevent to activate other connection
        // - know which connections to be closed
      })

      // close all connections
      val done = Promise[Unit]()
      def releaseFactory(): Unit = factory.release(done, timeout)

      if (ns.nodes.exists(_.connections.nonEmpty)) {
        val relistener = new ChannelGroupFutureListener {
          def operationComplete(future: ChannelGroupFuture) = releaseFactory()
        }

        allChannelGroup(ns).close().addListener(relistener)
      } else {
        releaseFactory()
      }

      // fail all requests waiting for a response
      val istate = internalState()

      requestTracker.withAwaiting { (resps, chans) =>
        resps.foreach {
          case (_, r) if (!r.promise.isCompleted) =>
            // fail all requests waiting for a response
            failureOrLog(
              r.promise,
              new ClosedException(supervisor, name, istate)
            )(err => warn("Already completed request on close", err))

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

    def operationComplete(op: ChannelFuture): Unit = {
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

    initNodeSet().foreach { inited =>
      val ns = connectAll(inited)

      nodeSetUpdated(s"Start(${ns.toShortString})", null, ns)
    }

    // Prepare the periodic jobs
    val interval = {
      val ms = options.heartbeatFrequencyMS / 5
      if (ms < 100) 100.milliseconds else heartbeatFrequency
    }

    @silent(".*schedule .*deprecated.*") @inline def schedule[T](msg: T) =
      scheduler.schedule(interval, interval, self, msg)

    refreshAllJob = schedule(RefreshAll)
    connectAllJob = schedule(ConnectAll)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    val details = message.fold("") { m => s": $m" }
    val msg = s"Restarting the MongoDBSystem${details}"

    warn(msg, reason)

    super.preRestart(reason, message)

    scheduler.scheduleOnce(Duration.Zero) {
      // ... so the monitors are restored after restart
      debug("Restore monitor registrations after restart")

      @SuppressWarnings(Array("UnnecessaryConversion"))
      val ms = monitors.toSeq // clone as local var before clearing

      monitors.clear()

      ms.foreach { mon => self.tell(RegisterMonitor, mon) }

      ()
    }

    ()
  }

  override def postStop(): Unit = {
    info("Stopping the MongoDBSystem")

    try {
      Await.result(release("PostStop", heartbeatFrequency), heartbeatFrequency)
    } catch {
      case NonFatal(_: TimeoutException) =>
        warn("Fails to stop in a timely manner")
    }

    ()
  }

  @SuppressWarnings(Array("NullParameter"))
  override def postRestart(reason: Throwable): Unit = {
    info("MongoDBSystem is restarted", reason)

    nodeSetUpdated("Restart", null, _nodeSet)

    super.postRestart(reason) // will call preStart()
  }

  // ---

  protected def sendAuthenticate(
      connection: Connection,
      authentication: Authenticate
    ): Connection

  @annotation.tailrec
  protected final def authenticateConnection(
      connection: Connection,
      auths: Set[Authenticate]
    ): Connection = {
    if (connection.authenticating.nonEmpty) {
      // _println(s"Already authenticating $connection")

      connection
    } else
      auths.headOption match {
        case Some(nextAuth) =>
          if (connection.isAuthenticated(nextAuth.db, nextAuth.user)) {
            authenticateConnection(connection, auths.drop(1))
          } else {
            sendAuthenticate(connection, nextAuth)
          }

        case _ => connection
      }
  }

  private final def authenticateNode(
      node: Node,
      auths: Set[Authenticate]
    ): Node = node.copy(connections = node.connections.map {
    case connection
        if (!connection.signaling &&
          connection.status == ConnectionStatus.Connected) =>
      authenticateConnection(connection, auths)

    case connection => connection
  })

  @SuppressWarnings(Array("NullParameter"))
  private def stopWhenDisconnected[T](state: String, msg: T): Unit = {
    val remainingConnections = _nodeSet.nodes.foldLeft(0) {
      _ + _.connected.size
    }

    if (logger.isDebugEnabled) {
      val disconnected = _nodeSet.nodes.foldLeft(0) { (open, node) =>
        open + node.connections.count(_.status == ConnectionStatus.Disconnected)
      }

      debug(
        s"Received $msg @ $state; remainingConnections = $remainingConnections, disconnected = $disconnected, connected = ${remainingConnections - disconnected}"
      )
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

  private def updateNodeSetOnDisconnect(
      channelId: ChannelId
    )(f: (Boolean, NodeSet) => NodeSet
    ): NodeSet = {
    @inline def event =
      s"ChannelDisconnected($channelId, ${_nodeSet.toShortString})"

    @volatile var updated: Int = 0

    val res = updateNodeSet(event) { ns =>
      val updSet = ns.updateConnectionByChannelId(channelId) { con =>
        if (con.channel.isOpen) { // #cch4: can still be reused
          updated = 1
          con.copy(status = ConnectionStatus.Disconnected)
        } else {
          updated = 2 // delayed
          con.copy(status = ConnectionStatus.Connecting)
        }
      }

      f(updated > 0, updSet)
    }

    if (updated == 2) {
      scheduler.scheduleOnce(heartbeatFrequency) {
        val reEvent =
          s"ChannelReconnecting($channelId, ${_nodeSet.toShortString})"

        updateNodeSet(reEvent) { ns =>
          ns.updateNodeByChannelId(channelId) { n =>
            // #cch5: replace channel

            n.updateByChannelId(channelId)({ con =>
              n.createConnection(
                channelFactory, {
                  if (con.signaling) signalingTimeoutMS
                  else options.maxIdleTimeMS
                },
                self,
                con.signaling
              ) match {
                case Success(c) =>
                  c

                case Failure(cause) => {
                  val msg = s"Cannot create connection for $n"

                  if (!closingFactory) warn(msg, cause)
                  else info(msg)

                  con // .copy(status = ConnectionStatus.Disconnected)
                }
              }
            })(identity)
          }
        }

        ()
      }
    }

    res
  }

  private val lastError: Response => Either[Throwable, LastError] = {
    val reader: BSONDocumentReader[LastError] =
      BSONDocumentReader[LastError] { doc =>
        new LastError(
          ok = doc.booleanLike("ok").getOrElse(false),
          errmsg = doc.string("err"),
          code = doc.int("code"),
          lastOp = doc.long("lastOp"),
          n = doc.int("n").getOrElse(0),
          singleShard = doc.string("singleShard"),
          updatedExisting = doc.booleanLike("updatedExisting").getOrElse(false),
          upserted = doc.getAsOpt[Upserted]("upserted"),
          wnote = doc.get("wnote").flatMap {
            case reactivemongo.api.bson.BSONString("majority") =>
              Some(WriteConcern.Majority)

            case reactivemongo.api.bson.BSONString(tagSet) =>
              Some(WriteConcern.TagSet(tagSet))

            case reactivemongo.api.bson.BSONInteger(acks) =>
              Some(WriteConcern.WaitForAcknowledgments(acks))

            case _ => Option.empty
          },
          wtimeout = doc.booleanLike("wtimeout").getOrElse(false),
          waited = doc.int("waited"),
          wtime = doc.int("wtime"),
          writeErrors = Seq.empty,
          writeConcernError = Option.empty
        )
      }

    Response.parse(_: Response).next().asTry[LastError](reader) match {
      case Failure(err) => Left(err)
      case Success(err) => Right(err)
    }
  }

  @inline private def requestRetries = options.failoverStrategy.retries

  private def failureOrLog[T](
      promise: Promise[T],
      cause: Throwable
    )(log: Throwable => Unit
    ): Unit = {
    if (promise.isCompleted) log(cause)
    else { promise.failure(cause); () }
  }

  val SocketDisconnected = new GenericDriverException(
    s"Socket disconnected ($lnm)"
  )

  protected def authReceive: Receive

  private class NodeOrdering(
      chans: IMap[ChannelId, Int])
      extends math.Ordering[Node] {

    def compare(x: Node, y: Node): Int = {
      val xReqs = x.connections.foldLeft(0) { (count, c) =>
        count + chans.getOrElse(c.channel.id, 0)
      }

      val yReqs = y.connections.foldLeft(0) { (count, c) =>
        count + chans.getOrElse(c.channel.id, 0)
      }

      val rdiff = xReqs - yReqs

      if (rdiff != 0) rdiff
      else {
        (x.pingInfo.lastIsMasterTime - y.pingInfo.lastIsMasterTime).toInt
      }
    }
  }

  private def acceptBalancedCon(
      chans: IMap[ChannelId, Int]
    ): Connection => Boolean = { (c: Connection) =>
    val count: Int = chans.getOrElse(c.channel.id, 0)

    !c.signaling && options.maxInFlightRequestsPerChannel.forall(count < _)
  }

  private val processing: Receive = {
    case (time: Long, event: String) => {
      history.synchronized {
        history.enqueue(time -> event)
      }

      ()
    }

    case RegisterMonitor => {
      debug(s"Register monitor ${sender()}")

      monitors += sender()

      // In case the NodeSet status has already been updated ...
      // TODO: Test

      val ns = nodeSetLock.synchronized { this._nodeSet }

      if (ns.isReachable) {
        sender() ! new SetAvailable(ns.protocolMetadata, ns.name, ns.isMongos)

        debug("The node set is available")
      }

      ns.primary.foreach { prim =>
        if (ns.authenticates.nonEmpty && prim.authenticated.isEmpty) {
          debug(s"The node set is available (${prim.names}); Waiting authentication: ${prim.authenticated}")
        } else {
          sender() ! new PrimaryAvailable(
            ns.protocolMetadata,
            ns.name,
            ns.isMongos
          )

          debug(s"The primary is available: $prim")
        }
      }

      ()
    }

    case req @ Close(src) => {
      debug(s"Received Close request from $src, closing connections")

      // moving to closing state
      context become closing

      release("Close", req.timeout).onComplete {
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

    case req @ PickNode(readPref) => {
      val p: Option[(Node, Connection)] = nodeSetLock.synchronized {
        requestTracker.withAwaiting { (_, chans) =>
          val unpriorised = math.ceil(_nodeSet.nodes.size.toDouble / 2D).toInt
          implicit val ord: NodeOrdering = new NodeOrdering(chans)

          _nodeSet.pick(readPref, unpriorised, acceptBalancedCon(chans))
        }
      }

      p match {
        case Some((node, _)) =>
          req.promise.success(node.name)

        case _ =>
          req.promise.failure(
            new ChannelNotFoundException(
              s"No active channel can be found: ${readPref}",
              false,
              internalState()
            )
          )

      }

      ()
    }

    case req: ExpectingResponse => {
      val reqId = RequestIdGenerator.common.next

      debug(s"Received a request expecting a response ($reqId): $req")

      val request = req.requestMaker(reqId)

      foldNodeConnection(_nodeSet, req.pinnedNode, request)(
        { r => req.promise.failure(r); () },
        { (_, con) =>
          if (request.op.expectsResponse) {
            requestTracker.withAwaiting { (resps, chans) =>
              val chanId = con.channel.id

              resps.put(
                reqId,
                new AwaitingResponse(
                  request,
                  chanId,
                  req.promise,
                  isGetLastError = false,
                  pinnedNode = None
                )
              )

              val countBefore = chans.getOrElse(chanId, 0)

              chans.put(chanId, countBefore + 1)

              trace(s"Registering awaiting response for requestID $reqId on channel #${con.channel.id}, awaitingResponses: $resps")
            }
          } else {
            trace(s"NOT registering awaiting response for requestID $reqId")
          }

          con
            .send(request, _nodeSet.compression)
            .addListener(
              new OperationHandler(
                error(s"Fails to send request expecting response $reqId", _),
                { chanId =>
                  trace(s"Request ${request.kind} ($reqId) successfully sent on channel #${chanId}")

                }
              )
            )

          ()
        }
      )
    }

    case ConnectAll => { // monitor
      val statusInfo = _nodeSet.toShortString

      updateNodeSet(s"ConnectAll($statusInfo)") { ns =>
        ns.createUserConnections(
          channelFactory,
          options.maxIdleTimeMS,
          self,
          userConnectionsPerNode
        ) match {
          case Success(upd) => connectAll(upd)

          case Failure(cause) => {
            warn("Fails to create channels for the NodeSet", cause)
            ns
          }
        }
      }

      debug(s"ConnectAll Job running... Status: $statusInfo")
    }

    case RefreshAll => {
      refreshNodeSet(statusInfo => {
        debug(s"RefreshAll Job running... Status: $statusInfo")

        s"RefreshAll($statusInfo)"
      }) { ns =>
        val nanow = System.nanoTime()

        ns.copy(nodes = ns.nodes.filter {
          case Node.Queryable(_) => true

          case n =>
            if ((nanow - n.statusChanged) < maxNonQueryableTime) true
            else {
              warn(s"Node ${n.toShortString} has been not queryable for at least ${maxNonQueryableTimeRepr}; Removing it from the set! Please check configuration and connectivity")

              false
            }
        }).updateAll(_)
      }

      ()
    }

    case ChannelConnected(chanId) => {
      refreshNodeSet(statusInfo => {
        trace(s"Channel #$chanId is connected; NodeSet status: $statusInfo")

        s"ChannelConnected($chanId, $statusInfo)"
      }) { ns =>
        ns.updateByChannelId(chanId)(
          _.copy(status = ConnectionStatus.Connected)
        )(_)
      }

      ()
    }

    case ChannelDisconnected(chanId) => {
      updateNodeSetOnDisconnect(chanId) {
        case (false, ns) => {
          debug(s"Unavailable channel #${chanId} is already unattached")
          ns
        }

        case (_, ns) =>
          onDisconnect(chanId, ns)
      }

      ()
    }

    case response: Response
        if (RequestIdGenerator.isMaster accepts response) => {

      val resp: Option[IsMasterCommand.IsMasterResult] =
        try {
          Option(pack.readAndDeserialize(response, isMasterReader))
        } catch {
          case NonFatal(cause) =>
            warn("Unexpected isMaster response", cause)

            None
        }

      resp.foreach(onIsMaster(response, _))
    }

    case err @ Response.CommandError(_, _, _, cause)
        if (RequestIdGenerator.authenticate accepts err) => {
      // Successful authenticate responses go to `authReceive` then

      warn("Fails to authenticate", cause)

      updateNodeSet(s"AuthenticationFailure(${err.info.channelId})") { ns =>
        handleAuthResponse(ns, err)(
          Left(FailedAuthentication(pack)(cause.getMessage, None, None))
        )
      }

      ()
    }
  }

  val closing: Receive = {
    case (time: Long, event: String) => {
      history.synchronized {
        history.enqueue(time -> event)
      }

      ()
    }

    case RegisterMonitor => { monitors += sender(); () }

    case req: ExpectingResponse => {
      import req.promise

      debug(s"Received an expecting response request during closing process: $req, completing its promise with a failure")

      updateHistory(f"Closing$$$$RejectExpectingResponse")

      promise.failure(new ClosedException(supervisor, name, internalState()))
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
        _.updateConnectionByChannelId(channelId) { con =>
          con.channel.close()
          con
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
    case response: Response if RequestIdGenerator.common accepts response =>
      requestTracker.withAwaiting { (resps, chans) =>
        resps.get(response.header.responseTo) match {
          case Some(AwaitingResponse(_, chanId, promise, isGetLastError)) => {

            trace(s"Got a response from ${response.info.channelId} to ${response.header.responseTo}! Will give back message=$response to promise ${System.identityHashCode(promise)}")

            resps -= response.header.responseTo

            val awaitingBefore = chans.getOrElse(chanId, 0)

            if (awaitingBefore <= 1) {
              // Response corresponds to the last one currently
              // awaited for the specified channel

              chans -= chanId
            } else {
              chans.put(chanId, awaitingBefore - 1)
            }

            response match {
              case cmderr: Response.CommandError =>
                promise.success(cmderr); ()

              case _ =>
                response.error match {
                  case Some(error) => {
                    debug(s"{${response.header.responseTo}} sending a failure... (${error})")

                    if (error.isNotAPrimaryError) {
                      onPrimaryUnavailable(error)
                    }

                    promise.failure(error)
                    ()
                  }

                  case _ if isGetLastError => {
                    debug(
                      s"{${response.header.responseTo}} it's a getlasterror"
                    )

                    lastError(response).fold(
                      { e =>
                        error(
                          s"Error deserializing LastError message #${response.header.responseTo}",
                          e
                        )
                        promise.failure(
                          new GenericDriverException(
                            s"Error deserializing LastError message #${response.header.responseTo} ($lnm)",
                            e
                          )
                        )
                      },
                      { lastError =>
                        if (lastError.inError) {
                          trace(s"{${response.header.responseTo}} sending a failure (lasterror is not ok)")

                          if (lastError.isNotAPrimaryError) {
                            onPrimaryUnavailable(lastError)
                          }

                          promise.failure(lastError)
                        } else {
                          trace(s"{${response.header.responseTo}} sending a success (lasterror is ok)")

                          response.documents.readerIndex(
                            response.documents.readerIndex
                          )

                          promise.success(response)
                        }
                      }
                    )

                    ()
                  }

                  case _ => {
                    trace(s"{${response.header.responseTo}} sending a success!")

                    promise.success(response)
                    ()
                  }
                }
            }
          }

          case _ =>
            info(s"Skip response that does not correspond to an awaiting request #${response.header.responseTo} (was discarded?): $response")
        }
      }

    case request: AuthRequest => {
      import request.authenticate

      debug(s"New authenticate request $authenticate")
      // For [[MongoConnection.authenticate]]

      AuthRequestsManager.addAuthRequest(request)

      updateNodeSet(authenticate.toString) { ns =>
        // Authenticate the entire NodeSet
        val authenticates = ns.authenticates + authenticate

        ns.copy(authenticates = authenticates).updateAll {
          case Node.Queryable(node) =>
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

  /*
   * @param event a function applied on description of the [[NodeSet]] before
   * @param update the update function
   */
  private def refreshNodeSet(
      event: String => String
    )(update: NodeSet => ((Node => Node) => NodeSet)
    ): NodeSet = {
    val statusInfo = _nodeSet.toShortString
    val discardedChannels = MMap.empty[ChannelId, Exception]

    val upd = updateNodeSet(event(statusInfo)) { ns =>
      update(ns) { node =>
        trace(s"Try to refresh ${node.name}")

        requestIsMaster("RefreshNodeSet", node).send() match {
          case (n, Some(err)) => {
            node.connections.foreach { c =>
              discardedChannels.put(c.channel.id, err)
            }

            n
          }

          case (n, _) =>
            n
        }
      }
    }

    scheduler.scheduleOnce(heartbeatFrequency) {
      retryAwaitingOnError(upd, discardedChannels.toMap)
    }

    upd
  }

  @silent("retain") private def retryAwaitingOnError(
      ns: NodeSet,
      discardedChannels: Map[ChannelId, Exception]
    ): Unit =
    requestTracker.withAwaiting { (resps, chans) =>
      val retried = MMap.empty[Int, AwaitingResponse]

      chans --= discardedChannels.keySet

      resps.retain { (_, awaitingResponse) =>
        discardedChannels.get(awaitingResponse.channelID) match {
          case Some(error) => {
            val retriedChans = Set.newBuilder[ChannelId]

            retry(ns, awaitingResponse) match {
              case Some(awaiting) => {
                trace(s"Retrying to await response for requestID ${awaiting.requestID} on channel #${awaiting.channelID}: $awaiting")

                retried.put(awaiting.requestID, awaiting)
                retriedChans += awaiting.channelID
              }

              case _ => {
                val msg = error.getMessage

                debug(s"Completing response for '${awaitingResponse.request.op}' with error '$msg' (channel #${awaitingResponse.channelID})")

                failureOrLog(awaitingResponse.promise, error)(err =>
                  warn(s"$msg (channel #${awaitingResponse.channelID})", err)
                )

              }
            }

            retriedChans.result().foreach { chanId =>
              val countBefore = chans.getOrElseUpdate(chanId, 0)

              chans.put(chanId, countBefore + 1)
            }

            false
          }

          case _ =>
            true
        }
      }

      resps ++= retried.result()

      ()
    }

  private def onDisconnect(chanId: ChannelId, nodeSet: NodeSet): NodeSet = {
    trace(s"Channel #$chanId is unavailable")

    val nodeSetWasReachable = nodeSet.isReachable
    val primaryWasAvailable = nodeSet.primary.isDefined

    val ns = nodeSet.updateNodeByChannelId(chanId) { n =>
      val node = {
        if (n.pingInfo.channelId contains chanId) {
          // #cch2: The channel used to send isMaster/ping request is
          // the one disconnected there

          debug(s"Discard pending isMaster ping: used channel #${chanId} is disconnected")

          n.copy(pingInfo = PingInfo())
        } else {
          n
        }
      }

      if (node.signaling.isEmpty && node.status != NodeStatus.Unknown) {
        // If there no longer established connection
        trace(s"Unset the node status on disconnect (#${chanId})")

        node.copy(
          status = NodeStatus.Unknown,
          statusChanged = System.nanoTime()
        )
      } else {
        node
      }
    }

    retryAwaitingOnError(
      ns,
      Map.empty[ChannelId, Exception].updated(chanId, SocketDisconnected)
    )

    if (!ns.isReachable) {
      if (nodeSetWasReachable) {
        warn("The entire node set is unreachable, is there a network problem?")

        broadcastMonitors(PrimaryUnavailable)
        broadcastMonitors(SetUnavailable)

        updateHistory(s"SetUnavailable(${ns.toShortString})")
      } else {
        debug("The entire node set is still unreachable, is there a network problem?")
      }
    } else if (!ns.primary.isDefined) {
      if (primaryWasAvailable) {
        warn("The primary is unavailable, is there a network problem?")

        broadcastMonitors(PrimaryUnavailable)

        updateHistory(s"OnDisconnect$$PrimaryUnavailable(${ns.toShortString})")
      } else {
        debug("The primary is still unavailable, is there a network problem?")
      }
    }

    trace(s"Channel #$chanId is released")

    ns
  }

  private def retry(
      ns: NodeSet,
      req: AwaitingResponse
    ): Option[AwaitingResponse] = {
    val onError = failureOrLog(req.promise, _: Throwable) { cause =>
      error(
        s"Fails to retry '${req.request.op}' (channel #${req.channelID})",
        cause
      )
    }

    req.retriable(requestRetries).flatMap { renew =>
      foldNodeConnection(ns, req.pinnedNode, req.request)(
        { error =>
          onError(error)
          None
        },
        { (_, con) =>
          val awaiting = renew(con.channel.id)

          awaiting.writeConcern.fold(
            con.send(awaiting.request, ns.compression)
          ) { wc => con.send(awaiting.request, wc, ns.compression) }

          Some(awaiting)
        }
      )
    }
  }

  @SuppressWarnings(Array("VariableShadowing"))
  private object Commands
      extends PackSupport[Pack]
      with LastErrorFactory[Pack]
      with UpsertedFactory[Pack] {
    val pack: selfSystem.pack.type = selfSystem.pack
  }

  private object IsMasterCommand
      extends reactivemongo.api.commands.IsMasterCommand[Pack]

  private lazy val isMasterReader = IsMasterCommand.reader(pack)

  private def onIsMaster(
      response: Response,
      isMaster: IsMasterCommand.IsMasterResult
    ): Unit = {
    trace(s"IsMaster response document: $isMaster")

    val updated = {
      val respTo = response.header.responseTo

      @inline def event =
        s"IsMasterResponse(${isMaster.isMaster}, ${respTo}, ${_nodeSet.toShortString})"

      updateNodeSet(event) { nodeSet =>
        val nodeSetWasReachable = nodeSet.isReachable
        val wasPrimary: Option[Node] = nodeSet.primary
        @volatile var chanNode = Option.empty[Node]
        val nanow = System.nanoTime()

        // Update the details of the node corresponding to the response chan
        import nodeSet.{ updateNodeByChannelId => updateNode }

        val prepared = updateNode(response.info.channelId) { node =>
          if (respTo < node.pingInfo.lastIsMasterId) {
            warn(s"Skip node update for delated response #$respTo < last #${node.pingInfo.lastIsMasterId}")

            node
          } else {
            val pingTime: Long = {
              if (node.pingInfo.lastIsMasterId == respTo) {
                nanow - node.pingInfo.lastIsMasterTime
              } else { // not accurate - fallback
                val expected = node.pingInfo.lastIsMasterId

                warn(
                  s"Received unexpected isMaster from ${node.name} response #$respTo (expected #${if (expected != -1) expected.toString else "<none>"})! Please check connectivity.",
                  internalState()
                )

                Long.MaxValue
              }
            }

            val pingInfo = node.pingInfo.copy(
              ping = pingTime, // latency
              lastIsMasterTime = 0,
              lastIsMasterId = -1,
              channelId = None
            )

            val nodeStatus = isMaster.status

            val authenticating =
              if (!nodeStatus.queryable || nodeSet.authenticates.isEmpty) {
                // Cannot or no need to authenticate the node
                node
              } else {
                authenticateNode(node, nodeSet.authenticates)
              }

            val meta = ProtocolMetadata(
              MongoWireVersion(isMaster.minWireVersion),
              MongoWireVersion(isMaster.maxWireVersion),
              isMaster.maxMessageSizeBytes,
              isMaster.maxBsonObjectSize,
              isMaster.maxWriteBatchSize
            )

            val an = authenticating.copy(
              status = nodeStatus,
              statusChanged = {
                if (authenticating.status == nodeStatus) {
                  authenticating.statusChanged
                } else nanow
              },
              pingInfo = pingInfo,
              tags = isMaster.replicaSet.map(_.tags).getOrElse(Map.empty),
              protocolMetadata = meta,
              isMongos = isMaster.isMongos
            )

            val n = isMaster.replicaSet.fold(an)(rs => an.withAlias(rs.me))
            chanNode = Some(n)

            trace(s"Node refreshed from isMaster: ${n.toShortString}")

            n
          }
        }

        val discoveredNodes = isMaster.replicaSet.toSeq.flatMap {
          _.hosts.collect {
            case host if (!prepared.nodes.exists(_.names contains host)) =>
              // Prepare node for newly discovered host in the RS
              val n = new Node(
                host,
                Set.empty,
                NodeStatus.Uninitialized,
                Vector.empty,
                Set.empty,
                tags = Map.empty[String, String],
                ProtocolMetadata.Default,
                PingInfo(),
                false,
                nanow
              )

              n.createSignalingConnection(
                channelFactory,
                signalingTimeoutMS,
                self
              ) match {
                case Success(upd) =>
                  upd

                case Failure(err) => {
                  warn(
                    s"Fails to create signaling channel for ${n.toShortString}",
                    err
                  )
                  n
                }
              }
          }
        }

        trace(s"""Discovered ${discoveredNodes.size} nodes${discoveredNodes
            .map(_.toShortString)
            .mkString(": [ ", ", ", " ]")}""")

        val upSet = prepared.copy(
          name = isMaster.replicaSet.map(_.setName),
          version = isMaster.replicaSet.map { _.setVersion.toLong },
          nodes = prepared.nodes ++ discoveredNodes,
          compression = options.compressors intersect isMaster.compression
        )

        chanNode.fold(upSet) { node =>
          if (upSet.authenticates.nonEmpty && node.authenticated.isEmpty) {
            debug(s"The node set is available (${node.names}); Waiting authentication: ${node.authenticated}")

          } else {
            if (!nodeSetWasReachable && upSet.isReachable) {
              debug("The node set is now available")

              broadcastMonitors(
                new SetAvailable(
                  upSet.protocolMetadata,
                  upSet.name,
                  upSet.isMongos
                )
              )

              updateHistory(s"IsMaster$$SetAvailable(${nodeSet.toShortString})")
            }

            @inline def wasPrimNames: Seq[String] =
              wasPrimary.toSeq.flatMap(_.names)

            if (upSet.primary.exists(n => !wasPrimNames.contains(n.name))) {
              val newPrim = upSet.primary.map(_.name)

              debug(s"The primary is now available: ${newPrim.mkString}")

              broadcastMonitors(
                new PrimaryAvailable(
                  upSet.protocolMetadata,
                  upSet.name,
                  upSet.isMongos
                )
              )

              updateHistory(
                s"IsMaster$$PrimaryAvailable(${nodeSet.toShortString})"
              )
            }
          }

          if (node.status != NodeStatus.Primary) upSet
          else {
            // Current node is known and primary

            // TODO: Test; 1. an NodeSet with node A & B, with primary A;
            // 2. Receive a isMaster response from B, with primary = B

            upSet.updateAll { n =>
              if (
                node.names.contains(n.name) && // the node itself
                n.status != node.status
              ) {
                // invalidate node status on status conflict
                warn(s"Invalid node status ${node.status} for ${node.name} (expected: ${n.status}); Fallback to Unknown status")

                n.copy(status = NodeStatus.Unknown, statusChanged = nanow)
              } else n
            }
          }
        }
      }
    }

    val origSender = context.sender()

    scheduler.scheduleOnce(Duration.Zero) {
      @inline def event =
        s"ConnectAll$$IsMaster(${response.header.responseTo}, ${updated.toShortString})"

      updateNodeSet(event) { ns =>
        ns.createUserConnections(
          channelFactory,
          options.maxIdleTimeMS,
          self,
          options.minIdleChannelsPerNode
        ) match {
          case Success(upd) => upd

          case Failure(cause) => {
            warn("Fails to create channel for NodeSet", cause)
            ns
          }
        }
      }

      if (origSender != null && origSender != context.system.deadLetters) {
        origSender ! _nodeSet
      }

      ()
    }

    ()
  }

  private[reactivemongo] def onPrimaryUnavailable(cause: Throwable): Unit = {
    self ! RefreshAll

    val error = s"${cause.getClass}(${cause.getMessage})"
    val st = s"OnError$$PrimaryUnavailable($error, ${_nodeSet.toShortString})"

    updateNodeSet(st)(_.updateAll { node =>
      if (node.status != NodeStatus.Primary) node
      else {
        node
          .copy(status = NodeStatus.Unknown, statusChanged = System.nanoTime())
      }
    })

    broadcastMonitors(PrimaryUnavailable)
  }

  private[reactivemongo] def updateNodeSet(
      event: String
    )(f: NodeSet => NodeSet
    ): NodeSet = {
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

  protected final def handleAuthResponse(
      nodeSet: NodeSet,
      resp: Response
    )(check: => Either[CommandException, SuccessfulAuthentication]
    ): NodeSet = {

    val chanId = resp.info.channelId
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
                originalAuthenticate,
                successfulAuthentication
              )

              if (nodeSet.isReachable) {
                debug("The node set is now authenticated")

                broadcastMonitors(
                  new SetAvailable(
                    nodeSet.protocolMetadata,
                    nodeSet.name,
                    nodeSet.isMongos
                  )
                )

                updateHistory(
                  s"AuthResponse$$SetAvailable(${nodeSet.toShortString})"
                )
              }

              if (nodeSet.primary.isDefined) {
                debug("The primary is now authenticated")

                broadcastMonitors(
                  new PrimaryAvailable(
                    nodeSet.protocolMetadata,
                    nodeSet.name,
                    nodeSet.isMongos
                  )
                )

                updateHistory(
                  s"AuthResponse$$PrimaryAvailable(${nodeSet.toShortString})"
                )

              } else if (nodeSet.isReachable) {
                warn(
                  s"""The node set is authenticated, but the primary is not available: ${nodeSet.name} -> ${nodeSet.nodes
                      .map(_.names) mkString ", "}"""
                )
              }

              Some(Authenticated(db, user))
            }

            case Left(error) => {
              AuthRequestsManager.handleAuthResult(originalAuthenticate, error)

              Option.empty[Authenticated]
            }
          }

          authenticated match {
            case Some(auth) =>
              node.updateByChannelId(chanId)({ con =>
                authenticateConnection(
                  con.copy(
                    authenticating = None,
                    authenticated = con.authenticated + auth
                  ),
                  nodeSet.authenticates
                )
              }) {
                _.copy(authenticated = node.authenticated + auth)
              }

            case _ =>
              node.updateByChannelId(chanId)(_.copy(authenticating = None))(
                identity /* unchanged */
              )
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
      case OpMsg(_, _, _) | KillCursors(_) | GetMore(_, _, _) => true
      case _                                                  => false
    })

  private def nodeInfo(reqAuth: Boolean, node: Node): String = {
    val info =
      s"connected:${node.connected.size}, channels:${node.connections.size}"

    if (!reqAuth) info
    else {
      s"authenticated:${node.authenticatedConnections.size}, authenticating: ${node.connected
          .count(_.authenticating.isDefined)}, $info"
    }
  }

  private def pickChannel(
      ns: NodeSet,
      pinnedNode: Option[String],
      request: Request
    ): Try[(Node, Connection)] = request.channelIdHint match {
    case Some(chanId) =>
      ns.pickByChannelId(chanId)
        .fold[Try[(Node, Connection)]](
          Failure(
            new ChannelNotFoundException(s"#${chanId}", false, internalState())
          )
        )(Success(_))

    case _ =>
      requestTracker.withAwaiting { (_, chans) =>
        // Ordering Node by pending requests or lastIsMasterTime
        implicit val ord = new NodeOrdering(chans)
        val unpriorised = math.ceil(ns.nodes.size.toDouble / 2D).toInt
        val accept = acceptBalancedCon(chans)

        def pick: Option[(Node, Connection)] = pinnedNode.fold(
          ns.pick(request.readPreference, unpriorised, accept)
        ) { name =>
          for {
            n <- ns.nodes.find(_.names contains name)
            c <- n.connections.find(accept)
          } yield n -> c
        }

        pick.fold({
          val secOk = secondaryOK(request)
          lazy val reqAuth = ns.authenticates.nonEmpty
          val cause: Throwable = if (!secOk) {
            ns.primary match {
              case Some(prim) => {
                val details =
                  s"'${prim.name}' { ${nodeInfo(reqAuth, prim)} } ($supervisor/$name)"

                if (!reqAuth || prim.authenticated.nonEmpty) {
                  new ChannelNotFoundException(
                    s"No active channel can be found to the primary node: $details",
                    true,
                    internalState()
                  )
                } else {
                  new NotAuthenticatedException(
                    s"No authenticated channel for the primary node: $details"
                  )
                }
              }

              case _ =>
                new PrimaryUnavailableException(
                  supervisor,
                  name,
                  internalState()
                )
            }
          } else if (!ns.isReachable) {
            new NodeSetNotReachableException(supervisor, name, internalState())
          } else {
            // Node set is reachable, secondary is ok, but no channel found
            val (authed, msgs) = ns.nodes.foldLeft(false -> Seq.empty[String]) {
              case ((authed, msgs), node) =>
                val msg =
                  s"'${node.name}' [${node.status}] { ${nodeInfo(reqAuth, node)} }"

                (authed || node.authenticated.nonEmpty) -> (msg +: msgs)
            }
            val details = s"""${msgs.mkString(", ")} ($supervisor/$name)"""

            if (!reqAuth || authed) {
              new ChannelNotFoundException(
                s"No active channel with '${request.readPreference}' found for the nodes: $details",
                true,
                internalState()
              )
            } else {
              new NotAuthenticatedException(
                s"No authenticated channel: $details"
              )
            }
          }

          Failure(cause): Try[(Node, Connection)]
        })(Success(_))
      }
  }

  private def foldNodeConnection[T](
      ns: NodeSet,
      pinnedNode: Option[String],
      request: Request
    )(e: Throwable => T,
      f: (Node, Connection) => T
    ): T = pickChannel(ns, pinnedNode, request) match {
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
    def updateNode(
        n: Node,
        before: Vector[Connection],
        updated: Vector[Connection]
      ): Node = before.headOption match {
      case Some(c) => {
        if (
          c.status == ConnectionStatus.Connecting ||
          c.status == ConnectionStatus.Connected
        ) {

          updateNode(n, before.drop(1), c +: updated)
        } else {
          val upStatus: ConnectionStatus = {
            if (c.channel.isActive) {
              ConnectionStatus.Connected
            } else {
              try {
                val asyncSt = Promise[ConnectionStatus]()
                val nodeAddr = new InetSocketAddress(n.host, n.port)

                try {
                  c.channel
                    .connect(nodeAddr)
                    .addListener(
                      new OperationHandler(
                        { cause =>
                          error(
                            s"Fails to connect channel #${c.channel.id}",
                            cause
                          )

                          asyncSt.success(ConnectionStatus.Disconnected)

                          if (cause.isInstanceOf[ClosedChannelException]) {
                            // #cch3: Channel is no longer open/cannot be active
                            warn(s"Will never be able to connect closed channel #${c.channel.id}")

                            self ! ChannelDisconnected(c.channel.id)
                          }

                          ()
                        },
                        { _ =>
                          asyncSt.success(ConnectionStatus.Connecting)

                          ()
                        }
                      )
                    )
                } catch {
                  case NonFatal(cause) =>
                    cause.printStackTrace()
                    throw cause
                }

                Await.result(asyncSt.future, connectTimeout)
              } catch {
                case NonFatal(reason) =>
                  warn(
                    s"Fails to connect node with channel #${c.channel.id}: ${n.toShortString}",
                    reason
                  )

                  c.status
              }
            }
          }

          val upCon = c.copy(status = upStatus)
          val upNode = {
            if (c.status == upStatus) n
            else n.updateByChannelId(c.channel.id)(_ => upCon)(identity)
          }

          updateNode(upNode, before.drop(1), upCon +: updated)
        }
      }

      case _ => {
        val cons = updated.reverse

        n.copy(
          connections = cons,
          authenticated = cons.toSet.flatMap((_: Connection).authenticated)
        )
      }
    }

    // _println(s"_connect_all: ${nodeSet.nodes}")

    nodeSet.copy(nodes = nodeSet.nodes.map { node =>
      updateNode(node, node.connections, Vector.empty)
    })
  }

  private class IsMasterRequest(
      val node: Node,
      f: () => Unit = () => (),
      error: Option[Exception] = None) {
    def send(): (Node, Option[Exception]) = { f(); node -> error }
  }

  private def requestIsMaster(context: String, node: Node): IsMasterRequest =
    node.signaling.fold(new IsMasterRequest(node)) { con =>
      import IsMasterCommand.{ IsMaster, writer }
      import reactivemongo.api.commands.Command

      lazy val id = RequestIdGenerator.isMaster.next
      val client: Option[ClientMetadata] =
        if (node.pingInfo.firstSent) None else Some(clientMetadata)

      lazy val isMaster = Command.buildRequestMaker(BSONSerializationPack)(
        CommandKind.Hello,
        new IsMaster(client, options.compressors, Some(id.toString)),
        writer(BSONSerializationPack),
        ReadPreference.primaryPreferred,
        "admin"
      ) // only "admin" DB for the admin command

      val now = System.nanoTime()

      def renewedPingInfo = node.pingInfo.copy(
        firstSent = true,
        ping = { // latency
          now - node.pingInfo.lastIsMasterTime
        },
        lastIsMasterTime = now,
        lastIsMasterId = id,
        channelId = Some(con.channel.id)
      )

      val sendFresh: () => Unit = { () =>
        con
          .send(
            isMaster(id),
            compression = ListSet.empty /* cannot compress */
          )
          .addListener(
            new OperationHandler(
              error(s"Fails to send a isMaster request to ${node.name} (channel #${con.channel.id})", _),
              { chanId =>
                trace(s"isMaster request to ${node.toShortString} successful on channel #${chanId}")
              }
            )
          )

        ()
      }

      if (node.pingInfo.lastIsMasterId == -1) {
        // There is no IsMaster request waiting response for the node:
        // sending a new one
        debug(s"Prepares a fresh IsMaster request to ${node.toShortString} (channel #${con.channel.id}@${con.channel.localAddress})")

        new IsMasterRequest(
          node = node.copy(pingInfo = renewedPingInfo),
          f = sendFresh
        )

      } else if ((node.pingInfo.lastIsMasterTime + pingTimeout) < now) {
        // Unregister the pending requests for this node
        val wasPrimary = node.status == NodeStatus.Primary
        val nanow = System.nanoTime()

        val updated = node.copy(
          status = NodeStatus.Unknown,
          statusChanged = {
            if (node.status != NodeStatus.Unknown) {
              nanow
            } else node.statusChanged
          },
          // connections = Vector.empty,
          authenticated = Set.empty,
          pingInfo = renewedPingInfo
        )

        // The previous IsMaster request is expired
        val msg =
          s"${updated.toShortString} hasn't answered in time to last ping! Please check its connectivity"

        warn(s"${msg} (<time:${formatNanos(nanow)}>).", internalState())

        // Reset node state
        updateHistory {
          val evtPrefix = s"${context}$$RequestIsMaster$$"

          if (wasPrimary) {
            s"${evtPrefix}PrimaryUnavailable(${node.toShortString})"
          } else s"${evtPrefix}NodeUnavailable(${node.toShortString})"
        }

        new IsMasterRequest(
          node = updated,
          f = sendFresh,
          error = Some {
            val cause = new ClosedException(s"$msg ($lnm)")

            if (!wasPrimary) cause
            else new PrimaryUnavailableException(supervisor, name, cause)
          }
        )
      } else {
        debug(
          s"Do not prepare a isMaster request to already probed ${node.name}"
        )

        new IsMasterRequest(node) // unchanged
      }
    }

  @inline private def scheduler = context.system.scheduler

  def allChannelGroup(nodeSet: NodeSet): DefaultChannelGroup = {
    val result = new DefaultChannelGroup(
      reactivemongo.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE
    )

    for (node <- nodeSet.nodes) {
      for (connection <- node.connections) result.add(connection.channel)
    }

    result
  }

  // Manage authenticate request other than the initial ones
  private object AuthRequestsManager {

    private var authRequests =
      Map.empty[Authenticate, List[Promise[SuccessfulAuthentication]]]

    def addAuthRequest(
        request: AuthRequest
      ): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] =
      this.synchronized {
        val found = authRequests.get(request.authenticate)

        authRequests =
          authRequests + (request.authenticate -> (request.promise :: found
            .getOrElse(Nil)))

        authRequests
      }

    private def withRequest[T](
        authenticate: Authenticate
      )(f: Promise[SuccessfulAuthentication] => T
      ): Map[Authenticate, List[Promise[SuccessfulAuthentication]]] =
      this.synchronized {
        authRequests.get(authenticate) match {
          case Some(found) => {
            authRequests = authRequests - authenticate

            found.foreach(f)
          }

          case _ => {
            debug(
              s"No pending authentication is matching response: $authenticate"
            )

            authRequests
          }
        }

        authRequests
      }

    def handleAuthResult(
        authenticate: Authenticate,
        result: SuccessfulAuthentication
      ) =
      withRequest(authenticate)(_.success(result))

    def handleAuthResult(authenticate: Authenticate, result: Throwable) =
      withRequest(authenticate)(_.failure(result))
  }

  private object NoJob extends Cancellable {
    def cancel() = false
    val isCancelled = false
  }

  // --- Logging ---

  protected final lazy val lnm = s"$supervisor/$name" // log naming

  @SuppressWarnings(Array("MethodNames"))
  @inline protected def _println(msg: => String) = println(s"[$lnm] $msg")

  @inline protected def debug(msg: => String) = logger.debug(s"[$lnm] $msg")

  @inline protected def debug(msg: => String, cause: Throwable) =
    logger.debug(s"[$lnm] $msg", cause)

  @inline protected def info(msg: => String) = logger.info(s"[$lnm] $msg")

  @inline protected def info(msg: => String, cause: Throwable) =
    logger.info(s"[$lnm] $msg", cause)

  @inline protected def trace(msg: => String) = logger.trace(s"[$lnm] $msg")

  @inline protected def warn(msg: => String) = logger.warn(s"[$lnm] $msg")

  @inline protected def warn(msg: => String, cause: Throwable) =
    logger.warn(s"[$lnm] $msg", cause)

  @inline protected def error(msg: => String) = logger.error(s"[$lnm] $msg")

  @inline protected def error(msg: => String, cause: Throwable) =
    logger.error(s"[$lnm] $msg", cause)
}

private[reactivemongo] final class StandardDBSystem(
    val supervisor: String,
    val name: String,
    val seeds: Seq[String],
    val initialAuthenticates: Seq[Authenticate],
    val options: MongoConnectionOptions)
    extends MongoDBSystem
    with MongoScramSha1Authentication {

  def newChannelFactory(effect: Unit): ChannelFactory =
    new ChannelFactory(supervisor, name, options)

}

private[reactivemongo] final class StandardDBSystemWithScramSha256(
    val supervisor: String,
    val name: String,
    val seeds: Seq[String],
    val initialAuthenticates: Seq[Authenticate],
    val options: MongoConnectionOptions)
    extends MongoDBSystem
    with MongoScramSha256Authentication {

  def newChannelFactory(effect: Unit): ChannelFactory =
    new ChannelFactory(supervisor, name, options)

}

private[reactivemongo] final class StandardDBSystemWithX509(
    val supervisor: String,
    val name: String,
    val seeds: Seq[String],
    val initialAuthenticates: Seq[Authenticate],
    val options: MongoConnectionOptions)
    extends MongoDBSystem
    with MongoX509Authentication {

  def newChannelFactory(effect: Unit): ChannelFactory =
    new ChannelFactory(supervisor, name, options)
}

private[reactivemongo] final class AuthRequest(
    val authenticate: Authenticate,
    val promise: Promise[SuccessfulAuthentication] = Promise()) {
  def future: Future[SuccessfulAuthentication] = promise.future

  override def equals(that: Any): Boolean = that match {
    case other: AuthRequest =>
      other.authenticate == this.authenticate

    case _ =>
      false
  }

  override def hashCode: Int = authenticate.hashCode
}

private[actors] final class RequestTracker {
  import scala.collection.mutable.LinkedHashMap

  /* Request ID -> AwaitingResponse */
  private val awaitingResponses = LinkedHashMap.empty[Int, AwaitingResponse]

  /* ChannelId -> count */
  private val awaitingChannels = LinkedHashMap.empty[ChannelId, Int]

  @inline def channels() = awaitingChannels.keySet.toSet

  private[actors] def withAwaiting[T](
      f: Function2[
        LinkedHashMap[Int, AwaitingResponse],
        LinkedHashMap[ChannelId, Int],
        T
      ]
    ): T =
    awaitingResponses.synchronized {
      f(awaitingResponses, awaitingChannels)
    }
}
