package reactivemongo.api

import java.util.UUID

import scala.util.Try

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ FiniteDuration, SECONDS }

import akka.util.Timeout
import akka.actor.ActorRef

import reactivemongo.io.netty.channel.{
  Channel,
  ChannelId,
  DefaultChannelId
}

import reactivemongo.io.netty.channel.Channel
import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.core.protocol.{
  Request,
  Response,
  ResponseFrameDecoder,
  ResponseDecoder
}
import reactivemongo.core.nodeset.{
  Authenticate,
  ChannelFactory,
  NodeSet
}
import reactivemongo.core.actors, actors.{
  ChannelConnected,
  ChannelDisconnected,
  MongoDBSystem,
  RequestIdGenerator,
  StandardDBSystem
}

import reactivemongo.bson.BSONDocument

import reactivemongo.api.collections.QueryCodecs

import reactivemongo.api.indexes.Index

package object tests {
  type Pack = Serialization.Pack
  val pack: Pack = Serialization.internalSerializationPack

  lazy val decoder = pack.newDecoder
  lazy val builder = pack.newBuilder

  def newBuilder[P <: SerializationPack](pack: P) = pack.newBuilder

  def reader[T](f: pack.Document => T): pack.Reader[T] = pack.reader[T](f)

  def writer[T](f: T => pack.Document): pack.Writer[T] = pack.writer[T](f)

  def numConnections(d: AsyncDriver): Int = d.numConnections

  // Test alias
  def _failover2[A](c: MongoConnection, s: FailoverStrategy)(p: () => Future[A])(implicit ec: ExecutionContext): Failover2[A] = Failover2.apply(c, s)(p)(ec)

  def isAvailable(con: MongoConnection, timeout: FiniteDuration)(implicit ec: ExecutionContext): Future[Boolean] = con.probe(timeout).map(_ => true).recover {
    case _ => false
  }

  def waitIsAvailable(con: MongoConnection, failoverStrategy: FailoverStrategy)(implicit ec: ExecutionContext) = con.waitIsAvailable(failoverStrategy, Array.empty)

  def standardDBSystem(supervisor: String, name: String, nodes: Seq[String], authenticates: Seq[Authenticate], options: MongoConnectionOptions) = new StandardDBSystem(supervisor, name, nodes, authenticates, options)

  def addConnection(d: AsyncDriver, name: String, nodes: Seq[String], options: MongoConnectionOptions, mongosystem: ActorRef): Future[Any] = {
    import akka.pattern.ask

    def message = d.addConnectionMsg(name, nodes, options, mongosystem)
    implicit def timeout = Timeout(10, SECONDS)

    d.supervisorActor ? message
  }

  def history(sys: MongoDBSystem): Traversable[(Long, String)] = {
    val snap = sys.history.synchronized {
      sys.history.toArray()
    }

    snap.toList.collect {
      case (time: Long, event: String) => time -> event
    }
  }

  def nodeSet(sys: MongoDBSystem): NodeSet = sys.getNodeSet

  def channelConnected(id: ChannelId) = ChannelConnected(id)

  def channelClosed(id: ChannelId) = ChannelDisconnected(id)

  def makeRequest[T](cursor: Cursor[T], maxDocs: Int)(implicit ec: ExecutionContext): Future[Response] = cursor.asInstanceOf[CursorOps[T]].makeRequest(maxDocs)

  def nextResponse[T](cursor: Cursor[T], maxDocs: Int) =
    cursor.asInstanceOf[CursorOps[T]].nextResponse(maxDocs)

  @inline def channelBuffer(doc: BSONDocument) =
    reactivemongo.core.netty.ChannelBufferWritableBuffer.single(doc)

  @inline def bufferSeq(doc: BSONDocument) =
    reactivemongo.core.netty.BufferSequence.single(doc)

  def fakeResponse(
    doc: BSONDocument,
    reqID: Int = 2,
    respTo: Int = 1,
    chanId: ChannelId = DefaultChannelId.newInstance()): Response = {
    val reply = reactivemongo.core.protocol.Reply(
      flags = 1,
      cursorID = 1,
      startingFrom = 0,
      numberReturned = 1)

    val message = bufferSeq(doc).merged

    val header = reactivemongo.core.protocol.MessageHeader(
      messageLength = message.capacity,
      requestID = reqID,
      responseTo = respTo,
      opCode = -1)

    Response(
      header,
      reply,
      documents = message,
      info = reactivemongo.core.protocol.ResponseInfo(chanId))
  }

  def foldResponses[T](
    makeRequest: ExecutionContext => Future[Response],
    next: (ExecutionContext, Response) => Future[Option[Response]],
    killCursors: (Long, String) => Unit,
    z: => T,
    maxDocs: Int,
    suc: (T, Response) => Future[Cursor.State[T]],
    err: Cursor.ErrorHandler[T])(implicit sys: akka.actor.ActorSystem, ec: ExecutionContext): Future[T] =
    FoldResponses[T](
      z, makeRequest, next, killCursors, suc, err, maxDocs)(sys, ec)

  val bsonReadPref: ReadPreference => BSONDocument = {
    val writeReadPref = QueryCodecs.writeReadPref(BSONSerializationPack)

    writeReadPref(_: ReadPreference)
  }

  @inline def RefreshAll = actors.RefreshAll

  @inline def updateNodeSet(sys: StandardDBSystem, event: String)(f: NodeSet => NodeSet) = sys.updateNodeSet(event)(f)

  @inline def nodeSet(sys: StandardDBSystem) = sys._nodeSet

  @inline def isMasterReqId(): Int = RequestIdGenerator.isMaster.next

  @inline def connectAll(sys: StandardDBSystem, ns: NodeSet) =
    sys.connectAll(ns)

  @inline def channelFactory(supervisorName: String, connectionName: String, options: MongoConnectionOptions): ChannelFactory = new ChannelFactory(supervisorName, connectionName, options)

  @inline def createChannel(
    factory: ChannelFactory,
    receiver: ActorRef,
    host: String,
    port: Int): Try[Channel] = factory.create(host, port, receiver)

  @inline def releaseChannelFactory(f: ChannelFactory, clb: Promise[Unit]) =
    f.release(clb)

  @inline def isMasterRequest(reqId: Int = RequestIdGenerator.isMaster.next): Request = {
    import reactivemongo.api.BSONSerializationPack
    import reactivemongo.api.commands.bson.{
      BSONIsMasterCommandImplicits,
      BSONIsMasterCommand
    }, BSONIsMasterCommand.IsMaster
    import reactivemongo.api.commands.Command

    val (isMaster, _) = Command.buildRequestMaker(BSONSerializationPack)(
      IsMaster,
      BSONIsMasterCommandImplicits.IsMasterWriter,
      reactivemongo.api.ReadPreference.primaryPreferred,
      "admin") // only "admin" DB for the admin command

    isMaster(reqId) // RequestIdGenerator.isMaster
  }

  @inline def isMasterResponse(response: Response) =
    RequestIdGenerator.isMaster accepts response

  def decodeResponse[T]: Array[Byte] => (Tuple2[ByteBuf, Response] => T) => T = {
    val decoder = new ResponseDecoder()

    { bytes =>
      val buf = Unpooled.buffer(bytes.size, bytes.size)
      val out = new java.util.ArrayList[Object](1)

      buf.retain()

      buf.writeBytes(bytes)
      buf.resetReaderIndex()

      decoder.decode(null, buf, out)

      { f: (Tuple2[ByteBuf, Response] => T) =>
        try {
          f(buf -> out.get(0).asInstanceOf[Response])
        } finally {
          buf.release()
          ()
        }
      }
    }
  }

  val decodeFrameResp: Array[Byte] => List[ByteBuf] = {
    val decoder = new ResponseFrameDecoder()

    { bytes =>
      val buf = Unpooled.buffer(bytes.size, bytes.size)
      val out = new java.util.ArrayList[Object](1)

      buf.writeBytes(bytes)
      buf.resetReaderIndex()

      decoder.decode(null, buf, out)

      val frames = List.newBuilder[ByteBuf]
      val it = out.iterator

      while (it.hasNext) {
        frames += it.next().asInstanceOf[ByteBuf]
      }

      frames.result()
    }
  }

  @inline def initChannel(
    factory: ChannelFactory,
    channel: Channel,
    host: String, port: Int,
    receiver: ActorRef) = factory.initChannel(channel, host, port, receiver)

  def getBytes(buf: ByteBuf, size: Int): Array[Byte] = {
    val bytes = Array.ofDim[Byte](size)

    buf.resetReaderIndex()
    buf.getBytes(0, bytes)

    bytes
  }

  def withContent[T](uri: java.net.URI)(f: java.io.InputStream => T): T =
    reactivemongo.util.withContent[T](uri)(f)

  def srvRecords(name: String, srvPrefix: String)(implicit ec: ExecutionContext) = reactivemongo.util.srvRecords(name)(reactivemongo.util.dnsResolve(srvPrefix = srvPrefix))

  def parseURI(
    uri: String,
    srvResolver: reactivemongo.util.SRVRecordResolver,
    txtResolver: reactivemongo.util.TXTResolver) =
    MongoConnection.parseURI(uri, srvResolver, txtResolver)

  @inline def probe(con: MongoConnection, timeout: FiniteDuration) = con.probe(timeout)

  def sessionId(db: DB): Option[UUID] = db.session.map(_.lsid)

  def preload(resp: Response)(implicit ec: ExecutionContext): Future[(Response, BSONDocument)] = Response.preload(resp)

  @inline def session(db: DefaultDB): Option[Session] = db.session

  @inline def indexOptions[P <: SerializationPack](i: Index.Aux[P]): i.pack.Document = i.optionDocument

  object commands {
    implicit val replSetMaintenanceWriter =
      reactivemongo.api.commands.ReplSetMaintenance.writer(pack)

    implicit val unitBoxReader =
      reactivemongo.api.commands.CommandCodecs.unitBoxReader(pack)

    implicit val replSetGetStatusWriter =
      reactivemongo.api.commands.ReplSetGetStatus.writer(pack)

    import reactivemongo.api.commands.ReplSetStatus

    implicit val replSetStatusReader: pack.Reader[ReplSetStatus] =
      reactivemongo.api.commands.ReplSetGetStatus.reader(pack)

    implicit val serverStatusWriter =
      reactivemongo.api.commands.ServerStatus.writer(pack)

    implicit val serverStatusReader =
      reactivemongo.api.commands.ServerStatus.reader(pack)

    import reactivemongo.api.commands.IsMasterCommand
    type IsMaster = IsMasterCommand[pack.type]#IsMaster

    def isMasterWriter(cmd: IsMasterCommand[pack.type]) = new {
      def get[T <: cmd.IsMaster]: pack.Writer[T] = cmd.writer[T](pack)
    }

    def isMasterReader(cmd: IsMasterCommand[pack.type]) = new {
      def get(implicit sr: pack.NarrowValueReader[String]): pack.Reader[cmd.IsMasterResult] = cmd.reader(pack)
    }
  }
}
