package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.SECONDS

import akka.util.Timeout
import akka.actor.ActorRef

import reactivemongo.core.protocol.Response
import reactivemongo.core.nodeset.{ Authenticate, NodeSet }
import reactivemongo.core.actors.{
  ChannelClosed,
  MongoDBSystem,
  StandardDBSystem,
  MongoScramSha1Authentication
}

import reactivemongo.bson.BSONDocument

package object tests {
  // Test alias
  def _failover2[A](c: MongoConnection, s: FailoverStrategy)(p: () => Future[A])(implicit ec: ExecutionContext): Failover2[A] = Failover2.apply(c, s)(p)(ec)

  def isAvailable(con: MongoConnection)(implicit ec: ExecutionContext): Future[Boolean] = con.probe.map(_.isEmpty)

  def waitIsAvailable(con: MongoConnection, failoverStrategy: FailoverStrategy)(implicit ec: ExecutionContext): Future[Unit] = con.waitIsAvailable(failoverStrategy)

  def standardDBSystem(supervisor: String, name: String, nodes: Seq[String], authenticates: Seq[Authenticate], options: MongoConnectionOptions) =
    new StandardDBSystem(supervisor, name, nodes, authenticates, options) with MongoScramSha1Authentication

  def addConnection(d: MongoDriver, name: String, nodes: Seq[String], options: MongoConnectionOptions, mongosystem: ActorRef): Future[Any] = {
    import akka.pattern.ask

    def message = d.AddConnection(name, nodes, options, mongosystem)
    implicit def timeout = Timeout(10, SECONDS)

    d.supervisorActor ? message
  }

  def history(sys: MongoDBSystem): Traversable[(Long, String)] =
    sys.history.toArray.toList.collect {
      case (time: Long, event: String) => time -> event
    }

  def nodeSet(sys: MongoDBSystem): NodeSet = sys.getNodeSet

  def channelClosed(id: Int) = ChannelClosed(id)

  def makeRequest[T](cursor: Cursor[T], maxDocs: Int)(implicit ec: ExecutionContext): Future[Response] = cursor.asInstanceOf[CursorOps[T]].makeRequest(maxDocs)

  def fakeResponse(doc: BSONDocument, reqID: Int = 2, respTo: Int = 1): Response = {
    val reply = reactivemongo.core.protocol.Reply(
      flags = 1,
      cursorID = 1,
      startingFrom = 0,
      numberReturned = 1
    )

    val message = reactivemongo.core.netty.BufferSequence.single(doc).merged

    val header = reactivemongo.core.protocol.MessageHeader(
      messageLength = message.capacity,
      requestID = reqID,
      responseTo = respTo,
      opCode = -1
    )

    Response(
      header,
      reply,
      documents = message,
      info = reactivemongo.core.protocol.ResponseInfo(1)
    )
  }

  def foldResponses[T](
    makeRequest: ExecutionContext => Future[Response],
    next: (ExecutionContext, Response) => Future[Option[Response]],
    killCursors: (Long, String) => Unit,
    z: => T,
    maxDocs: Int,
    suc: (T, Response) => Future[Cursor.State[T]],
    err: Cursor.ErrorHandler[T]
  )(implicit sys: akka.actor.ActorSystem, ec: ExecutionContext): Future[T] =
    FoldResponses[T](
      z, makeRequest, next, killCursors, suc, err, maxDocs
    )(sys, ec)

  def bsonReadPref(pref: ReadPreference): BSONDocument =
    reactivemongo.api.collections.bson.BSONReadPreference.write(pref)
}
