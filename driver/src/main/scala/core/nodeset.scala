package reactivemongo.core.nodeset

import akka.actor._
import akka.io.Tcp.Register
import akka.util.ByteStringBuilder
import core.SocketWriter
import reactivemongo.core.actors._
import reactivemongo.core.protocol.{Request, _}
import reactivemongo.core.{SocketReader, _}
import reactivemongo.core.commands._

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.HashMap
import scala.util.{Failure, Success}

package object utils {
  def updateFirst[A, M[T] <: Iterable[T]](coll: M[A])(ƒ: A => Option[A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): M[A] = {
    val builder = cbf.apply
    def run(iterator: Iterator[A]): Unit = {
      while (iterator.hasNext) {
        val e = iterator.next
        val updated = ƒ(e)
        if (updated.isDefined) {
          builder += updated.get
          builder ++= iterator
        } else builder += e
      }
    }
    builder.result
  }

  def update[A, M[T] <: Iterable[T]](coll: M[A])(f: PartialFunction[A, A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): (M[A], Boolean) = {
    val builder = cbf.apply
    val (head, tail) = coll.span(!f.isDefinedAt(_))
    builder ++= head
    if (!tail.isEmpty) {
      builder += f(tail.head)
      builder ++= tail.drop(1)
    }
    builder.result -> !tail.isEmpty
  }
}



case class ProtocolMetadata(
  minWireVersion: MongoWireVersion,
  maxWireVersion: MongoWireVersion,
  maxMessageSizeBytes: Int,
  maxBsonSize: Int,
  maxBulkSize: Int
)

object ProtocolMetadata {
  val Default = ProtocolMetadata(MongoWireVersion.V24AndBefore, MongoWireVersion.V24AndBefore, 48000000, 16 * 1024 * 1024, 1000)
}

case class Connection(
      socketManager: ActorRef,
      port: Int) extends Actor with ActorLogging {

  private var protocolMetadata: Option[ProtocolMetadata] = None
  private var pendingIsMaster : (Int, PingInfo) = null
  private var awaitingResponses = HashMap[Int, AwaitingResponse]()
  private var awaitingAuth = List.empty[AuthRequest]
  private var authenticating: Option[Authenticating] = None
  val requestIds = new RequestIds
  val socketReader = context.actorOf(Props(classOf[SocketReader], socketManager, port))
  val socketWriter = context.actorOf(Props(classOf[SocketWriter], socketManager))
  socketManager ! Register(socketReader, keepOpenOnPeerClosed = true)


  override def receive: Actor.Receive = {
    case Node.IsMaster => {
      val initialInfo = PingInfo(Int.MaxValue, System.currentTimeMillis())
      val request = IsMaster().maker(requestIds.isMaster.next)
      val builder = new ByteStringBuilder
      request.append(builder)
      pendingIsMaster = (request.requestID, initialInfo)
      socketWriter ! builder.result()
    }
    case req: RequestMaker => {
      val requestId = requestIds.common.next
      val request = req(requestId)
      log.debug("Send request expecting response with a header {}", request.header)
      socketWriter ! request.message
    }
    case auth: AuthRequest => {
      authenticating match {
        case None => {
          log.debug("get AuthRequest, sending getnonce")
          authenticating = Some(Authenticating(auth, None))
          val getNonceRequest = Getnonce(auth.authenticate.db).maker(requestIds.getNonce.next)
          socketWriter ! getNonceRequest.message
        }
        case _ => {
          log.debug("get AuthRequest, put it buffer")
          awaitingAuth = auth +: awaitingAuth
        }
      }
    }
    case req: RequestMakerExpectingResponse => {
      val requestId = requestIds.common.next
      val request = req.requestMaker(requestId)
      log.debug("Send request expecting response with a header {}", request.header)
      awaitingResponses += request.requestID -> AwaitingResponse(request.requestID, port, req.promise, isGetLastError = false, isMongo26WriteOp = req.isMongo26WriteOp)
      socketWriter ! request.message
    }
    // getnonce response
    case response: Response if requestIds.getNonce accepts response =>
      Getnonce.ResultMaker(response).fold(
        e => {
          log.error(e, "error while processing getNonce response {}", response)
          authenticating = None
          awaitingAuth.headOption.map(self ! _)
        },
        nonce => {
          log.debug("AUTH: got nonce for channel " + response.channelId + ": " + nonce)
          authenticating = Some(authenticating.get.copy(nonce = Some(nonce)))
          socketWriter ! AuthenticateCommand(authenticating.get.user, authenticating.get.authRequest.authenticate.password, authenticating.get.nonce.get)(authenticating.get.db)
            .maker(requestIds.authenticate.next).message
        })
    case response: Response if requestIds.authenticate accepts response => {
      log.info("AUTH: got authenticated response! " + response.channelId)
      AuthenticateCommand(response) match {
        case Right(successfulAuthentication) => {
          log.info("AUTH: successful authentication on channel {}", port)
          authenticating.get.authRequest.promise.success(successfulAuthentication)
        }
        case Left(error) => {
          log.error(error, "Unable to authenticate with credential {} on channel {}", authenticating.get, port)
          authenticating.get.authRequest.promise.failure(error)
        }
      }
      authenticating = None
      awaitingAuth.headOption.map(self ! _)
    }
    case response: Response => {
      if (requestIds.isMaster accepts response) {
        if (pendingIsMaster._1 == response.header.responseTo) {
          import reactivemongo.api.BSONSerializationPack
          import reactivemongo.api.commands.Command
          import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits
          val initialInfo = pendingIsMaster._2
          val isMaster = Command.deserialize(BSONSerializationPack, response)(BSONIsMasterCommandImplicits.IsMasterResultReader)
          log.debug("received isMaster {}", isMaster)
          context.parent ! Node.IsMasterInfo(isMaster, initialInfo.copy(ping = System.currentTimeMillis() - initialInfo.lastIsMasterTime))
        } else {
          // prevent race condition
          self ! Node.IsMaster
        }
      } else {
        processResponse(response)
      }
    }
    case Close => {
      socketManager ! akka.io.Tcp.Close
      socketReader ! Close
      context.become(closing)
    }
    case a : Any => log.warning("unhandled messsage {}", a)
  }

  private def closing = waitClose orElse receive

  private def waitClose: Receive = {
    case akka.io.Tcp.Closed => {
      log.info("connection is closed")
      context.parent ! Closed
    }
    case Close => log.warning("closing connection multiple times")
  }

  private def processResponse(response: Response) = {
      awaitingResponses.get(response.header.responseTo) match {
        case Some(AwaitingResponse(_, _, promise, isGetLastError, isMongo26WriteOp)) => {
          log.debug("Got a response from " + response.channelId + "! Will give back message=" + response + " to promise " + promise)
          awaitingResponses -= response.header.responseTo
          if (response.error.isDefined) {
            log.debug("{" + response.header.responseTo + "} sending a failure... (" + response.error.get + ")")
            if (response.error.get.isNotAPrimaryError) context.parent ! Node.PrimaryUnavailable
            promise.failure(response.error.get)
          } else if (isGetLastError) {
            log.debug("{" + response.header.responseTo + "} it's a getlasterror")
            // todo, for now rewinding buffer at original index
            LastError(response).fold(e => {
              log.error(s"Error deserializing LastError message #${response.header.responseTo}", e)
              promise.failure(new RuntimeException(s"Error deserializing LastError message #${response.header.responseTo}", e))
            },
              lastError => {
                if (lastError.inError) {
                  log.debug("{" + response.header.responseTo + "} sending a failure (lasterror is not ok)")
                  if (lastError.isNotAPrimaryError) context.parent ! Node.PrimaryUnavailable
                  promise.failure(lastError)
                } else {
                  log.debug("{" + response.header.responseTo + "} sending a success (lasterror is ok)")
                  promise.success(response)
                }
              })
          } else if (isMongo26WriteOp) {
            // TODO - logs, bson
            // MongoDB 26 Write Protocol errors
            log.debug("received a response to a MongoDB2.6 Write Op")
            import reactivemongo.bson.lowlevel._
            val reader = new LowLevelBsonDocReader(new AkkaReadableBuffer(response.documents))
            val fields = reader.fieldStream
            val okField = fields.find(_.name == "ok")
            log.debug(s"{${response.header.responseTo}} ok field is: $okField")
            val processedOk = okField.collect {
              case BooleanField(_, v) => v
              case IntField(_, v) => v != 0
              case DoubleField(_, v) => v != 0
            }.getOrElse(false)

            if (processedOk) {
              log.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
              promise.success(response)
            } else {
              log.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] processedOk is false! sending an error")
              val notAPrimary = fields.find(_.name == "errmsg").exists {
                case errmsg @ LazyField(0x02, _, buf) =>
                  log.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg!")
                  buf.readString == "not a primary"
                case errmsg =>
                  log.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] errmsg is $errmsg but not interesting!")
                  false
              }
              if(notAPrimary) {
                log.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] not a primary error!")
                context.parent ! Node.PrimaryUnavailable
              }
              promise.failure(new RuntimeException("not ok"))
            }
          } else {
            log.debug(s"{${response.header.responseTo}} [MongoDB26 Write Op response] sending a success!")
            promise.success(response)
          }
        }
        case None => {
          log.error("oups. " + response.header.responseTo + " not found! complete message is " + response)
        }
      }
  }
}


object Connection {
  case class RequestMakerWithDeserialization(requestMaker: RequestMakerExpectingResponse)
  case class ConnectionStatus(isMongos: Boolean, isPrimary: Boolean, connection: ActorRef)
}

case class PingInfo(
  ping: Long = 0,
  lastIsMasterTime: Long = 0)

object PingInfo {
  val pingTimeout = 60 * 1000
}

sealed trait NodeStatus { def queryable = false }
sealed trait QueryableNodeStatus { self: NodeStatus => override def queryable = true }
sealed trait CanonicalNodeStatus { self: NodeStatus => }
object NodeStatus {
  object Uninitialized extends NodeStatus { override def toString = "Uninitialized" }
  object NonQueryableUnknownStatus extends NodeStatus { override def toString = "NonQueryableUnknownStatus" }

  /** Cannot vote. All members start up in this state. The mongod parses the replica set configuration document while in STARTUP. */
  object Startup extends NodeStatus with CanonicalNodeStatus { override def toString = "Startup" }
  /** Can vote. The primary is the only member to accept write operations. */
  object Primary extends NodeStatus with QueryableNodeStatus with CanonicalNodeStatus { override def toString = "Primary" }
  /** Can vote. The secondary replicates the data store. */
  object Secondary extends NodeStatus with QueryableNodeStatus with CanonicalNodeStatus { override def toString = "Secondary" }
  /** Can vote. Members either perform startup self-checks, or transition from completing a rollback or resync. */
  object Recovering extends NodeStatus with CanonicalNodeStatus { override def toString = "Recovering" }
  /** Cannot vote. Has encountered an unrecoverable error. */
  object Fatal extends NodeStatus with CanonicalNodeStatus { override def toString = "Fatal" }
  /** Cannot vote. Forks replication and election threads before becoming a secondary. */
  object Startup2 extends NodeStatus with CanonicalNodeStatus { override def toString = "Startup2" }
  /** Cannot vote. Has never connected to the replica set. */
  object Unknown extends NodeStatus with CanonicalNodeStatus { override def toString = "Unknown" }
  /** Can vote. Arbiters do not replicate data and exist solely to participate in elections. */
  object Arbiter extends NodeStatus with CanonicalNodeStatus { override def toString = "Arbiter" }
  /** Cannot vote. Is not accessible to the set. */
  object Down extends NodeStatus with CanonicalNodeStatus { override def toString = "Down" }
  /** Can vote. Performs a rollback. */
  object Rollback extends NodeStatus with CanonicalNodeStatus { override def toString = "Rollback" }
  /** Shunned. */
  object Shunned extends NodeStatus with CanonicalNodeStatus { override def toString = "Shunned" }

  def apply(code: Int): NodeStatus = code match {
    case 0  => Startup
    case 1  => Primary
    case 2  => Secondary
    case 3  => Recovering
    case 4  => Fatal
    case 5  => Startup2
    case 6  => Unknown
    case 7  => Arbiter
    case 8  => Down
    case 9  => Rollback
    case 10 => Shunned
    case _  => NonQueryableUnknownStatus
  }
}

sealed trait ConnectionStatus
object ConnectionStatus {
  object Disconnected extends ConnectionStatus { override def toString = "Disconnected" }
  object Connected extends ConnectionStatus { override def toString = "Connected" }
}

sealed trait Authentication {
  def user: String
  def db: String
}

case class Authenticate(db: String, user: String, password: String) extends Authentication {
  override def toString: String = "Authenticate(" + db + ", " + user + ")"
}
case class Authenticating(authRequest: AuthRequest, nonce: Option[String]) extends Authentication {
  override def toString: String =
    s"Authenticating($db, $user, ${nonce.map(_ => "<nonce>").getOrElse("<>")})"

  override def user: String = authRequest.authenticate.user

  override def db: String = authRequest.authenticate.db
}
case class Authenticated(db: String, user: String) extends Authentication

object RandomPick {
  import scala.util.Random

  implicit class IterableExt[A](val coll: Iterable[A]) {
    def getRandom() : A ={
      val rnd = new Random(coll.hashCode())
      val next = rnd.nextInt(coll.size)
      coll.drop(next).head
    }
  }
}

class ContinuousIterator[A](iterable: Iterable[A], private var toDrop: Int = 0) extends Iterator[A] {
  private var iterator = iterable.iterator
  private var i = 0

  val hasNext = iterator.hasNext

  if (hasNext) drop(toDrop)

  def next =
    if (!hasNext) throw new NoSuchElementException("empty iterator")
    else {
      if (!iterator.hasNext) {
        iterator = iterable.iterator
        i = 0
      }
      val a = iterator.next
      i += 1
      a
    }

  def nextIndex = i
}

