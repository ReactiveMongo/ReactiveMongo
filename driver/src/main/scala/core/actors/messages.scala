package reactivemongo.core.actors

import scala.concurrent.{ Future, Promise }

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.protocol.{
  CheckedWriteRequest,
  RequestMaker,
  Response
}
import reactivemongo.core.nodeset.ProtocolMetadata

/**
 * A message expecting a response from database.
 * It holds a promise that will be completed by the MongoDBSystem actor.
 * The future can be used to get the error or the successful response.
 */
@deprecated("Internal: will be made private", "0.16.0")
sealed trait ExpectingResponse {
  private[actors] val promise: Promise[Response] = Promise()

  /** The future response of this request. */
  val future: Future[Response] = promise.future
}

@deprecated("Internal: will be made private", "0.16.0")
object ExpectingResponse {
  def unapply(that: Any): Option[Promise[Response]] = that match {
    case req @ RequestMakerExpectingResponse(_, _) => Some(req.promise)
    case _                                         => None
  }
}

/**
 * A request expecting a response.
 *
 * @param requestMaker the request maker
 * @param isMongo26WriteOp true if the operation is a MongoDB 2.6 write one
 */
@deprecated("Internal: will be made private", "0.16.0")
case class RequestMakerExpectingResponse(
  requestMaker: RequestMaker,
  isMongo26WriteOp: Boolean) extends ExpectingResponse

/**
 * A checked write request expecting a response.
 *
 * @param checkedWriteRequest The request maker.
 */
@deprecated("Unused", "0.16.0")
case class CheckedWriteRequestExpectingResponse(
  checkedWriteRequest: CheckedWriteRequest) extends ExpectingResponse

@deprecated(message = "Internal: will be made private", since = "0.12.8")
sealed class Close {
  def source: String = "unknown"
}

/**
 * Message to close all active connections.
 * The MongoDBSystem actor must not be used after this message has been sent.
 */
@deprecated("Internal: will be made private", "0.16.0")
case object Close extends Close {
  def apply(src: String): Close = new Close {
    override val source = src
  }

  def unapply(msg: Close): Option[String] = Some(msg.source)
}

/**
 * Message to send in order to get warned the next time a primary is found.
 */
private[reactivemongo] case object ConnectAll
private[reactivemongo] case object RefreshAll
private[reactivemongo] case class ChannelConnected(channelId: ChannelId)

private[reactivemongo] case class ChannelDisconnected(channelId: ChannelId)

/** Message sent when the primary has been discovered. */
@deprecated("Internal: will be made private", "0.16.0")
class PrimaryAvailable(
  val metadata: ProtocolMetadata,
  private[reactivemongo] val setName: Option[String]) extends Product with Serializable {

  val productArity = 2

  def productElement(n: Int): Any = n match {
    case 1 => metadata
    case _ => setName
  }

  override def equals(that: Any): Boolean = that match {
    case other: PrimaryAvailable =>
      (metadata -> setName) == (other.metadata -> other.setName)

    case _ => false
  }

  override def hashCode: Int = (metadata -> setName).hashCode

  def canEqual(that: Any): Boolean = that match {
    case _: PrimaryAvailable => true
    case _                   => false
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object PrimaryAvailable extends scala.runtime.AbstractFunction1[ProtocolMetadata, PrimaryAvailable] {

  def apply(metadata: ProtocolMetadata): PrimaryAvailable =
    new PrimaryAvailable(metadata, None)

  def unapply(that: Any): Option[ProtocolMetadata] = that match {
    case a: PrimaryAvailable => Option(a.metadata)
    case _                   => None
  }
}

/** Message sent when the primary has been lost. */
@deprecated("Internal: will be made private", "0.16.0")
case object PrimaryUnavailable

@deprecated("Internal: will be made private", "0.16.0")
class SetAvailable(
  val metadata: ProtocolMetadata,
  private[reactivemongo] val setName: Option[String])
  extends Product with Serializable {

  val productArity = 2

  def productElement(n: Int): Any = n match {
    case 1 => metadata
    case _ => setName
  }

  override def equals(that: Any): Boolean = that match {
    case other: SetAvailable =>
      (metadata -> setName) == (other.metadata -> other.setName)

    case _ => false
  }

  override def hashCode: Int = (metadata -> setName).hashCode

  def canEqual(that: Any): Boolean = that match {
    case _: SetAvailable => true
    case _               => false
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object SetAvailable extends scala.runtime.AbstractFunction1[ProtocolMetadata, SetAvailable] {

  def apply(metadata: ProtocolMetadata): SetAvailable =
    new SetAvailable(metadata, None)

  def unapply(that: Any): Option[ProtocolMetadata] = that match {
    case a: SetAvailable => Option(a.metadata)
    case _               => None
  }
}

// TODO
@deprecated("Internal: will be made private", "0.16.0")
case object SetUnavailable

/** Register a monitor. */
@deprecated("Internal: will be made private", "0.16.0")
case object RegisterMonitor

/** MongoDBSystem has been shut down. */
@deprecated("Internal: will be made private", "0.16.0")
case object Closed

@deprecated("Unused", "0.16.0")
case object GetLastMetadata

private[actors] object IsMasterResponse {
  def unapply(response: Response): Option[Response] =
    if (RequestId.isMaster accepts response) Some(response) else None
}
