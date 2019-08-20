package reactivemongo.core.actors

import scala.concurrent.{ Future, Promise }

import reactivemongo.api.ReadPreference

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
sealed trait ExpectingResponse { // TODO: Merge with RequestMakerExpectingResponse once CheckedWriteRequestExpectingResponse is removed
  // TODO: final
  private[actors] val promise: Promise[Response] = Promise()

  /** The future response of this request. */
  val future: Future[Response] = promise.future // TODO: final

  private[reactivemongo] def pinnedNode: Option[String] = None
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
class RequestMakerExpectingResponse private[reactivemongo] (
  val requestMaker: RequestMaker,
  val isMongo26WriteOp: Boolean,
  private[reactivemongo] override val pinnedNode: Option[String]) extends ExpectingResponse with Product with Serializable {
  @deprecated("Use constructor with `pinnedNode`", "0.18.5")
  def this(
    requestMaker: RequestMaker,
    isMongo26WriteOp: Boolean) = this(requestMaker, isMongo26WriteOp, None)

  def canEqual(that: Any): Boolean = that match {
    case _: RequestMakerExpectingResponse => true
    case _                                => false
  }

  override def equals(that: Any): Boolean = that match {
    case other: RequestMakerExpectingResponse =>
      tupled == other.tupled

    case _ =>
      false
  }

  override lazy val hashCode: Int = tupled.hashCode

  lazy val productArity: Int = tupled.productArity

  @inline def productElement(n: Int): Any = tupled.productElement(n)

  private lazy val tupled = Tuple3(requestMaker, isMongo26WriteOp, pinnedNode)
}

@deprecated("Internal: will be made private", "0.16.0")
object RequestMakerExpectingResponse extends scala.runtime.AbstractFunction2[RequestMaker, Boolean, RequestMakerExpectingResponse] {

  def apply(
    requestMaker: RequestMaker,
    isMongo26WriteOp: Boolean): RequestMakerExpectingResponse =
    new RequestMakerExpectingResponse(requestMaker, isMongo26WriteOp, None)

  def unapply(m: RequestMakerExpectingResponse): Option[(RequestMaker, Boolean)] = Option(m).map { s => s.requestMaker -> s.isMongo26WriteOp }

}

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
class PrimaryAvailable private[reactivemongo] (
  val metadata: ProtocolMetadata,
  private[reactivemongo] val setName: Option[String],
  private[reactivemongo] val isMongos: Boolean) extends Product with Serializable {

  @deprecated("Use the constructor with `isMongos`", "0.18.5")
  def this(
    metadata: ProtocolMetadata,
    setName: Option[String]) = this(metadata, setName, false)

  @inline def productArity: Int = tupled.productArity

  @inline def productElement(n: Int): Any = tupled.productElement(n)

  override def equals(that: Any): Boolean = that match {
    case other: PrimaryAvailable =>
      tupled == other.tupled

    case _ => false
  }

  override lazy val hashCode: Int = tupled.hashCode

  def canEqual(that: Any): Boolean = that match {
    case _: PrimaryAvailable => true
    case _                   => false
  }

  private lazy val tupled = Tuple3(metadata, setName, isMongos)
}

@deprecated("Internal: will be made private", "0.16.0")
object PrimaryAvailable extends scala.runtime.AbstractFunction1[ProtocolMetadata, PrimaryAvailable] {

  def apply(metadata: ProtocolMetadata): PrimaryAvailable =
    new PrimaryAvailable(metadata, None, false)

  def unapply(that: Any): Option[ProtocolMetadata] = that match {
    case a: PrimaryAvailable => Option(a.metadata)
    case _                   => None
  }
}

/** Message sent when the primary has been lost. */
@deprecated("Internal: will be made private", "0.16.0")
case object PrimaryUnavailable

@deprecated("Internal: will be made private", "0.16.0")
class SetAvailable private[reactivemongo] (
  val metadata: ProtocolMetadata,
  private[reactivemongo] val setName: Option[String],
  private[reactivemongo] val isMongos: Boolean)
  extends Product with Serializable {

  @deprecated("Use the constructor with `isMongos`", "0.18.5")
  def this(
    metadata: ProtocolMetadata, setName: Option[String]) =
    this(metadata, setName, false)

  lazy val productArity: Int = tupled.productArity

  @inline def productElement(n: Int): Any = tupled.productElement(n)

  override def equals(that: Any): Boolean = that match {
    case other: SetAvailable =>
      tupled == other.tupled

    case _ => false
  }

  override lazy val hashCode: Int = tupled.hashCode

  def canEqual(that: Any): Boolean = that match {
    case _: SetAvailable => true
    case _               => false
  }

  private lazy val tupled = Tuple3(metadata, setName, isMongos)
}

@deprecated("Internal: will be made private", "0.16.0")
object SetAvailable extends scala.runtime.AbstractFunction1[ProtocolMetadata, SetAvailable] {

  def apply(metadata: ProtocolMetadata): SetAvailable =
    new SetAvailable(metadata, None, false)

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

private[reactivemongo] case class PickNode(readPreference: ReadPreference) {
  private[actors] val promise = Promise[String]()

  /** The node name */
  private[reactivemongo] def future: Future[String] = promise.future
}
