package reactivemongo.core.actors

import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.ReadPreference

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.protocol.{ RequestMaker, Response, ProtocolMetadata }

/**
 * A request expecting a response.
 *
 * @param requestMaker the request maker
 */
private[reactivemongo] final class ExpectingResponse(
    val requestMaker: RequestMaker,
    val pinnedNode: Option[String]) {
  val promise: Promise[Response] = Promise()

  /** The future response of this request. */
  val future: Future[Response] = promise.future

  override def equals(that: Any): Boolean = that match {
    case other: ExpectingResponse =>
      tupled == other.tupled

    case _ =>
      false
  }

  override lazy val hashCode: Int = tupled.hashCode

  private lazy val tupled = requestMaker -> pinnedNode
}

private[reactivemongo] sealed class Close {
  def source: String = "unknown"

  private[reactivemongo] def timeout: FiniteDuration =
    FiniteDuration(10, "seconds")
}

/**
 * Message to close all active connections.
 * The MongoDBSystem actor must not be used after this message has been sent.
 */
private[reactivemongo] case object Close extends Close {

  @SuppressWarnings(Array("VariableShadowing"))
  def apply(src: String, timeout: FiniteDuration): Close = {
    def t = timeout
    new Close {
      override val source = src
      override val timeout = t
    }
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
private[reactivemongo] class PrimaryAvailable(
    val metadata: ProtocolMetadata,
    val setName: Option[String],
    val isMongos: Boolean) {

  override def equals(that: Any): Boolean = that match {
    case other: PrimaryAvailable =>
      tupled == other.tupled

    case _ => false
  }

  override lazy val hashCode: Int = tupled.hashCode

  private lazy val tupled = Tuple3(metadata, setName, isMongos)
}

private[reactivemongo] object PrimaryAvailable {

  def apply(metadata: ProtocolMetadata): PrimaryAvailable =
    new PrimaryAvailable(metadata, None, false)

  def unapply(that: Any): Option[ProtocolMetadata] = that match {
    case a: PrimaryAvailable => Option(a.metadata)
    case _                   => None
  }
}

/** Message sent when the primary has been lost. */
private[reactivemongo] case object PrimaryUnavailable

private[reactivemongo] class SetAvailable(
    val metadata: ProtocolMetadata,
    val setName: Option[String],
    val isMongos: Boolean) {

  override def equals(that: Any): Boolean = that match {
    case other: SetAvailable =>
      tupled == other.tupled

    case _ => false
  }

  override lazy val hashCode: Int = tupled.hashCode

  private lazy val tupled = Tuple3(metadata, setName, isMongos)
}

private[reactivemongo] object SetAvailable {

  def apply(metadata: ProtocolMetadata): SetAvailable =
    new SetAvailable(metadata, None, false)

  def unapply(that: Any): Option[ProtocolMetadata] = that match {
    case a: SetAvailable => Option(a.metadata)
    case _               => None
  }
}

private[reactivemongo] case object SetUnavailable

/** Register a monitor. */
private[reactivemongo] case object RegisterMonitor

/** MongoDBSystem has been shut down. */
private[reactivemongo] case object Closed

private[reactivemongo] case object GetLastMetadata

private[reactivemongo] case class PickNode(readPreference: ReadPreference) {
  private[actors] val promise = Promise[String]()

  /** The node name */
  def future: Future[String] = promise.future
}
