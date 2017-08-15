package reactivemongo.core.actors

import scala.concurrent.{ Future, Promise }

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
sealed trait ExpectingResponse {
  private[actors] val promise: Promise[Response] = Promise()

  /** The future response of this request. */
  val future: Future[Response] = promise.future
}

object ExpectingResponse {
  def unapply(that: Any): Option[Promise[Response]] = that match {
    case req @ RequestMakerExpectingResponse(_, _) => Some(req.promise)
    case req @ CheckedWriteRequestExpectingResponse(_) => Some(req.promise)
    case _ => None
  }
}

/**
 * A request expecting a response.
 *
 * @param requestMaker the request maker
 * @param isMongo26WriteOp true if the operation is a MongoDB 2.6 write one
 */
case class RequestMakerExpectingResponse(
  requestMaker: RequestMaker,
  isMongo26WriteOp: Boolean) extends ExpectingResponse

/**
 * A checked write request expecting a response.
 *
 * @param checkedWriteRequest The request maker.
 */
case class CheckedWriteRequestExpectingResponse(
  checkedWriteRequest: CheckedWriteRequest) extends ExpectingResponse

/**
 * Message to close all active connections.
 * The MongoDBSystem actor must not be used after this message has been sent.
 */
case object Close

/**
 * Message to send in order to get warned the next time a primary is found.
 */
private[reactivemongo] case object ConnectAll
private[reactivemongo] case object RefreshAll
private[reactivemongo] case class ChannelConnected(channelId: Int)

private[reactivemongo] sealed trait ChannelUnavailable { def channelId: Int }

private[reactivemongo] object ChannelUnavailable {
  def unapply(cu: ChannelUnavailable): Option[Int] = Some(cu.channelId)
}

private[reactivemongo] case class ChannelDisconnected(
  channelId: Int) extends ChannelUnavailable

private[reactivemongo] case class ChannelClosed(
  channelId: Int) extends ChannelUnavailable

/** Message sent when the primary has been discovered. */
case class PrimaryAvailable(metadata: ProtocolMetadata)

/** Message sent when the primary has been lost. */
case object PrimaryUnavailable

// TODO
case class SetAvailable(metadata: ProtocolMetadata)

// TODO
case object SetUnavailable

/** Register a monitor. */
case object RegisterMonitor

/** MongoDBSystem has been shut down. */
case object Closed
case object GetLastMetadata

/**
 * A message to send to a MonitorActor to be warned when a primary has been discovered.
 */
@deprecated(message = "Will be removed", since = "0.11.10")
case object WaitForPrimary

private[actors] object IsMasterResponse {
  def unapply(response: Response): Option[Response] =
    if (RequestId.isMaster accepts response) Some(response) else None
}
