package reactivemongo.core.actors

import scala.concurrent.Promise

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.protocol.{ Request, Response }

private[actors] final class AwaitingResponse(
  val request: Request,
  val channelID: ChannelId,
  val promise: Promise[Response],
  val isGetLastError: Boolean,
  val pinnedNode: Option[String],
  val retry: Int = 0,
  val writeConcern: Option[Request] = None) {

  @inline def requestID: Int = request.requestID

  /**
   * If this is not already completed and,
   * if the current retry count is less then the maximum.
   */
  def retriable(max: Int): Option[ChannelId => AwaitingResponse] =
    if (!promise.isCompleted && retry >= max) None
    else Some({ (id: ChannelId) =>
      copy(
        channelID = id,
        retry = this.retry + 1)
    })

  @SuppressWarnings(Array("VariableShadowing"))
  private def copy(
    channelID: ChannelId,
    retry: Int,
    request: Request = this.request,
    promise: Promise[Response] = this.promise,
    isGetLastError: Boolean = this.isGetLastError,
    pinnedNode: Option[String] = this.pinnedNode,
    writeConcern: Option[Request] = this.writeConcern): AwaitingResponse =
    new AwaitingResponse(request, channelID, promise,
      isGetLastError, pinnedNode, retry, writeConcern)

  override def equals(that: Any): Boolean = that match {
    case other: AwaitingResponse =>
      tupled == other.tupled

    case _ =>
      false
  }

  override lazy val hashCode: Int = tupled.hashCode

  private lazy val tupled = Tuple7(
    request, channelID, promise, isGetLastError,
    pinnedNode, retry, writeConcern)

}

private[actors] object AwaitingResponse {
  def unapply(req: AwaitingResponse): Option[(Request, ChannelId, Promise[Response], Boolean)] = Some(Tuple4(req.request, req.channelID, req.promise, req.isGetLastError))
}
