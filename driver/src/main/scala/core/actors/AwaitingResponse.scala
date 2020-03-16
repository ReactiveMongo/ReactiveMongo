package reactivemongo.core.actors

import scala.concurrent.Promise

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.protocol.{ Request, Response }

private[actors] class AwaitingResponse(
  val request: Request,
  val channelID: ChannelId,
  val promise: Promise[Response],
  val isGetLastError: Boolean,
  val isMongo26WriteOp: Boolean,
  val pinnedNode: Option[String]) {

  @inline def requestID: Int = request.requestID

  private var _retry = 0 // TODO#1.1: Refactor as property

  // TODO#1.1: Refactor as Property
  var _writeConcern: Option[Request] = None
  def withWriteConcern(wc: Request): AwaitingResponse = {
    _writeConcern = Some(wc)
    this
  }
  def getWriteConcern: Option[Request] = _writeConcern

  /**
   * If this is not already completed and,
   * if the current retry count is less then the maximum.
   */
  def retriable(max: Int): Option[ChannelId => AwaitingResponse] =
    if (!promise.isCompleted && _retry >= max) None else Some({ id: ChannelId =>
      val req = copy(this.request, channelID = id)

      req._retry = _retry + 1
      req._writeConcern = _writeConcern

      req
    })

  def copy(
    request: Request = this.request,
    channelID: ChannelId = this.channelID,
    promise: Promise[Response] = this.promise,
    isGetLastError: Boolean = this.isGetLastError,
    isMongo26WriteOp: Boolean = this.isMongo26WriteOp,
    pinnedNode: Option[String] = this.pinnedNode): AwaitingResponse =
    new AwaitingResponse(request, channelID, promise,
      isGetLastError, isMongo26WriteOp, pinnedNode)

  override def equals(that: Any): Boolean = that match {
    case other: AwaitingResponse =>
      tupled == other.tupled

    case _ =>
      false
  }

  override lazy val hashCode: Int = tupled.hashCode

  private lazy val tupled = Tuple6(request, this.channelID, promise,
    isGetLastError, isMongo26WriteOp, pinnedNode)

}

private[actors] object AwaitingResponse {
  def unapply(req: AwaitingResponse): Option[(Request, ChannelId, Promise[Response], Boolean, Boolean)] = Some(Tuple5(req.request, req.channelID, req.promise, req.isGetLastError, req.isMongo26WriteOp))
}
