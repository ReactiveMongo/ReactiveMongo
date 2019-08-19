package reactivemongo.core.actors

import scala.concurrent.Promise

import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.nodeset.Node
import reactivemongo.core.protocol.{ Request, Response }

private[actors] case class AwaitingResponse(
  request: Request,
  channelID: ChannelId,
  promise: Promise[Response],
  nodeResolution: Option[Promise[Node]],
  isGetLastError: Boolean,
  isMongo26WriteOp: Boolean) {
  @inline def requestID: Int = request.requestID

  private var _retry = 0 // TODO: Refactor as property

  // TODO: Refactor as Property
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
    nodeResolution: Option[Promise[Node]] = this.nodeResolution,
    isGetLastError: Boolean = this.isGetLastError,
    isMongo26WriteOp: Boolean = this.isMongo26WriteOp): AwaitingResponse =
    AwaitingResponse(request, channelID, promise, nodeResolution,
      isGetLastError, isMongo26WriteOp)

}

private[actors] object AwaitingResponse extends scala.runtime.AbstractFunction5[Request, ChannelId, Promise[Response], Boolean, Boolean, AwaitingResponse] {
  @deprecated("Use complete constructor", "0.18.5")
  def apply(
    request: Request,
    channelID: ChannelId,
    promise: Promise[Response],
    isGetLastError: Boolean,
    isMongo26WriteOp: Boolean): AwaitingResponse =
    new AwaitingResponse(
      request, channelID, promise, None, isGetLastError, isMongo26WriteOp)
}
