package reactivemongo.core.actors

import scala.concurrent.Promise

import reactivemongo.core.protocol.{ Request, Response }

private[actors] case class AwaitingResponse(
  request: Request,
  channelID: Int,
  promise: Promise[Response],
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
  def retriable(max: Int): Option[Int => AwaitingResponse] =
    if (!promise.isCompleted && _retry >= max) None else Some({ chanId: Int =>
      val req = copy(this.request, channelID = chanId)

      req._retry = _retry + 1
      req._writeConcern = _writeConcern

      req
    })

  def copy(
    request: Request = this.request,
    channelID: Int = this.channelID,
    promise: Promise[Response] = this.promise,
    isGetLastError: Boolean = this.isGetLastError,
    isMongo26WriteOp: Boolean = this.isMongo26WriteOp): AwaitingResponse =
    AwaitingResponse(request, channelID, promise,
      isGetLastError, isMongo26WriteOp)

  @deprecated(message = "Use [[copy]] with `Request`", since = "0.12-RC1")
  def copy(
    requestID: Int,
    channelID: Int,
    promise: Promise[Response],
    isGetLastError: Boolean,
    isMongo26WriteOp: Boolean): AwaitingResponse = {
    val req = copy(
      this.request,
      channelID = channelID,
      promise = promise,
      isGetLastError = isGetLastError,
      isMongo26WriteOp = isMongo26WriteOp)

    req._retry = this._retry
    req._writeConcern = this._writeConcern

    req
  }
}
