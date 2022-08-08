package reactivemongo.core.nodeset

import scala.collection.immutable.{ ListSet, Set }

import scala.util.{ Failure, Success, Try }

import scala.util.control.NonFatal

import reactivemongo.io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  DefaultChannelPromise
}

import reactivemongo.core.protocol.Request

import reactivemongo.api.Compressor

/**
 * @param signaling if true it's a signaling connection (not for r/w ops)
 */
private[reactivemongo] class Connection(
    val channel: Channel,
    val status: ConnectionStatus,
    val authenticated: Set[Authenticated],
    val authenticating: Option[Authenticating],
    val signaling: Boolean) {

  def send(
      message: Request,
      writeConcern: Request,
      compression: ListSet[Compressor]
    ): ChannelFuture =
    withPrepared(compression, message) { (msg, onComplete) =>
      channel
        .write(msg)
        .addListener(new ChannelFutureListener {
          def operationComplete(op: ChannelFuture): Unit = {
            onComplete()
            ()
          }
        })

      channel.writeAndFlush(writeConcern)
    }

  def send(
      message: Request,
      compression: ListSet[Compressor]
    ): ChannelFuture = withPrepared(compression, message) { (msg, onComplete) =>
    channel
      .writeAndFlush(msg)
      .addListener(new ChannelFutureListener {
        def operationComplete(op: ChannelFuture): Unit = {
          onComplete()
          ()
        }
      })
  }

  /** Returns whether the `user` is authenticated against the `db`. */
  def isAuthenticated(db: String, user: String): Boolean =
    authenticated.exists(auth => auth.user == user && auth.db == db)

  private def withPrepared(
      compression: ListSet[Compressor],
      request: Request
    ): Function2[Request, () => Any, ChannelFuture] => ChannelFuture =
    compression.headOption match {
      case Some(compressor) =>
        compress(request, compressor, compression.tail) match {
          case Success(prepared) =>
            _(prepared, () => prepared.payload.release())

          case Failure(cause) => {
            val failed = new DefaultChannelPromise(channel)

            failed.setFailure(cause)

            _ => failed
          }
        }

      case _ => _(request, () => {})
    }

  @annotation.tailrec
  private def compress(
      request: Request,
      next: Compressor,
      alternatives: ListSet[Compressor]
    ): Try[Request] =
    Request.compress(request, next, channel.alloc.directBuffer(_: Int)) match {
      case compressed @ Success(_) =>
        try {
          compressed
        } finally {
          request.payload.release()
          ()
        }

      case failed @ Failure(_) =>
        alternatives.headOption match {
          case Some(c) =>
            compress(request, c, alternatives.tail)

          case _ => {
            if (request.payload.refCnt > 0) try {
              request.payload.release()
            } catch {
              case NonFatal(_) =>
            }

            failed
          }
        }
    }

  // ---

  @SuppressWarnings(Array("VariableShadowing"))
  private[core] def copy(
      channel: Channel = this.channel,
      status: ConnectionStatus = this.status,
      authenticated: Set[Authenticated] = this.authenticated,
      authenticating: Option[Authenticating] = this.authenticating
    ): Connection =
    new Connection(channel, status, authenticated, authenticating, signaling)

  private[nodeset] def tupled =
    Tuple4(channel, status, authenticated, authenticating)

  override def equals(that: Any): Boolean = that match {
    case other: Connection =>
      (tupled == other.tupled) && signaling == other.signaling

    case _ => false
  }

  override def hashCode: Int = tupled.hashCode + signaling.hashCode

  override def toString: String = s"Connection$tupled"
}
