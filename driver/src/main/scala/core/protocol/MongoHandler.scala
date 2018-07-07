package reactivemongo.core.protocol

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef

import shaded.netty.channel.{
  ChannelHandlerContext,
  ChannelPromise
}
import shaded.netty.handler.timeout.IdleStateEvent

import reactivemongo.core.actors.{ ChannelConnected, ChannelDisconnected }

import reactivemongo.util.LazyLogger

private[reactivemongo] class MongoHandler(
  supervisor: String,
  connection: String,
  receiver: ActorRef,
  idleTimeMS: Long) extends shaded.netty.handler.timeout.IdleStateHandler(
  idleTimeMS, idleTimeMS, idleTimeMS, TimeUnit.MILLISECONDS) {

  private var last: Long = -1L

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    log(ctx, "Channel is active")

    last = System.currentTimeMillis()

    receiver ! ChannelConnected(ctx.channel.id)

    super.channelActive(ctx)
  }

  override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent) = {
    if (last != -1L) {
      val now = System.currentTimeMillis()

      log(ctx, s"Channel has been inactive for ${now - last} (last = $last)")
    }

    ctx.channel.close() // configured timeout - See channelInactive

    super.channelIdle(ctx, e)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    val now = System.currentTimeMillis()

    if (last != -1) {
      val chan = ctx.channel

      if (chan.remoteAddress != null) {
        log(ctx, s"Channel is closed: $last")
      }

      last = now

      receiver ! ChannelDisconnected(chan.id)
    }

    super.channelInactive(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    last = System.currentTimeMillis()

    msg match {
      case response: Response => {
        log(ctx, s"Channel received message $response; Will be send to ${receiver.path}")

        receiver ! response

        //super.channelRead(ctx, msg) - Do not bubble as it's the last handler
      }

      case _ => {
        log(ctx, s"Unexpected message: $msg")
        //super.channelRead(ctx, msg)
      }
    }
  }

  override def write(
    ctx: ChannelHandlerContext,
    msg: Any,
    promise: ChannelPromise): Unit = {
    log(ctx, s"Channel is requested to write")

    last = System.currentTimeMillis()

    super.write(ctx, msg, promise)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    log(ctx, s"Error on channel #${ctx.channel.id}", cause)

    //super.exceptionCaught(ctx, cause) - Do not bubble as it's the last handler
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    if (ctx.channel.isActive) {
      channelActive(ctx)
    }

    super.handlerAdded(ctx)
  }

  /*
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    println("_READ_COMP")
    super.channelReadComplete(ctx)
  }

  override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
    println(s"_REG #${ctx.channel.id}")
    super.channelRegistered(ctx)
  }

  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
    println(s"_UNREG #${ctx.channel.id}")
    super.channelUnregistered(ctx)
  }

  override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
    println(s"_REMOVED #${ctx.channel.id}")
    super.handlerRemoved(ctx)
  }
   */

  @inline def log(ctx: ChannelHandlerContext, s: String) =
    MongoHandler.logger.trace(
      s"[$supervisor/$connection] $s (channel ${ctx.channel})")

  @inline def log(ctx: ChannelHandlerContext, s: String, cause: Throwable) =
    MongoHandler.logger.trace(
      s"[$supervisor/$connection] $s (channel ${ctx.channel})", cause)
}

private[reactivemongo] object MongoHandler {
  val logger = LazyLogger("reactivemongo.core.protocol.MongoHandler")
}
