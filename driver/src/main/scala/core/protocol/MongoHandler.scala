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

  override def channelActive(ctx: ChannelHandlerContext) {
    log(ctx, "Channel is active")

    last = System.currentTimeMillis()

    receiver ! ChannelConnected(ctx.channel.id)

    super.channelActive(ctx)
  }

  override def channelIdle(ctx: ChannelHandlerContext, e: IdleStateEvent) = {
    val now = System.currentTimeMillis()

    log(ctx, s"Channel has been inactive for ${now - last} (last = $last)")

    last = System.currentTimeMillis()

    ctx.channel.close()

    super.channelIdle(ctx, e)
  }

  override def channelInactive(ctx: ChannelHandlerContext) {
    val chan = ctx.channel

    if (chan.remoteAddress != null) {
      log(ctx, "Channel is closed")
    }

    last = System.currentTimeMillis()

    receiver ! ChannelDisconnected(chan.id)

    super.channelInactive(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any) {
    last = System.currentTimeMillis()

    //println("_channelRead")

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
    promise: ChannelPromise) {
    log(ctx, s"Channel is requested to write")

    last = System.currentTimeMillis()

    super.write(ctx, msg, promise)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    log(ctx, s"Error on channel #${ctx.channel.id}", cause)

    //super.exceptionCaught(ctx, cause) - Do not bubble as it's the last handler
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


  override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
    println(s"_REMOVED #${ctx.channel.id}")
    super.handlerRemoved(ctx)
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    println(s"_ADDED #${ctx.channel.id} ? ${ctx.channel.isActive}")
    super.handlerAdded(ctx)
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
