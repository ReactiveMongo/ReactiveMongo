package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.ByteBuf
import reactivemongo.io.netty.channel.ChannelHandlerContext

private[reactivemongo] class RequestEncoder
  extends reactivemongo.io.netty.handler.codec.MessageToByteEncoder[Request] {
  def encode(
    ctx: ChannelHandlerContext,
    message: Request,
    buffer: ByteBuf): Unit = {

    message writeTo buffer

    ()
  }
}
