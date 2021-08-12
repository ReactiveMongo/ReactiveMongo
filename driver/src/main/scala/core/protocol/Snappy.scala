package reactivemongo.core.protocol

import scala.util.Try

import reactivemongo.io.netty.buffer.ByteBuf

private[reactivemongo] final class Snappy {
  private val underlying = new Snappy()

  def decode(in: ByteBuf, out: ByteBuf): Try[Int] = Try {
    val before = out.writerIndex

    underlying.decode(in, out)

    out.writerIndex - before
  }

  def encode(in: ByteBuf, out: ByteBuf): Try[Unit] = Try {
    underlying.encode(in, out)

    ()
  }
}
