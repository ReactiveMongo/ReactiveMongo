import java.net.SocketAddress

import java.util.{ ConcurrentModificationException, Queue, LinkedList }

import scala.reflect.ClassTag

import shaded.netty.buffer.ChannelBufferFactory

import shaded.netty.channel.{
  AbstractChannel,
  Channel,
  Channels,
  ChannelDownstreamHandler,
  ChannelEvent,
  ChannelFactory,
  ChannelFuture,
  ChannelHandler,
  ChannelHandlerContext,
  ChannelPipeline,
  ChannelPipelineException,
  ChannelSink,
  ChannelUpstreamHandler,
  DefaultChannelConfig,
  DefaultChannelPipeline,
  ExceptionEvent,
  MessageEvent,
  SucceededChannelFuture
}

object NettyEmbedder {
  // Inspired from https://github.com/netty/netty/blob/3.10/src/main/java/org/jboss/netty/handler/codec/embedder/EmbeddedChannelFactory.java
  private lazy val cf: ChannelFactory = new ChannelFactory {
    def newChannel(pipeline: ChannelPipeline): Channel = ???

    def releaseExternalResources() { /* No external resources */ }

    def shutdown() { /* Nothing to shutdown */ }
  }

  def ChannelFactory() = cf

  // Inspired from https://github.com/netty/netty/blob/3.10/src/main/java/org/jboss/netty/handler/codec/embedder/EmbeddedSocketAddress.java
  private lazy val addr = new SocketAddress {}
  def SocketAddress(): SocketAddress = addr

  // Inspired from https://github.com/netty/netty/blob/3.10/src/main/java/org/jboss/netty/handler/codec/embedder/AbstractCodecEmbedder.java#L229
  def ChannelPipeline(): ChannelPipeline = new DefaultChannelPipeline() {
    override def notifyHandlerException(e: ChannelEvent, t: Throwable) {
      var cause = t

      t.printStackTrace()

      while (cause.isInstanceOf[ChannelPipelineException] &&
        cause.getCause() != null) {
        cause = cause.getCause()
      }

      if (cause.isInstanceOf[CodecEmbedderException]) {
        throw t.asInstanceOf[CodecEmbedderException]
      } else throw new CodecEmbedderException(t)
    }
  }

  // Inspired from https://github.com/netty/netty/blob/3.10/src/main/java/org/jboss/netty/handler/codec/embedder/AbstractCodecEmbedder.java#L185
  def ChannelSink(productQueue: Queue[Object]) =
    new ChannelSink with ChannelUpstreamHandler {
      private def handleEvent(e: ChannelEvent): Unit = e match {
        case me: MessageEvent => {
          val offered = productQueue.offer(me.getMessage)
          assert(offered)
        }

        case ee: ExceptionEvent =>
          throw new CodecEmbedderException(ee.getCause)

        case _ => // OK
      }

      def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
        handleEvent(e)
      }

      def eventSunk(pipeline: ChannelPipeline, e: ChannelEvent) {
        handleEvent(e)
      }

      def exceptionCaught(
        pipeline: ChannelPipeline,
        e: ChannelEvent,
        cause: ChannelPipelineException): Unit = {
        def actualCause: Throwable = Option(cause.getCause).getOrElse(cause)
        throw new CodecEmbedderException(actualCause);
      }

      def execute(pipeline: ChannelPipeline, task: Runnable): ChannelFuture =
        try {
          task.run()
          return Channels.succeededFuture(pipeline.getChannel())
        } catch {
          case t: Throwable =>
            return Channels.failedFuture(pipeline.getChannel(), t);
        }
    }

  def EncoderEmbedder[E: ClassTag](chanId: Int, connected: Boolean, handlers: ChannelDownstreamHandler*) = new AbstractCodecEmbedder[E](chanId, connected, handlers) {
    def offer(input: Object): Boolean = {
      Channels.write(getChannel(), input).setSuccess()
      isEmpty()
    }
  }

  // ---

  def withChannel[T](chanId: Int, connected: Boolean, handler: ChannelEvent => Unit)(f: Channel => T): T = {
    lazy val em = NettyEmbedder.EncoderEmbedder(
      chanId,
      connected,
      new ChannelDownstreamHandler {
        def handleDownstream(ctx: ChannelHandlerContext, evt: ChannelEvent) =
          handler(evt)
      })

    try {
      f(em.getChannel())
    } finally {
      em.finish(); ()
    }
  }

  def withChannel[T](chanId: Int, handler: ChannelEvent => Unit)(f: Channel => T): T = withChannel(chanId, false, handler)(f)
}

// Inspired from https://github.com/netty/netty/blob/3.10/src/main/java/org/jboss/netty/handler/codec/embedder/EmbeddedChannel.java
class EmbeddedChannel(
  id: Int,
  coned: Boolean,
  pipeline: ChannelPipeline,
  sink: ChannelSink) extends AbstractChannel(
  id, null, NettyEmbedder.ChannelFactory(), pipeline, sink) {
  @volatile private var connected = coned

  val config = new DefaultChannelConfig()
  val localAddr = NettyEmbedder.SocketAddress()
  val remoteAddr = NettyEmbedder.SocketAddress()

  def getConfig() = config
  def getLocalAddress() = localAddr
  def getRemoteAddress() = remoteAddr
  def isBound() = true

  override def connect(remote: SocketAddress): ChannelFuture = {
    connected = true
    new SucceededChannelFuture(this)
  }

  def isConnected() = connected
}

final class CodecEmbedderException(
  msg: String, cause: Throwable) extends RuntimeException() {
  def this() = this(null, null)
  def this(msg: String) = this(msg, null)
  def this(cause: Throwable) = this(null, cause)
}

/**
 * @param chanId the channel ID
 * @param connected the initial connection status
 */
abstract class AbstractCodecEmbedder[E: ClassTag](
  chanId: Int,
  connected: Boolean,
  handlers: Seq[ChannelHandler]) {
  private val pipeline = NettyEmbedder.ChannelPipeline()
  private val productQueue: Queue[Object] = new LinkedList[Object]()
  private val sink = NettyEmbedder.ChannelSink(productQueue)

  configurePipeline(handlers)

  private val channel = new EmbeddedChannel(chanId, connected, pipeline, sink)

  fireInitialEvents()

  def this(
    chanId: Int,
    connected: Boolean,
    cbf: ChannelBufferFactory,
    handlers: ChannelHandler*) = {
    this(chanId, connected, handlers)
    channel.getConfig().setBufferFactory(cbf)
  }

  @inline private def fireInitialEvents(): Unit = {
    // Fire the typical initial events.
    Channels.fireChannelOpen(channel)
    Channels.fireChannelBound(channel, channel.getLocalAddress())
    Channels.fireChannelConnected(channel, channel.getRemoteAddress())
  }

  private def configurePipeline(handlers: Seq[ChannelHandler]) {
    if (handlers == null) {
      throw new NullPointerException("handlers")
    } else if (handlers.size == 0) {
      throw new IllegalArgumentException(s"handlers should contain at least one ${classOf[ChannelHandler].getSimpleName()}.")
    } else (0 until handlers.size).foreach { i =>
      val h: ChannelHandler = handlers(i)

      if (h == null) {
        throw new NullPointerException(s"handlers[$i]")
      } else pipeline.addLast(String.valueOf(i), h)
    }

    pipeline.addLast("SINK", sink)
  }

  def finish(): Boolean = {
    Channels.close(channel)

    Channels.fireChannelDisconnected(channel)
    Channels.fireChannelUnbound(channel)
    Channels.fireChannelClosed(channel)

    return !productQueue.isEmpty()
  }

  def getChannel(): Channel = channel

  protected def isEmpty(): Boolean = productQueue.isEmpty()

  def size(): Int = productQueue.size()

  def getPipeline(): ChannelPipeline = pipeline

  @SuppressWarnings(Array("unchecked"))
  def poll() = productQueue.poll().asInstanceOf[E]

  @SuppressWarnings(Array("unchecked"))
  def peek() = productQueue.peek().asInstanceOf[E]

  def pollAll(): Array[Object] = (0 until size()).map { i =>
    val product: E = poll()

    if (product == null) {
      throw new ConcurrentModificationException()
    }

    product.asInstanceOf[Object]
  }.toArray

  @SuppressWarnings(Array("unchecked"))
  def pollAll[T: ClassTag](a: Array[T]): Array[T] = {
    if (a == null) throw new NullPointerException("a")

    val sz = size()

    // Create a new array if the specified one is too small.
    val buf: Array[T] = if (a.length >= sz) a else Array.ofDim[T](sz)

    @annotation.tailrec
    def go(index: Int): Unit = poll() match {
      case null => ()

      case product =>
        buf(index) = product.asInstanceOf[T]
        go(index + 1)
    }

    // Put the terminator if necessary.
    if (buf.length > sz) buf(sz) = null.asInstanceOf[T]

    buf
  }
}
