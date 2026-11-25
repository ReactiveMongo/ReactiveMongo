import java.net.{ InetSocketAddress, ServerSocket, Socket }
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }

import scala.collection.immutable.ListSet
import scala.util.Try
import scala.util.control.NonFatal

import scala.concurrent.{ Await, Future }

import reactivemongo.api.{ Compressor, MongoConnection }
import reactivemongo.api.bson.BSONDocument

import org.specs2.concurrent.ExecutionEnv

final class RequestRetrySpec(
    implicit
    ee: ExecutionEnv)
    extends org.specs2.mutable.Specification {

  "Request retry".title

  sequential

  import tests.Common
  import Common.{ timeout, DefaultOptions }

  "Request retried after the channel is lost" should {
    "be fully resent and keep the connection usable" >> {
      "without compression" in {
        spec(ListSet.empty)
      }

      "with snappy compression" in {
        spec(ListSet(Compressor.Snappy))
      }
    }
  }

  // ---

  private def awaitAnsweredChannels(
      proxy: DropOnceProxy,
      expected: Int
    ): Int = {
    val deadline = timeout.fromNow

    while (proxy.answeredChannels < expected && deadline.hasTimeLeft()) {
      Thread.sleep(50)
    }

    proxy.answeredChannels
  }

  private def awaitDropped(proxy: DropOnceProxy): Boolean = {
    val deadline = timeout.fromNow

    while (!proxy.dropped && deadline.hasTimeLeft()) {
      Thread.sleep(10)
    }

    proxy.dropped
  }

  private def spec(compressors: ListSet[Compressor]) = {
    val marker =
      s"retry${System.identityHashCode(compressors)}${System.nanoTime()}"
    val proxy = new DropOnceProxy(Common.primaryHost, marker)

    val options = DefaultOptions
      .copy(nbChannelsPerNode = 3, minIdleChannelsPerNode = 3)
      .withCompressors(compressors)

    val connection: MongoConnection = Await.result(
      Common.driver.connect(List(s"localhost:${proxy.port}"), options),
      timeout
    )

    try {
      val db = Await.result(connection.database(Common.commonDb), timeout)
      val retried = db.collection(marker)
      val other = db.collection(s"other${System.nanoTime()}")

      def followingRequests: Future[Seq[Option[BSONDocument]]] =
        Future.sequence((1 to 20).map { i =>
          other.find(BSONDocument("i" -> i)).one[BSONDocument]
        })

      def warmUp(): Int = {
        Await.result(followingRequests, timeout)

        awaitAnsweredChannels(proxy, 3)
      }

      warmUp() must be_>=(3) and {
        val retriedResult = retried.find(BSONDocument.empty).one[BSONDocument]

        awaitDropped(proxy) must beTrue and {
          val following = followingRequests

          val retriedOutcome = Try(Await.result(retriedResult, timeout))
          val followingOutcome = Try(Await.result(following, timeout))

          proxy.malformedMessages must beEmpty and {
            retriedOutcome must beSuccessfulTry(Option.empty[BSONDocument])
          } and {
            followingOutcome.map(_.size) must beSuccessfulTry(20)
          }
        }
      }
    } finally {
      try {
        Await.result(connection.close()(timeout), timeout)
      } catch {
        case NonFatal(_) =>
      }

      proxy.close()
    }
  }
}

/**
 * Forwards MongoDB wire messages as-is, but closes the client connection
 * (without forwarding) on the first message containing `marker`.
 */
final class DropOnceProxy(remote: String, marker: String) {
  private val markerBytes = marker.getBytes(StandardCharsets.UTF_8)
  private val dropping = new AtomicBoolean(false)

  @volatile private var running = true

  private val server = new ServerSocket(0)

  val port: Int = server.getLocalPort

  def dropped: Boolean = dropping.get

  private val answered = new AtomicInteger(0)

  /** Number of client connections that received at least one reply */
  def answeredChannels: Int = answered.get

  private val malformed =
    new java.util.concurrent.ConcurrentLinkedQueue[String]()

  /** Forwarded OP_MSG messages the server would reject with code 40432 */
  def malformedMessages: List[String] = {
    import scala.jdk.CollectionConverters._

    malformed.asScala.toList
  }

  private val remoteAddr: InetSocketAddress = remote.split(":") match {
    case Array(host, p) => new InetSocketAddress(host, p.toInt)
    case _              => new InetSocketAddress(remote, 27017)
  }

  private def daemon(name: String)(f: => Unit): Unit = {
    val t = new Thread(() => f, name)

    t.setDaemon(true)
    t.start()
  }

  daemon("drop-once-proxy-accept") {
    while (running) {
      try {
        val client = server.accept()
        val backend = new Socket(remoteAddr.getHostString, remoteAddr.getPort)

        daemon("drop-once-proxy-s2c") { pump(backend, client) }
        daemon("drop-once-proxy-c2s") { forwardMessages(client, backend) }
      } catch {
        case NonFatal(_) =>
      }
    }
  }

  private def pump(from: Socket, to: Socket): Unit = {
    val buf = new Array[Byte](8192)

    try {
      val in = from.getInputStream
      val out = to.getOutputStream

      var n = in.read(buf)
      var first = true

      while (n >= 0) {
        out.write(buf, 0, n)
        out.flush()

        if (first) {
          answered.incrementAndGet()
          first = false
        }

        n = in.read(buf)
      }
    } catch {
      case NonFatal(_) =>
    } finally {
      closeQuietly(from)
      closeQuietly(to)
    }
  }

  private def forwardMessages(client: Socket, backend: Socket): Unit = {
    try {
      val in = new java.io.DataInputStream(client.getInputStream)
      val out = backend.getOutputStream
      val lenBytes = new Array[Byte](4)

      while (true) {
        in.readFully(lenBytes)

        val len = (lenBytes(0) & 0xff) | ((lenBytes(1) & 0xff) << 8) |
          ((lenBytes(2) & 0xff) << 16) | ((lenBytes(3) & 0xff) << 24)

        val msg = new Array[Byte](len)

        System.arraycopy(lenBytes, 0, msg, 0, 4)
        in.readFully(msg, 4, len - 4)

        if (containsMarker(msg) && dropping.compareAndSet(false, true)) {
          throw new java.io.IOException("Dropping connection")
        }

        checkSectionKind(msg)

        out.write(msg)
        out.flush()
      }
    } catch {
      case NonFatal(_) =>
    } finally {
      closeQuietly(client)
      closeQuietly(backend)
    }
  }

  private def checkSectionKind(msg: Array[Byte]): Unit = {
    val opMsg = 2013
    val opCode = java.nio.ByteBuffer
      .wrap(msg, 12, 4)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .getInt

    if (opCode == opMsg && msg.length > 20) {
      val kind = msg(20).toInt

      if (kind != 0 && kind != 1) {
        malformed.add(s"Unknown section kind ${kind} (length = ${msg.length})")
      }
    }

    ()
  }

  private def containsMarker(msg: Array[Byte]): Boolean =
    msg.indexOfSlice(markerBytes) >= 0

  private def closeQuietly(s: Socket): Unit =
    try {
      s.close()
    } catch {
      case NonFatal(_) =>
    }

  def close(): Unit = {
    running = false
    server.close()
  }
}
