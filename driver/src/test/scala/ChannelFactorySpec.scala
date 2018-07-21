import scala.concurrent.Promise

import akka.actor.Actor

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.io.netty.channel.{ ChannelFuture, ChannelFutureListener }

import reactivemongo.core.protocol.{
  MessageHeader,
  Reply,
  Response,
  ResponseInfo
}

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.bson.{
  BSONIsMasterCommand,
  BSONIsMasterCommandImplicits
}, BSONIsMasterCommandImplicits.IsMasterResultReader
import BSONIsMasterCommand.IsMasterResult

import org.specs2.specification.AfterAll
import org.specs2.concurrent.ExecutionEnv

import _root_.tests.{ Common, NettyEmbedder }

class ChannelFactorySpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification with AfterAll {

  "Channel factory" title

  import reactivemongo.api.tests.{
    channelFactory,
    getBytes,
    initChannel,
    isMasterRequest,
    isMasterResponse,
    releaseChannelFactory,
    createChannel
  }
  import Common.timeout
  implicit def actorSys = Common.driver.system

  val factory = channelFactory("sup-1", "con-2", Common.DefaultOptions)

  section("unit")

  "Embedded channel" should {
    import reactivemongo.io.netty.channel.DefaultChannelId

    "manage isMaster command" in {
      val cid = DefaultChannelId.newInstance()
      val expectedResp = {
        val documents = Unpooled.buffer(
          isMasterRespBytes.size, isMasterRespBytes.size)

        documents.writeBytes(isMasterRespBytes)

        Response(
          MessageHeader(205, 13, 0, 1), Reply(8, 0, 0, 1),
          documents, ResponseInfo(cid))
      }

      val response = Promise[Response]()
      def actor = new Actor {
        val receive: Receive = {
          case resp: Response if isMasterResponse(resp) => {
            response.success(resp)
            ()
          }

          case msg =>
            Common.logger.info(s"Unhandled message: $msg")
        }
      }

      val actorRef = akka.testkit.TestActorRef(actor, "test1")

      val req = isMasterRequest()
      val reqBytes: Array[Byte] = {
        val buf = Unpooled.buffer(req.size, req.size)
        req.writeTo(buf)
        getBytes(buf, req.size)
      }
      val sentRequest = Promise[Array[Byte]]()

      NettyEmbedder.withChannel2(cid, true) { chan =>
        initChannel(factory, chan, "foo", 27017, actorRef)

        chan.writeAndFlush(req).addListener(printOnError).
          addListener(new ChannelFutureListener {
            def operationComplete(op: ChannelFuture): Unit = {
              if (!sentRequest.isCompleted && op.isSuccess) {
                val buf = chan.readOutbound[ByteBuf]

                sentRequest.success(getBytes(buf, buf.readableBytes))

                buf.release()

                chan.writeOneInbound(expectedResp)

                ()
              }
            }
          })

        // ---

        chan.isRegistered and {
          sentRequest.future must beEqualTo(reqBytes).await(1, timeout)
        } and {
          response.future must beEqualTo(expectedResp).await(1, timeout)
        }
      }
    }
  }

  Common.nettyNativeArch.foreach { arch =>
    s"Netty native support for $arch" should {
      "be loaded" in {
        def actor = new Actor {
          val receive: Receive = {
            case _ => ???
          }
        }

        val actorRef = akka.testkit.TestActorRef(actor, "test3")
        lazy val chan = createChannel(factory, actorRef, "foo", 27017)

        arch must beLike[String] {
          case "osx" =>
            chan.close(); chan.getClass.getName must startWith(
              "reactivemongo.io.netty.channel.kqueue.KQueue")

          case "linux" =>
            chan.close(); chan.getClass.getName must startWith(
              "reactivemongo.io.netty.channel.epoll.Epoll")
        }
      }
    }
  }
  section("unit")

  s"""Channel ${Common.nettyNativeArch getOrElse "nio"}""" should {
    "manage isMaster command" in {
      val result = Promise[IsMasterResult]()
      val chanConnected = Promise[Unit]()

      def actor = new Actor {
        val receive: Receive = {
          case msg if (msg.toString startsWith "ChannelConnected(") =>
            chanConnected.success(Common.logger.info(s"NIO $msg")); ()

          case resp: Response if (
            chanConnected.isCompleted && isMasterResponse(resp)) => {

            result.tryComplete(scala.util.Try {
              val bson = BSONSerializationPack.readAndDeserialize(
                resp, BSONSerializationPack.IdentityReader)

              IsMasterResultReader.read(bson)
            })

            Common.logger.info(s"NIO isMasterResponse: $resp")
          }

          case msg =>
            Common.logger.warn(s"Unhandled message [connected: ${chanConnected.isCompleted}]: $msg")
        }
      }

      val actorRef = akka.testkit.TestActorRef(actor, "test2")
      val chan = createChannel(factory, actorRef,
        host = Common.primaryHost.takeWhile(_ != ':'),
        port = Common.primaryHost.dropWhile(_ != ':').drop(1).toInt)

      chanConnected.future must beEqualTo({}).await(1, timeout) and {
        chan.writeAndFlush(isMasterRequest()).addListener(printOnError)

        result.future must beLike[IsMasterResult] {
          case IsMasterResult(true, 16777216, 48000000, _,
            Some(_), min, max, _, _) => min must be_<(max)
        }.await(1, timeout) and {
          if (!chan.closeFuture.isDone) {
            chan.close()
          }

          actorRef.stop()

          Common.nettyNativeArch.fold(ok) {
            case "osx" => chan.getClass.getName must startWith(
              "reactivemongo.io.netty.channel.kqueue.KQueue")

            case "linux" => chan.getClass.getName must startWith(
              "reactivemongo.io.netty.channel.epoll.Epoll")

          }
        }
      }
    }
  }

  // ---

  lazy val isMasterRespBytes = Array[Byte](-87, 0, 0, 0, 8, 105, 115, 109, 97, 115, 116, 101, 114, 0, 1, 16, 109, 97, 120, 66, 115, 111, 110, 79, 98, 106, 101, 99, 116, 83, 105, 122, 101, 0, 0, 0, 0, 1, 16, 109, 97, 120, 77, 101, 115, 115, 97, 103, 101, 83, 105, 122, 101, 66, 121, 116, 101, 115, 0, 0, 108, -36, 2, 16, 109, 97, 120, 87, 114, 105, 116, 101, 66, 97, 116, 99, 104, 83, 105, 122, 101, 0, -24, 3, 0, 0, 9, 108, 111, 99, 97, 108, 84, 105, 109, 101, 0, -88, 86, 56, -100, 95, 1, 0, 0, 16, 109, 97, 120, 87, 105, 114, 101, 86, 101, 114, 115, 105, 111, 110, 0, 5, 0, 0, 0, 16, 109, 105, 110, 87, 105, 114, 101, 86, 101, 114, 115, 105, 111, 110, 0, 0, 0, 0, 0, 8, 114, 101, 97, 100, 79, 110, 108, 121, 0, 0, 1, 111, 107, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0)

  def afterAll(): Unit = {
    releaseChannelFactory(factory, scala.concurrent.Promise())
  }

  def printOnError = new ChannelFutureListener {
    def operationComplete(op: ChannelFuture): Unit = {
      if (!op.isSuccess) op.cause.printStackTrace()
    }
  }
}
