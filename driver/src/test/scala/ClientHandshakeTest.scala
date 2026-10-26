package reactivemongo

import scala.collection.immutable.ListSet

import reactivemongo.io.netty.channel.DefaultChannelId
import reactivemongo.io.netty.channel.embedded.EmbeddedChannel

import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.buffer.ReadableBuffer
import reactivemongo.api.bson.collection.BSONSerializationPack

import reactivemongo.core.actors.StandardDBSystem
import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeStatus,
  PingInfo
}
import reactivemongo.core.protocol.{ ProtocolMetadata, Query, Request }

import reactivemongo.api.MongoConnectionOptions

import reactivemongo.actors.testkit.TestActorRef

trait ClientHandshakeTest { _self: NodeSetSpec =>
  import reactivemongo.api.tests.{
    channelClosed,
    channelConnected,
    fakeResponse,
    nodeSet,
    updateNodeSet,
    RefreshAll
  }

  private val appName = "probe-app"

  private lazy val handshakeOpts = MongoConnectionOptions.default.copy(
    nbChannelsPerNode = 2,
    heartbeatFrequencyMS = 60000,
    appName = Some(appName)
  )

  def clientHandshakeSpec = "send the client metadata" >> {
    "first on a pooled connection, only once and before authentication" in {
      withHandshakeSystem { (ref, signaling, pooled) =>
        ref.receive(channelConnected(signaling.id))
        ref.receive(channelConnected(pooled.id))

        val pooledHello = commands(pooled)

        pooledHello must beLike[List[(Int, BSONDocument)]] {
          case (_, hello) :: Nil =>
            isHello(hello) must beTrue and {
              applicationName(hello) must beSome(appName)
            }
        } and {
          ref.receive(primaryResponse(ref, signaling))

          commands(pooled) must beLike[List[(Int, BSONDocument)]] {
            case (_, auth) :: _ =>
              auth.contains("saslStart") must beTrue
          }
        } and {
          ref.receive(RefreshAll)

          commands(pooled).count {
            case (_, cmd) => cmd.contains("client")
          } must_=== 0
        }
      }
    }

    "but ignore the handshake response of a pooled connection for monitoring" in {
      withHandshakeSystem { (ref, signaling, pooled) =>
        ref.receive(channelConnected(signaling.id))
        ref.receive(channelConnected(pooled.id))
        ref.receive(primaryResponse(ref, signaling))

        val before = probeNode(ref)

        val helloId = commands(pooled).collectFirst {
          case (id, cmd) if isHello(cmd) => id
        }

        helloId must beSome[Int].which { id =>
          ref.receive(
            fakeResponse(
              BSONDocument(
                "ok" -> 1,
                "ismaster" -> false,
                "secondary" -> true,
                "minWireVersion" -> 0,
                "maxWireVersion" -> 21
              ),
              reqID = id + 1,
              respTo = id,
              chanId = pooled.id
            )
          )

          val after = probeNode(ref)

          after.status must_=== NodeStatus.Primary and {
            after.pingInfo.ping must_=== before.pingInfo.ping
          } and {
            after.pingInfo.lastIsMasterId must_=== before.pingInfo.lastIsMasterId
          }
        }
      }
    }

    "first on the signaling connection, and again once reconnected" in {
      withHandshakeSystem { (ref, signaling, _) =>
        ref.receive(channelConnected(signaling.id))
        ref.receive(primaryResponse(ref, signaling))
        ref.receive(RefreshAll)

        val sent = commands(signaling)

        sent.headOption.map(_._2) must beSome[BSONDocument].which { first =>
          isHello(first) must beTrue and {
            applicationName(first) must beSome(appName)
          }
        } and {
          sent.count { case (_, cmd) => cmd.contains("client") } must_=== 1
        } and {
          ref.receive(primaryResponse(ref, signaling))

          val reconnected = newChannel()

          ref.receive(channelClosed(signaling.id))

          replaceConnections(ref) { con =>
            if (con.signaling) {
              connection(reconnected, signaling = true)
            } else con
          }

          ref.receive(channelConnected(reconnected.id))

          commands(reconnected).headOption
            .map(_._2) must beSome[BSONDocument].which { first =>
            isHello(first) must beTrue and {
              applicationName(first) must beSome(appName)
            }
          }
        }
      }
    }

    "again once a pooled connection is reconnected" in {
      withHandshakeSystem { (ref, signaling, pooled) =>
        ref.receive(channelConnected(signaling.id))
        ref.receive(channelConnected(pooled.id))
        ref.receive(primaryResponse(ref, signaling))

        commands(pooled).count {
          case (_, cmd) => cmd.contains("client")
        } must_=== 1 and {
          val reconnected = newChannel()

          ref.receive(channelClosed(pooled.id))

          replaceConnections(ref) { con =>
            if (con.channel.id == pooled.id) {
              connection(reconnected, signaling = false)
            } else con
          }

          ref.receive(channelConnected(reconnected.id))

          commands(reconnected).headOption
            .map(_._2) must beSome[BSONDocument].which { first =>
            isHello(first) must beTrue and {
              applicationName(first) must beSome(appName)
            }
          }
        }
      }
    }
  }

  // ---

  private def withHandshakeSystem[T](
      f: (TestActorRef[StandardDBSystem], EmbeddedChannel, EmbeddedChannel) => T
    ): T = {
    val result =
      withConAndSys(md, options = handshakeOpts, _nodes = Seq.empty) {
        (_, ref) =>
          import ref.{ underlyingActor => sys }

          val signaling = newChannel()
          val pooled = newChannel()

          val node = new Node(
            "handshake.node:27017",
            Set.empty,
            NodeStatus.Unknown,
            Vector(
              connection(signaling, signaling = true),
              connection(pooled, signaling = false)
            ),
            Set.empty,
            tags = Map.empty[String, String],
            ProtocolMetadata.Default,
            PingInfo(),
            false,
            System.nanoTime()
          )

          updateNodeSet(sys, "_handshake") { _ =>
            new NodeSet(
              None,
              None,
              Vector(node),
              Set(
                Authenticate(tests.Common.commonDb, "test", Some("password"))
              ),
              ListSet.empty
            )
          }

          val res = f(ref, signaling, pooled)

          // Embedded channels don't notify the system once closed
          nodeSet(sys).nodes.flatMap(_.connections).foreach { c =>
            ref.receive(channelClosed(c.channel.id))
          }

          scala.concurrent.Future.successful(res)
      }

    scala.concurrent.Await.result(result, timeout)
  }

  private def newChannel() = new EmbeddedChannel(DefaultChannelId.newInstance())

  private def connection(channel: EmbeddedChannel, signaling: Boolean) =
    new Connection(
      channel,
      ConnectionStatus.Connecting,
      authenticated = Set.empty,
      authenticating = None,
      signaling = signaling
    )

  private def replaceConnections(
      ref: TestActorRef[StandardDBSystem]
    )(f: Connection => Connection
    ): Unit = {
    updateNodeSet(ref.underlyingActor, "_reconnect") {
      _.updateAll { n => n.copy(connections = n.connections.map(f)) }
    }

    ()
  }

  private def probeNode(ref: TestActorRef[StandardDBSystem]): Node =
    nodeSet(ref.underlyingActor).nodes.head

  private def primaryResponse(
      ref: TestActorRef[StandardDBSystem],
      signaling: EmbeddedChannel
    ) = {
    val id = probeNode(ref).pingInfo.lastIsMasterId

    fakeResponse(
      BSONDocument(
        "ok" -> 1,
        "ismaster" -> true,
        "minWireVersion" -> 0,
        "maxWireVersion" -> 21
      ),
      reqID = id + 1,
      respTo = id,
      chanId = signaling.id
    )
  }

  private def commands(channel: EmbeddedChannel): List[(Int, BSONDocument)] =
    Iterator
      .continually(channel.readOutbound[Request]())
      .takeWhile(_ != null)
      .map { req =>
        val payload = req.payload.duplicate()

        if (!req.op.isInstanceOf[Query]) {
          payload.skipBytes(1) // OP_MSG section kind
        }

        req.requestID -> BSONSerializationPack.readFromBuffer(
          ReadableBuffer(payload)
        )
      }
      .toList

  private def isHello(cmd: BSONDocument): Boolean =
    cmd.headOption.exists(e => e.name == "hello" || e.name == "ismaster")

  private def applicationName(cmd: BSONDocument): Option[String] = for {
    client <- cmd.getAsOpt[BSONDocument]("client")
    application <- client.getAsOpt[BSONDocument]("application")
    name <- application.getAsOpt[String]("name")
  } yield name
}
