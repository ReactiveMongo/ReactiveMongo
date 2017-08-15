import java.lang.management.ManagementFactory
import javax.management.{
  MBeanServer,
  MBeanInfo,
  MBeanNotificationInfo,
  Notification,
  NotificationListener,
  ObjectInstance,
  ObjectName
}

import scala.util.{ Failure, Success, Try }

import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._

import org.specs2.concurrent.{ ExecutionEnv => EE }
import org.specs2.matcher.MatchResult

import external.reactivemongo.ConnectionListener

import reactivemongo.jmx.{ Node, NodeSet }

class JmxSpec extends org.specs2.mutable.Specification {
  "JMX" title

  sequential

  import Common.db

  "Connection listener" should {
    lazy val listener = ConnectionListener()

    "be resolved as the default one" in {
      listener.map(_.getClass.getName) must beSome(
        "reactivemongo.jmx.ConnectionListener")
    }
  }

  "NodeSet MBean" should {
    "be registered" in { implicit ee: EE =>
      Try(db).map(_.name) must beSuccessfulTry[String].like {
        case name => name aka "database name" must_== Common.dbName and {
          val mbeans = mbs.queryMBeans(new ObjectName(
            "org.reactivemongo.Supervisor-*:type=NodeSet,*"), null)

          Try(mbeans.iterator.next()) must beSuccessfulTry[ObjectInstance].
            which { bi =>
              val on = bi.getObjectName

              verifyBeanInstance(bi, "reactivemongo.jmx.NodeSet",
                nodeSetAttrs, NodeSet.notificationInfo) and {
                val attrs = Promise[List[Any]]()
                val listener = new NotificationListener {
                  def handleNotification(n: Notification, b: Object): Unit = {
                    Try(mbs.getAttribute(on, "Nearest")).toOption.
                      filter(_ => !attrs.isCompleted).foreach { _ =>
                        Try(nodeSetAttrs.map {
                          case (name, typ, _, _) =>
                            val v = mbs.getAttribute(on, name)

                            if (typ == "java.lang.String" && (
                              v != null && v.toString.startsWith("Node["))) {

                              v.asInstanceOf[String].take(20)
                            } else v
                        }) match {
                          case Success(_ :: _ :: _ :: _ :: null :: _) => ()
                          case res =>
                            attrs.tryComplete(res)
                        }
                      }
                  }
                }

                mbs.addNotificationListener(on, listener, null, null)

                attrs.future must beEqualTo(List(
                  null.asInstanceOf[String], // name
                  -1L, // version
                  s"Node[$host:$port", // primary
                  null.asInstanceOf[String], // mongos
                  s"Node[$host:$port", // nearest
                  s"Node[$host:$port", // nodes
                  "" // secondaries
                )).await(0, 5.seconds)
              }
            }
        }
      }
    }
  }

  "Node MBean" should {
    "be registered" in { implicit ee: EE =>
      Try(db).map(_.name) must beSuccessfulTry[String].like {
        case name => name aka "database name" must_== Common.dbName and {
          val mbeans = mbs.queryMBeans(new ObjectName(s"org.reactivemongo.Supervisor-*.${Common.connection.name}:type=Node,*"), null)

          nodeMBean aka ("Node MBean") must beLike[ObjectInstance] {
            case bi => verifyBeanInstance(bi, "reactivemongo.jmx.Node",
              nodeAttrs, Node.notificationInfo) and {
              val on = bi.getObjectName
              val exAttr = List("Supervisor", "Status", "PingInfo",
                "Connections", "Connected", "Authenticated")

              Try(nodeAttrs.collect {
                case (name, _, _, _) if (!exAttr.contains(name)) =>
                  mbs.getAttribute(on, name) match {
                    case null => null.asInstanceOf[String]
                    case v    => v.toString
                  }
              }) must beSuccessfulTry[List[String]](List(
                Common.connection.name,
                s"$host:$port", // name
                "", // aliases
                host,
                port.toString,
                null.asInstanceOf[String], // tags
                "minWireVersion = 3.0, maxWireVersion = 3.0, maxMessageSizeBytes = 48000000, maxBsonSize = 16777216, maxBulkSize = 1000", // protocol metadata
                "false" // mongos
              ))
            }
          }.await(1, 5.seconds)
        }
      }
    }
  }

  def verifyBeanInstance(instance: => ObjectInstance, beanType: String, attrs: List[AttrDef], notifInfo: Array[MBeanNotificationInfo]): MatchResult[Option[ObjectInstance]] = Option(instance) must beSome[ObjectInstance].
    like {
      case bi => Option(bi.getObjectName).flatMap(
        _.toString.drop(18).takeWhile(_ != ':').
          split("\\.").reverse.headOption).
        aka("connection fragment") must beSome(Common.connection.name) and {
          bi.getClassName must_== beanType
        } and {
          Try(mbs getMBeanInfo bi.getObjectName).
            aka("MBean info") must beSuccessfulTry[MBeanInfo].like {
              case info => info.getAttributes.map(attr => {
                (attr.getName, attr.getType, attr.isReadable, attr.isWritable)
              }).iterator must containAllOf(attrs) and {
                info.getOperations aka "operations" must beEmpty
              } and {
                info.getNotifications must_== notifInfo
              }
            }
        }
    }

  def nodeMBean(implicit ee: EE): Future[ObjectInstance] = {
    val mbeans = mbs.queryMBeans(new ObjectName(
      "org.reactivemongo.Supervisor-*:type=Node,*"), null)

    Try(mbeans.iterator.next()) match {
      case Success(node) => Future.successful(node)
      case _             => waitNodeMBean
    }
  }

  @inline def fromTry[T](`try`: Try[T]): Future[T] = // for Scala 2.10.5 compat
    `try` match {
      case Success(v) => Future.successful[T](v)
      case Failure(e) => Future.failed[T](e)
    }

  def waitNodeMBean(implicit ee: EE): Future[ObjectInstance] = {
    val mbeans = mbs.queryMBeans(new ObjectName(
      "org.reactivemongo.Supervisor-*:type=NodeSet,*"), null)

    fromTry(Try(mbeans.iterator.next())).flatMap { ns =>
      val filter = new javax.management.NotificationFilter {
        def isNotificationEnabled(n: Notification) = n.getType == "nodeAdded"
      }
      val objName = Promise[ObjectName]()
      val listener = new NotificationListener {
        def handleNotification(n: Notification, b: Object): Unit =
          objName.tryComplete(Try(n.getSource.asInstanceOf[ObjectName]))
      }

      mbs.addNotificationListener(ns.getObjectName, listener, filter, null)

      objName.future.flatMap { n =>
        val mbeans = mbs.queryMBeans(n, null)

        fromTry(Try(mbeans.iterator.next()))
      }
    }
  }

  // ---

  lazy val (host, port) = Common.primaryHost.split(":").toList match {
    case h :: p :: Nil => h -> p.toInt
    case _             => sys.error(s"Malformed primary host: ${Common.primaryHost}")
  }

  type AttrDef = (String, String, Boolean, Boolean)

  val nodeSetAttrs = List[AttrDef](
    ("Name", "java.lang.String", true, false),
    ("Version", "long", true, false),
    ("Primary", "java.lang.String", true, false),
    ("Mongos", "java.lang.String", true, false),
    ("Nearest", "java.lang.String", true, false),
    ("Nodes", "java.lang.String", true, false),
    ("Secondaries", "java.lang.String", true, false))

  val nodeAttrs = List[AttrDef](
    ("Supervisor", "java.lang.String", true, false),
    ("Connection", "java.lang.String", true, false),
    ("Name", "java.lang.String", true, false),
    ("Aliases", "java.lang.String", true, false),
    ("Host", "java.lang.String", true, false),
    ("Port", "int", true, false),
    ("Status", "java.lang.String", true, false),
    ("Connections", "int", true, false),
    ("Connected", "int", true, false),
    ("Authenticated", "int", true, false),
    ("Tags", "java.lang.String", true, false),
    ("ProtocolMetadata", "java.lang.String", true, false),
    ("PingInfo", "java.lang.String", true, false),
    ("Mongos", "boolean", true, false))

  lazy val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer()
}
