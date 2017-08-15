package reactivemongo.jmx

import java.util.Hashtable
import javax.management.{
  AttributeChangeNotification,
  MBeanNotificationInfo,
  Notification,
  NotificationBroadcasterSupport,
  ObjectName
}

import scala.reflect.ClassTag

import reactivemongo.api.MongoConnectionOptions
import reactivemongo.core.nodeset.{ NodeSetInfo, NodeInfo }

/** Listener definition for the connection events. */
final class ConnectionListener
  extends external.reactivemongo.ConnectionListener {

  import java.lang.management.ManagementFactory
  import javax.management.MBeanServer

  /* Logging prefix */
  private var lnm = "unknown"

  private lazy val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer()

  private var unregister: () => Unit = () => {}

  @inline private def nodeProps(node: NodeInfo) = {
    val props = new Hashtable[String, String]()
    props.put("type", "Node")
    props.put("name", s"${node.host}-${node.port}")

    props
  }

  private var nodeObjName: NodeInfo => ObjectName = { node: NodeInfo =>
    new ObjectName("org.reactivemongo", nodeProps(node))
  }

  private var createNode: () => Node = () => new Node("unknown", "unknown")

  private lazy val nodeSet: NodeSet = new NodeSet()

  // Handler functions

  def poolCreated(options: MongoConnectionOptions, supervisor: String, connection: String): Unit = {
    lnm = s"$supervisor/$connection"

    val props = new Hashtable[String, String]()
    props.put("type", "NodeSet")

    val domain = s"org.reactivemongo.$supervisor.$connection"
    val objName = new ObjectName(domain, props)
    def opts = MongoConnectionOptions.toStrings(options).mkString(", ")

    nodeSet.init(opts, supervisor, connection)

    mbs.registerMBean(nodeSet, objName)

    unregister = () => {
      logger.info(s"[$lnm] Unregister the NodeSet MBean: $objName")

      nodeSet.sendNotification("stateChange", domain,
        "The connection pool is being terminated")

      mbs.unregisterMBean(objName)
    }

    createNode = () => new Node(supervisor, connection)
    nodeObjName = { node: NodeInfo =>
      new ObjectName(
        s"org.reactivemongo.$supervisor.$connection", nodeProps(node))
    }

    nodeSet.sendNotification("stateChange", domain,
      "The connection pool is has been created")

  }

  private val nodes = scala.collection.mutable.Map.empty[String, Node]

  def nodeSetUpdated(previous: NodeSetInfo, updated: NodeSetInfo): Unit = {
    nodeSet.update(updated)

    def nodeMap(ns: NodeSetInfo): Map[String, NodeInfo] =
      Option(ns).fold(List.empty[NodeInfo])(_.nodes.toList).
        map { n => n.name -> n }.toMap

    val prev = nodeMap(previous)
    val upd = nodeMap(updated)

    nodes.synchronized {
      val rmd = prev -- upd.keys

      // Removed nodes
      rmd.foreach {
        case (name, removed) =>
          lazy val objName = nodeObjName(removed)

          try {
            mbs.unregisterMBean(objName)
            nodes -= name

            nodeSet.sendNotification("nodeRemoved", objName,
              s"The node is no longer available: $name")

          } catch {
            case _: javax.management.InstanceNotFoundException =>
              logger.debug(s"The node MBean is not registered: $objName")

            case reason: Throwable =>
              logger.warn(s"Fails to remove node MBean: $objName", reason)
          }
      }

      // Added nodes
      (upd -- prev.keys).foreach {
        case (name, added) =>
          val node = createNode()
          lazy val objName = nodeObjName(added)

          try {
            mbs.registerMBean(node, objName)
            nodes += name -> node
            node.update(added)

            nodeSet.sendNotification("nodeAdded", objName,
              s"The node is now available: $name")

          } catch {
            case _: javax.management.InstanceAlreadyExistsException =>
              logger.warn(s"The node MBean is already registered: $objName")

            case reason: Throwable => logger.warn(
              s"Fails to register the node MBean: $objName", reason)
          }
      }

      // Updated nodes
      (prev -- rmd.keys).foreach {
        case (name, node) => try {
          nodes.get(name).foreach { bean =>
            bean.update(node)

            nodeSet.sendNotification("nodeUpdated", name,
              s"The node properties have been updated: $name")

          }
        } catch {
          case reason: Throwable => logger.warn(
            s"Fails to update the node MBean: $name", reason)
        }
      }
    }
  }

  def poolShutdown(supervisor: String, connection: String) = {
    nodes.foreach {
      case (name, node) =>
        nodes -= name

        try {
          val props = new Hashtable[String, String]()
          props.put("type", "Node")
          props.put("name", s"${node.getHost}-${node.getPort}")

          val objName = new ObjectName(
            s"org.reactivemongo.${node.getSupervisor}.${node.getConnection}",
            props)

          mbs.unregisterMBean(objName)
        } catch {
          case reason: Throwable =>
            logger.warn(s"Fails to unregister the node MBean: $name", reason)
        }
    }

    unregister()
  }
}

sealed trait NotificationSupport { self: NotificationBroadcasterSupport =>
  protected val changeSeq = new java.util.concurrent.atomic.AtomicLong()

  protected def attributeChanged[T: ClassTag](name: String, message: String, oldValue: T, newValue: T)(f: T => Unit): Unit = if (oldValue != newValue) {
    val n = new AttributeChangeNotification(
      this, changeSeq.incrementAndGet(), System.currentTimeMillis(),
      message, name, implicitly[ClassTag[T]].toString, oldValue, newValue)

    f(newValue)

    sendNotification(n)
  }
}

final class NodeSet private[jmx] () extends NotificationBroadcasterSupport
  with NodeSetMBean with NotificationSupport {

  private var options: String = null
  private var supervisor: String = null
  private var connection: String = null
  private var name: String = null
  private var version: Long = -1L
  private var primary: String = null
  private var mongos: String = null
  private var nearest: String = null
  private var nodes: String = null
  private var secondaries: String = null

  // MBean attributes

  def getConnectionOptions() = options
  def getSupervisor() = supervisor
  def getConnection() = connection
  def getName() = name
  def getVersion() = version
  def getMongos(): String = mongos
  def getNearest(): String = nearest
  def getPrimary(): String = primary
  def getNodes() = nodes
  def getSecondaries() = secondaries

  // Notification support

  private[jmx] def sendNotification[T](typ: String, source: T, msg: String) {
    sendNotification(new Notification(typ, source,
      changeSeq.incrementAndGet(), System.currentTimeMillis(), msg))
  }

  override def getNotificationInfo() = NodeSet.notificationInfo

  // ---

  private[jmx] def init(opts: String, s: String, c: String): Unit = {
    options = opts
    supervisor = s
    connection = c
  }

  private[jmx] def update(updated: NodeSetInfo): Unit = {
    val info = Option(updated)

    var _name: String = null
    var _version: Long = -1L
    var _primary: String = null
    var _mongos: String = null
    var _nearest: String = null
    var _nodes: String = null
    var _secondaries: String = null

    info.foreach { i =>
      _name = i.name.getOrElse(null)
      _version = i.version.getOrElse(-1L)
      _primary = i.primary.fold[String](null)(_.toString)
      _mongos = i.mongos.fold[String](null)(_.toString)
      _nearest = i.nearest.fold[String](null)(_.toString)
      _nodes = i.nodes.toArray.map(_.toString).mkString("; ")
      _secondaries = i.secondaries.toArray.map(_.toString).mkString("; ")
    }

    attributeChanged("Name", "The name of node set has changed",
      name, _name) { name = _ }

    attributeChanged[java.lang.Long](
      "Version", "The version of node set has changed",
      version, _version) { version = _ }

    attributeChanged("Primary", "The information about the primary node",
      primary, _primary) { primary = _ }

    attributeChanged("Mongos", "The information about the mongos node",
      mongos, _mongos) { mongos = _ }

    attributeChanged("Nearest", "The information about the nearest node",
      nearest, _nearest) { nearest = _ }

    attributeChanged("Nodes", "The information about the node list",
      nodes, _nodes) { nodes = _ }

    attributeChanged("Secondaries", "The information about the secondary nodes",
      secondaries, _secondaries) { secondaries = _ }

  }
}

object NodeSet {
  lazy val notificationInfo: Array[MBeanNotificationInfo] = Array(
    new MBeanNotificationInfo(
      Array("stateChange"),
      classOf[Notification].getName,
      "The state of the connection pool has changed"),
    new MBeanNotificationInfo(
      Array("nodeAdded"),
      classOf[Notification].getName,
      "A node has been added to the set"),
    new MBeanNotificationInfo(
      Array("nodeUpdated"),
      classOf[Notification].getName,
      "A node has been updated to the set"),
    new MBeanNotificationInfo(
      Array("nodeRemoved"),
      classOf[Notification].getName,
      "A node has been removed to the set"),
    new MBeanNotificationInfo(
      Array[String](AttributeChangeNotification.ATTRIBUTE_CHANGE),
      classOf[AttributeChangeNotification].getName,
      "The node set has changed"))
}

final class Node private[jmx] (
  supervisor: String,
  connection: String) extends NotificationBroadcasterSupport
  with NodeMBean with NotificationSupport {

  import reactivemongo.bson.BSONDocument

  private var name = "unknown"
  private var aliases: String = null
  private var host = "unknown"
  private var port = -1
  private var status = "unknown"
  private var connections = 0
  private var connected = 0
  private var authenticated = 0
  private var tags: String = null
  private var protocolMetadata = "unknown"
  private var pingInfo = "unknown"
  private var mongos = false

  private[jmx] def update(info: NodeInfo): Unit = {
    val _name = info.name
    val _aliases = info.aliases.mkString(", ")
    val _host = info.host
    val _port = info.port
    val _status = info.status.toString
    val _connections = info.connections
    val _connected = info.connected
    val _authenticated = info.authenticated
    val _tags = info.tags.fold[String](null)(BSONDocument.pretty(_))
    val _mongos = info.isMongos

    val _protocolMetadata = {
      val m = info.protocolMetadata

      s"minWireVersion = ${m.minWireVersion}, maxWireVersion = ${m.maxWireVersion}, maxMessageSizeBytes = ${m.maxMessageSizeBytes}, maxBsonSize = ${m.maxBsonSize}, maxBulkSize = ${m.maxBulkSize}"
    }

    val _pingInfo = {
      val i = info.pingInfo
      s"sent = ${i.ping}, lastIsMaster(time = ${i.lastIsMasterTime}, id = ${i.lastIsMasterId})"
    }

    attributeChanged("Name", "The node name", name, _name) { name = _ }

    attributeChanged("Aliases", "The aliases of the node",
      aliases, _aliases) { aliases = _ }

    attributeChanged("Host", "The name of the node host",
      host, _host) { host = _ }

    attributeChanged("Port", "The MongoDB port on the node",
      port, _port) { port = _ }

    attributeChanged("Status", "The node status",
      status, _status) { status = _ }

    attributeChanged("Connections", "The number of connections to the node",
      connections, _connections) { connections = _ }

    attributeChanged(
      "Connected",
      "The number of connections established to the node",
      connected, _connected) { connected = _ }

    attributeChanged(
      "Authenticated",
      "The number of authenticated connections to the node",
      authenticated, _authenticated) { authenticated = _ }

    attributeChanged("Tags", "The tags for the node", tags, _tags) { tags = _ }

    attributeChanged(
      "ProtocolMetadata",
      "The metadata for the protocol to connect to the node",
      protocolMetadata, _protocolMetadata) { protocolMetadata = _ }

    attributeChanged(
      "PingInfo",
      "The information about the ping to the node",
      pingInfo, _pingInfo) { pingInfo = _ }

    attributeChanged("Mongos", "Indicates whether the node is a Mongos one",
      mongos, _mongos) { mongos = _ }

  }

  // MBean attributes

  def getSupervisor() = supervisor
  def getConnection() = connection
  def getName() = name
  def getAliases() = aliases
  def getHost() = host
  def getPort() = port
  def getStatus() = status
  def getConnections() = connections
  def getConnected() = connected
  def getAuthenticated() = authenticated
  def getTags() = tags
  def getProtocolMetadata() = protocolMetadata
  def getPingInfo() = pingInfo
  def isMongos() = mongos

  // Notification support

  override def getNotificationInfo() = Node.notificationInfo
}

object Node {
  lazy val notificationInfo: Array[MBeanNotificationInfo] = Array(
    new MBeanNotificationInfo(
      Array[String](AttributeChangeNotification.ATTRIBUTE_CHANGE),
      classOf[AttributeChangeNotification].getName,
      "The node has changed"))
}
