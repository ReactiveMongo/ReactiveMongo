package external.reactivemongo

import scala.util.Try

import reactivemongo.api.MongoConnectionOptions
import reactivemongo.core.nodeset.NodeSetInfo

/**
 * Listener definition for the connection events.
 *
 * @define supervisorParam the name of the pool supervisor (for logging)
 * @define connectionParam the name of the connection pool
 */
trait ConnectionListener {
  /** Logger available for the listener implementation. */
  lazy val logger: org.slf4j.Logger = ConnectionListener.logger

  /**
   * The connection pool is initialized.
   *
   * @param options the connection options
   * @param supervisor $supervisorParam
   * @param connection $connectionParam
   */
  def poolCreated(
    options: MongoConnectionOptions,
    supervisor: String,
    connection: String): Unit

  /**
   * The node set of the connection pool has been updated.
   * This is fired asynchronously.
   *
   * @param supervisor $supervisorParam
   * @param connection $connectionParam
   * @param previous the previous node set
   * @param updated the new/updated node set
   */
  def nodeSetUpdated(
    supervisor: String,
    connection: String,
    previous: NodeSetInfo,
    updated: NodeSetInfo): Unit

  /**
   * The connection is being shut down.
   *
   * @param supervisor $supervisorParam
   * @param connection $connectionParam
   */
  def poolShutdown(supervisor: String, connection: String): Unit
}

object ConnectionListener {
  import java.net.URL

  val staticListenerBinderPath =
    "external/reactivemongo/StaticListenerBinder.class";

  private[reactivemongo] val logger =
    org.slf4j.LoggerFactory.getLogger("reactivemongo.core.ConnectionListener")

  /** Optionally creates a listener according the available binding. */
  @SuppressWarnings(Array("NullParameter"))
  def apply(): Option[ConnectionListener] = {
    val binderPathSet = scala.collection.mutable.LinkedHashSet[URL]()

    try {
      val reactiveMongoLoader = classOf[ConnectionListener].getClassLoader
      val paths: java.util.Enumeration[URL] =
        if (reactiveMongoLoader == null) {
          ClassLoader.getSystemResources(staticListenerBinderPath)
        } else {
          reactiveMongoLoader.getResources(staticListenerBinderPath)
        }

      while (paths.hasMoreElements()) binderPathSet.add(paths.nextElement())
    } catch {
      case ioe: java.io.IOException =>
        logger.warn("Error getting resources from path", ioe);
    }

    binderPathSet.headOption.flatMap { first =>
      if (binderPathSet.tail.nonEmpty) {
        logger.warn(s"Class path contains multiple StaticListenerBinder: $first, ${binderPathSet.tail mkString ", "}")
      }

      val `try` = Try(new StaticListenerBinder().connectionListener())

      `try`.failed.foreach { reason =>
        logger.warn("Fails to create connection listener; Fallbacks to the default one", reason)
      }

      `try`.toOption
    }
  }
}

final class StaticListenerBinder {
  /**
   * Returns a new listener instance;
   * At most one will be used per driver.
   */
  def connectionListener(): ConnectionListener = ???
}
