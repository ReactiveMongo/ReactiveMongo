package reactivemongo.api

import scala.util.{ Try, Failure, Success }

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration.{ Duration, FiniteDuration, SECONDS }

import com.typesafe.config.Config

import akka.actor.ActorRef

import reactivemongo.core.nodeset.Authenticate
import reactivemongo.util.LazyLogger

/**
 * @param config a custom configuration (otherwise the default options are used)
 * @param classLoader a classloader used to load the actor system
 *
 * @define parsedURIParam the URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
 * @define connectionNameParam the name for the connection pool
 * @define strictUriParam if true the parsed URI must be strict, without ignored/unsupported options
 * @define nbChannelsParam the number of channels to open per node
 * @define optionsParam the options for the new connection pool
 * @define nodesParam The list of node names (e.g. ''node1.foo.com:27017''); Port is optional (27017 is used by default)
 * @define authParam the list of authentication instructions
 * @define seeConnectDBTutorial See [[http://reactivemongo.org/releases/0.12/documentation/tutorial/connect-database.html how to connect to the database]]
 * @define uriStrictParam the strict URI, that will be parsed by [[reactivemongo.api.MongoConnection.parseURI]]
 */
class MongoDriver(
  protected val config: Option[Config] = None,
  protected val classLoader: Option[ClassLoader] = None) extends Driver {

  @deprecated("Use the constructor with the classloader", "0.12-RC6")
  def this(config: Option[Config]) = this(config, None)

  import MongoDriver.logger

  /** Keep a list of all connections so that we can terminate the actors */
  @deprecated(message = "Will be made private", since = "0.12-RC1")
  def connections: Iterable[MongoConnection] = connectionMonitors.values

  @deprecated(message = "Will be made private", since = "0.12-RC1")
  def numConnections: Int = connectionMonitors.size

  @deprecated("Will be made private", "0.12.7")
  case class AddConnection(
    name: String,
    nodes: Seq[String],
    options: MongoConnectionOptions,
    mongosystem: ActorRef)

  @deprecated("Will be made private", "0.12.7")
  class SupervisorActor() extends akka.actor.Actor with Product
    with Serializable with java.io.Serializable {

    def receive: Receive = throw new UnsupportedOperationException()
    def canEqual(that: Any): Boolean = false
    def productArity: Int = 0
    def productElement(n: Int): Any = throw new UnsupportedOperationException()
  }

  /**
   * Closes this driver (and all its connections and resources).
   * Awaits the termination until the timeout is expired.
   */
  def close(timeout: FiniteDuration = FiniteDuration(2, SECONDS)): Unit = {
    Await.result(askClose(timeout)(ExecutionContext.global), timeout) // Unsafe
  }

  /**
   * Creates a new MongoConnection.
   *
   * $seeConnectDBTutorial
   *
   * @param nodes $nodesParam
   * @param authentications $authParam
   * @param nbChannelsPerNode $nbChannelsParam
   * @param name $connectionNameParam
   * @param options $optionsParam
   */
  @deprecated(message = "Must use `connection` with `nbChannelsPerNode` set in the `options`.", since = "0.11.3")
  def connection(nodes: Seq[String], options: MongoConnectionOptions, authentications: Seq[Authenticate], nbChannelsPerNode: Int, name: Option[String]): MongoConnection = connection(nodes, options, authentications, name)

  /**
   * Creates a new MongoConnection.
   *
   * $seeConnectDBTutorial
   *
   * @param nodes $nodesParam
   * @param authentications $authParam
   * @param name $connectionNameParam
   * @param options $optionsParam
   */
  def connection(nodes: Seq[String], options: MongoConnectionOptions = MongoConnectionOptions(), authentications: Seq[Authenticate] = Seq.empty, name: Option[String] = None): MongoConnection = {
    Await.result(askConnection(nodes, options, authentications, name), Duration.Inf)
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param uriStrict $uriStrictParam
   */
  def connection(uriStrict: String): Try[MongoConnection] =
    connection(uriStrict, name = None, strictUri = true)

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param uri the URI to be parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param name $connectionNameParam
   * @param strictUri $strictUriParam
   */
  def connection(uri: String, name: Option[String], strictUri: Boolean): Try[MongoConnection] = MongoConnection.parseURI(uri).flatMap(connection(_, name, strictUri))

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param parsedURI $parsedURIParam
   * @param nbChannelsPerNode $nbChannelsParam
   * @param name $connectionNameParam
   */
  @deprecated(message = "Must you reactivemongo.api.MongoDriver.connection(reactivemongo.api.MongoConnection.ParsedURI,Option[String]):reactivemongo.api.MongoConnection connection(..)]] with `nbChannelsPerNode` set in the `parsedURI`.", since = "0.11.3")
  def connection(parsedURI: MongoConnection.ParsedURI, nbChannelsPerNode: Int, name: Option[String]): MongoConnection = connection(parsedURI, name)

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param parsedURI $parsedURIParam
   * @param name $connectionNameParam
   */
  def connection(parsedURI: MongoConnection.ParsedURI, name: Option[String]): MongoConnection = connection(parsedURI, name, strictUri = false).get // Unsafe

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param name $connectionNameParam
   * @param strictUri $strictUriParam
   */
  def connection(parsedURI: MongoConnection.ParsedURI, name: Option[String], strictUri: Boolean): Try[MongoConnection] = {
    if (!parsedURI.ignoredOptions.isEmpty && strictUri) {
      Failure(new IllegalArgumentException(s"The connection URI contains unsupported options: ${
        parsedURI.ignoredOptions
          .mkString(", ")
      }"))
    } else {
      if (!parsedURI.ignoredOptions.isEmpty) logger.warn(s"Some options were ignored because they are not supported (yet): ${parsedURI.ignoredOptions.mkString(", ")}")

      Success(connection(parsedURI.hosts.map(h => h._1 + ':' + h._2), parsedURI.options, parsedURI.authenticate.toSeq, name))
    }
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param parsedURI $parsedURIParam
   * @param nbChannelsPerNode $nbChannelsParam
   */
  @deprecated(message = "Must you `connection` with `nbChannelsPerNode` set in the options of the `parsedURI`.", since = "0.11.3")
  def connection(parsedURI: MongoConnection.ParsedURI, nbChannelsPerNode: Int): MongoConnection = connection(parsedURI, None, false).get // Unsafe

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param parsedURI $parsedURIParam
   * @param strictUri $strictUriParam
   */
  def connection(parsedURI: MongoConnection.ParsedURI, strictUri: Boolean): Try[MongoConnection] = connection(parsedURI, None, strictUri)

  /**
   * Creates a new MongoConnection from URI.
   *
   * $seeConnectDBTutorial
   *
   * @param parsedURI $parsedURIParam
   */
  def connection(parsedURI: MongoConnection.ParsedURI): MongoConnection =
    connection(parsedURI, None, false).get // Unsafe

}

/** The driver factory */
object MongoDriver {
  private val logger = LazyLogger("reactivemongo.api.MongoDriver")

  /** Creates a new [[MongoDriver]] with a new ActorSystem. */
  def apply(): MongoDriver = new MongoDriver()

  /** Creates a new [[MongoDriver]] with the given `config`. */
  def apply(config: Config): MongoDriver = new MongoDriver(Some(config), None)

  /** Creates a new [[MongoDriver]] with the given `config`. */
  def apply(config: Config, classLoader: ClassLoader): MongoDriver =
    new MongoDriver(Some(config), Some(classLoader))
}
