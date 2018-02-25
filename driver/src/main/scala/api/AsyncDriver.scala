package reactivemongo.api

import scala.util.{ Failure, Success }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ FiniteDuration, SECONDS }

import com.typesafe.config.Config

import reactivemongo.core.nodeset.Authenticate

/**
 * @param config a custom configuration (otherwise the default options are used)
 * @param classLoader a classloader used to load the actor system
 *
 * @define parsedURIParam the URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
 * @define connectionNameParam the name for the connection pool
 * @define optionsParam the options for the new connection pool
 * @define nodesParam The list of node names (e.g. ''node1.foo.com:27017''); Port is optional (27017 is used by default)
 * @define authParam the list of authentication instructions
 * @define uriStrictParam the strict URI, that will be parsed by [[reactivemongo.api.MongoConnection.parseURI]]
 */
class AsyncDriver(
  protected val config: Option[Config] = None,
  protected val classLoader: Option[ClassLoader] = None) extends Driver {

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes $nodesParam
   * @param authentications $authParam
   * @param name $connectionNameParam
   * @param options $optionsParam
   */
  def connect(nodes: Seq[String], options: MongoConnectionOptions = MongoConnectionOptions(), authentications: Seq[Authenticate] = Seq.empty, name: Option[String] = None): Future[MongoConnection] = askConnection(nodes, options, authentications, name)

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param uriStrict $uriStrictParam
   */
  def connect(uriStrict: String): Future[MongoConnection] =
    connect(uriStrict, name = None)

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param uriStrict $uriStrictParam
   * @param name $connectionNameParam
   */
  def connect(uriStrict: String, name: Option[String]): Future[MongoConnection] = MongoConnection.parseURI(uriStrict) match {
    case Success(parsedURI) => connect(parsedURI, name)
    case Failure(exception) => Future.failed(exception)
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param name $connectionNameParam
   */
  def connect(parsedURI: MongoConnection.ParsedURI, name: Option[String]): Future[MongoConnection] = {
    if (!parsedURI.ignoredOptions.isEmpty) {
      Future.failed(new IllegalArgumentException(s"The connection URI contains unsupported options: ${parsedURI.ignoredOptions.mkString(", ")}"))
    } else {
      askConnection(
        parsedURI.hosts.map(h => h._1 + ':' + h._2),
        parsedURI.options, parsedURI.authenticate.toSeq, name)
    }
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param parsedURI $parsedURIParam
   */
  def connect(parsedURI: MongoConnection.ParsedURI): Future[MongoConnection] =
    connect(parsedURI, None)

  /**
   * Closes this driver (and all its connections and resources).
   * Will wait until the timeout for proper closing of connections before forcing hard shutdown.
   */
  final def close(timeout: FiniteDuration = FiniteDuration(2, SECONDS))(implicit executionContext: ExecutionContext): Future[Unit] = askClose(timeout)

}

/** The driver factory */
object AsyncDriver {
  /** Creates a new [[AsyncDriver]] with a new ActorSystem. */
  def apply(): AsyncDriver = new AsyncDriver()

  /** Creates a new [[AsyncDriver]] with the given `config`. */
  def apply(config: Config): AsyncDriver = new AsyncDriver(Some(config), None)

  /** Creates a new [[AsyncDriver]] with the given `config`. */
  def apply(config: Config, classLoader: ClassLoader): AsyncDriver =
    new AsyncDriver(Some(config), Some(classLoader))
}
