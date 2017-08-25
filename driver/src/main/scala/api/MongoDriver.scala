package reactivemongo.api

import java.util.concurrent.atomic.AtomicLong

import scala.util.{ Try, Failure, Success }

import scala.collection.mutable
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, FiniteDuration, SECONDS }

import com.typesafe.config.Config

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }
import akka.pattern.ask

import reactivemongo.core.actors.{
  Close,
  Closed,
  LegacyDBSystem,
  MongoDBSystem,
  StandardDBSystem
}
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
  config: Option[Config] = None,
  classLoader: Option[ClassLoader] = None) {
  @deprecated("Use the constructor with the classloader", "0.12-RC6")
  def this(config: Option[Config]) = this(config, None)

  import scala.collection.mutable.{ Map => MutableMap }
  import MongoDriver.logger
  import reactivemongo.core.{
    AsyncSystemControl,
    SystemControl,
    TimedSystemControl
  }

  /* MongoDriver always uses its own ActorSystem
   * so it can have complete control separate from other
   * Actor Systems in the application
   */
  val system = {
    import com.typesafe.config.ConfigFactory
    val reference = config getOrElse ConfigFactory.load()
    val cfg = if (!reference.hasPath("mongo-async-driver")) {
      logger.info("No mongo-async-driver configuration found")
      ConfigFactory.empty()
    } else reference.getConfig("mongo-async-driver")

    ActorSystem("reactivemongo", Some(cfg), classLoader)
  }

  private val systemClose: Option[FiniteDuration] => Future[Unit] =
    SystemControl(system) match {
      case Success(TimedSystemControl(close)) => { timeout =>
        close(timeout) match {
          case Failure(cause) => Future.failed[Unit](cause)
          case _              => Future.successful({})
        }
      }

      case Success(AsyncSystemControl(close)) =>
        { _ => close() }

      case Failure(cause) => { _ => Future.failed[Unit](cause) }
    }

  private val supervisorName = s"Supervisor-${MongoDriver.nextCounter}"
  private[reactivemongo] val supervisorActor =
    system.actorOf(Props(new SupervisorActor(this)), supervisorName)

  private val connectionMonitors = MutableMap.empty[ActorRef, MongoConnection]

  /** Keep a list of all connections so that we can terminate the actors */
  @deprecated(message = "Will be made private", since = "0.12-RC1")
  def connections: Iterable[MongoConnection] = connectionMonitors.values

  @deprecated(message = "Will be made private", since = "0.12-RC1")
  def numConnections: Int = connectionMonitors.size

  /**
   * Closes this driver (and all its connections and resources).
   * Awaits the termination until the timeout is expired.
   */
  @deprecated(message = "Use `askClose` instead", since = "0.12.7")
  def close(timeout: FiniteDuration = FiniteDuration(2, SECONDS)): Unit = {
    Await.result(askClose(timeout)(ExecutionContext.global), timeout) // Unsafe
  }

  /**
   * Closes this driver (and all its connections and resources).
   * Will wait the until the timeout for proper closing of connections before forcing hard shutdown.
   */
  def askClose(timeout: FiniteDuration = FiniteDuration(2, SECONDS))(implicit executionContext: ExecutionContext): Future[Unit] = {
    logger.info(s"[$supervisorName] Closing instance of ReactiveMongo driver")
    // Tell the supervisor to close.
    // It will shut down all the connections and monitors
    (supervisorActor ? Close)(Timeout(timeout)).recover {
      case err =>
        logger.warn(s"[$supervisorName] Failed to close connections within timeout. Continuing closing of ReactiveMongo driver anyway.", err)
    } // and then shut down the ActorSystem as it is exiting.
      .flatMap(_ => systemClose(Some(timeout)))
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
   * Creates a new MongoConnection.
   *
   * @param nodes $nodesParam
   * @param authentications $authParam
   * @param name $connectionNameParam
   * @param options $optionsParam
   */
  def askConnection(nodes: Seq[String], options: MongoConnectionOptions = MongoConnectionOptions(), authentications: Seq[Authenticate] = Seq.empty, name: Option[String] = None): Future[MongoConnection] = {
    val nm = name.getOrElse(s"Connection-${+MongoDriver.nextCounter}")

    // TODO: Passing ref to MongoDBSystem.history to AddConnection
    lazy val dbsystem: MongoDBSystem = options.authMode match {
      case CrAuthentication => new LegacyDBSystem(
        supervisorName, nm, nodes, authentications, options)

      case _ => new StandardDBSystem(
        supervisorName, nm, nodes, authentications, options)
    }

    val mongosystem = system.actorOf(Props(dbsystem), nm)

    def connection = (supervisorActor ? AddConnection(
      nm, nodes, options, mongosystem))(Timeout(10, SECONDS))

    logger.info(s"[$supervisorName] Creating connection: $nm")

    import system.dispatcher

    connection.mapTo[MongoConnection].map { c =>
      // TODO: Review
      c.history = () => dbsystem.internalState
      c
    }
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
   * @param uri the (strict) URI to be parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   */
  def askConnection(uri: String): Future[MongoConnection] =
    askConnection(uri, name = None)

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
   * @param uri the (strict) URI to be parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param name $connectionNameParam
   */
  def askConnection(uri: String, name: Option[String]): Future[MongoConnection] = MongoConnection.parseURI(uri) match {
    case Success(parsedURI) => askConnection(parsedURI, name)
    case Failure(exception) => Future.failed(exception)
  }

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
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param name $connectionNameParam
   */
  def askConnection(parsedURI: MongoConnection.ParsedURI, name: Option[String]): Future[MongoConnection] = {
    if (!parsedURI.ignoredOptions.isEmpty)
      Future.failed(new IllegalArgumentException(s"The connection URI contains unsupported options: ${parsedURI.ignoredOptions.mkString(", ")}"))
    else
      askConnection(parsedURI.hosts.map(h => h._1 + ':' + h._2), parsedURI.options, parsedURI.authenticate.toSeq, name)
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

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param parsedURI $parsedURIParam
   */
  def askConnection(parsedURI: MongoConnection.ParsedURI): Future[MongoConnection] =
    askConnection(parsedURI, None)

  private[reactivemongo] case class AddConnection(
    name: String,
    nodes: Seq[String],
    options: MongoConnectionOptions,
    mongosystem: ActorRef)

  private final class SupervisorActor(driver: MongoDriver) extends Actor {
    def isEmpty = driver.connectionMonitors.isEmpty

    val receive: Receive = {
      case AddConnection(name, _, opts, sys) => {
        logger.debug(
          s"[$supervisorName] Add connection to the supervisor: $name")

        val connection = new MongoConnection(
          supervisorName, name, driver.system, sys, opts)
        //connection.nodes = nodes

        driver.connectionMonitors.put(connection.monitor, connection)
        context.watch(connection.monitor)
        sender ! connection
      }

      case Terminated(actor) => {
        logger.debug(
          s"[$supervisorName] Pool actor is terminated: ${actor.path}")

        driver.connectionMonitors.remove(actor)
        ()
      }

      case Close => {
        logger.debug(s"[$supervisorName] Close the supervisor")

        if (isEmpty) {
          context.stop(self)
          sender() ! Closed
        } else {
          context.become(closing)
          driver.connectionMonitors.values.foreach(_.close())
        }
      }
    }

    def closing: Receive = {
      val waitingForClose: mutable.Queue[ActorRef] = mutable.Queue[ActorRef](sender)

      {
        case AddConnection(name, _, _, _) =>
          logger.warn(s"[$supervisorName] Refusing to add connection while MongoDriver is closing: $name")

        case Terminated(actor) =>
          driver.connectionMonitors.remove(actor).foreach { con =>
            logger.debug(
              s"[$supervisorName] Connection is terminated: ${con.name}")

            if (isEmpty) {
              context.stop(self)
              waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
            }
          }

        case Close =>
          logger.warn(s"[$supervisorName] Close received but already closing.")
          waitingForClose += sender
          ()
      }
    }

    override def postStop: Unit = {
      logger.info(s"[$supervisorName] Stopping the monitor...")

      ()
    }
  }
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

  private[api] val _counter = new AtomicLong(0)
  private[api] def nextCounter: Long = _counter.incrementAndGet()
}
