package reactivemongo.api

import java.util.concurrent.atomic.AtomicLong

import scala.util.{ Failure, Success }

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS, SECONDS }

import com.typesafe.config.Config

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }
import akka.pattern.ask

import reactivemongo.core.actors.{
  Close,
  Closed,
  MongoDBSystem,
  StandardDBSystem,
  StandardDBSystemWithX509,
  StandardDBSystemWithScramSha256
}

import reactivemongo.core.nodeset.Authenticate

/**
 * The asynchronous driver (see [[MongoConnection]]).
 *
 * {{{
 * import scala.concurrent.Future
 * import reactivemongo.api.{ AsyncDriver, MongoConnection }
 *
 * val driver = AsyncDriver()
 * val connection: Future[MongoConnection] =
 *   driver.connect("mongodb://node:27017")
 * }}}
 *
 * @param config a custom configuration (otherwise the default options are used)
 * @param classLoader a classloader used to load the actor system
 *
 * @define parsedURIParam the URI parsed by [[reactivemongo.api.MongoConnection.fromString]]
 * @define connectionNameParam the name for the connection pool
 * @define optionsParam the options for the new connection pool
 * @define nodesParam The list of node names (e.g. ''node1.foo.com:27017''); Port is optional (27017 is used by default)
 * @define authParam the list of authentication instructions
 * @define uriStrictParam the strict URI, that will be parsed by [[reactivemongo.api.MongoConnection.fromString]]
 */
final class AsyncDriver(
  protected val config: Option[Config] = None,
  protected val classLoader: Option[ClassLoader] = None) {

  import scala.collection.mutable.{ Map => MutableMap }
  import AsyncDriver.logger
  import reactivemongo.core.{
    AsyncSystemControl,
    SystemControl,
    TimedSystemControl
  }

  /* Driver always uses its own ActorSystem
   * so it can have complete control separate from other
   * Actor Systems in the application
   */
  private[reactivemongo] val system = {
    import com.typesafe.config.ConfigFactory

    val reference = config getOrElse ConfigFactory.load()
    val cfg = if (!reference.hasPath("mongo-async-driver")) {
      logger.info("No mongo-async-driver configuration found")
      ConfigFactory.empty()
    } else reference.getConfig("mongo-async-driver")

    ActorSystem("reactivemongo", Some(cfg), classLoader)
  }

  private[reactivemongo] def numConnections: Int = connectionMonitors.size

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes $nodesParam
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.MongoConnection
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * val con: Future[MongoConnection] = driver.connect(
   *   Seq("node1:27017", "node2:27017", "node3:27017"))
   * // with default options and automatic name
   * }}}
   */
  def connect(nodes: Seq[String]): Future[MongoConnection] =
    askConnection(nodes, MongoConnectionOptions.default, Option.empty)

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes $nodesParam
   * @param options $optionsParam
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.{ MongoConnection, MongoConnectionOptions }
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * val con: Future[MongoConnection] = driver.connect(
   *   nodes = Seq("node1:27017", "node2:27017", "node3:27017"),
   *   options = MongoConnectionOptions.default.copy(nbChannelsPerNode = 10))
   * // with automatic name
   * }}}
   */
  def connect(
    nodes: Seq[String],
    options: MongoConnectionOptions): Future[MongoConnection] =
    askConnection(nodes, options, Option.empty)

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes $nodesParam
   * @param options $optionsParam
   * @param name $connectionNameParam
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.{ MongoConnection, MongoConnectionOptions }
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * val con: Future[MongoConnection] = driver.connect(
   *   nodes = Seq("node1:27017", "node2:27017", "node3:27017"),
   *   options = MongoConnectionOptions.default.copy(nbChannelsPerNode = 10),
   *   name = "ConnectionName")
   * }}}
   */
  def connect(
    nodes: Seq[String],
    options: MongoConnectionOptions,
    name: String): Future[MongoConnection] = askConnection(nodes, options, Some(name))

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param uriStrict $uriStrictParam
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.MongoConnection
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * val con: Future[MongoConnection] = driver.connect("mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authenticationMechanism=scram-sha1")
   * // with automatic name
   * }}}
   */
  def connect(uriStrict: String): Future[MongoConnection] =
    connect(uriStrict, name = None)

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param uriStrict $uriStrictParam
   * @param name $connectionNameParam
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.MongoConnection
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * val con: Future[MongoConnection] = driver.connect("mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authenticationMechanism=scram-sha1", name = Some("ConnectionName"))
   * }}}
   */
  def connect(uriStrict: String, name: Option[String]): Future[MongoConnection] = {
    implicit def ec: ExecutionContext =
      reactivemongo.util.sameThreadExecutionContext

    MongoConnection.fromString(uriStrict).flatMap {
      connect(_, name)
    }
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.fromString]]
   * @param name $connectionNameParam
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.MongoConnection, MongoConnection.ParsedURI
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * def con(uri: ParsedURI): Future[MongoConnection] =
   *   driver.connect(uri, name = Some("ConnectionName"))
   * }}}
   */
  def connect[T](parsedURI: MongoConnection.URI[T], name: Option[String]): Future[MongoConnection] = connect(parsedURI, name, true)

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.fromString]]
   * @param name $connectionNameParam
   * @param strictMode the flag to indicate whether the given URI must be a strict one (no ignored/invalid options)
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.MongoConnection, MongoConnection.ParsedURI
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * def con(uri: ParsedURI): Future[MongoConnection] =
   *   driver.connect(uri, name = Some("ConnectionName"))
   * }}}
   */
  def connect[T](
    parsedURI: MongoConnection.URI[T],
    name: Option[String],
    strictMode: Boolean): Future[MongoConnection] = {
    if (strictMode && parsedURI.ignoredOptions.nonEmpty) {
      Future.failed(new IllegalArgumentException(s"The connection URI contains unsupported options: ${parsedURI.ignoredOptions.mkString(", ")}"))
    } else {
      askConnection(
        parsedURI.hosts.map(h => h._1 + ':' + h._2).toSeq,
        parsedURI.options.copy(credentials = parsedURI.options.credentials),
        name)
    }
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * @param parsedURI $parsedURIParam
   *
   * {{{
   * import scala.concurrent.Future
   * import reactivemongo.api.MongoConnection, MongoConnection.ParsedURI
   *
   * val driver = reactivemongo.api.AsyncDriver()
   *
   * def con(uri: ParsedURI): Future[MongoConnection] = driver.connect(uri)
   * // with automatic name
   * }}}
   */
  def connect[T](parsedURI: MongoConnection.URI[T]): Future[MongoConnection] =
    connect(parsedURI, None)

  private var closedBy = Array.empty[StackTraceElement]

  /**
   * Closes this driver (and all its connections and resources).
   * Will wait until the timeout for proper closing of connections
   * before forcing hard shutdown.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * def afterClose(drv: reactivemongo.api.AsyncDriver)(
   *   implicit ec: ExecutionContext) = drv.close().andThen {
   *     case res => println("Close 'Try' result: " + res)
   *   }
   * }}}
   */
  def close(timeout: FiniteDuration = FiniteDuration(2, SECONDS))(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"[$supervisorName] Closing instance of ReactiveMongo driver")

    val callerSTE = Thread.currentThread.getStackTrace.drop(3).take(3)

    val alreadyClosing = systemClose.synchronized {
      if (closedBy.isEmpty) {
        closedBy = callerSTE
        false
      } else {
        true
      }
    }

    if (alreadyClosing) {
      logger.info(s"System already closed: $supervisorName")

      Future.successful({})
    } else {
      // Tell the supervisor to close.
      // It will shut down all the connections and monitors
      def msg = Close("AsyncDriver.close", timeout)

      (supervisorActor ? msg)(Timeout(timeout)).recover {
        case err =>
          err.setStackTrace(callerSTE)

          logger.warn(s"[$supervisorName] Fails to close connections within timeout. Continuing closing of ReactiveMongo driver anyway.", err)
      }.flatMap { _ =>
        // ... and then shut down the ActorSystem as it is exiting.
        systemClose(Some(timeout))
      }
    }
  }

  // ---

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

  protected final val supervisorName =
    s"Supervisor-${AsyncDriver.counter.incrementAndGet()}"

  private[reactivemongo] final val supervisorActor =
    system.actorOf(Props(new SupervisorActor(this)), supervisorName)

  protected final val connectionMonitors =
    MutableMap.empty[ActorRef, MongoConnection]

  private val connectionCounter = new AtomicLong(0)

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes $nodesParam
   * @param options $optionsParam
   * @param name $connectionNameParam
   */
  protected def askConnection(
    nodes: Seq[String],
    options: MongoConnectionOptions,
    name: Option[String]): Future[MongoConnection] = {

    if (nodes.isEmpty) {
      Future.failed[MongoConnection](
        new reactivemongo.core.errors.ConnectionException("No node specified"))
    } else {
      val nm = name.getOrElse(
        s"Connection-${connectionCounter.incrementAndGet()}")

      val authentications = options.credentials.map {
        case (db, c) => Authenticate(db, c.user, c.password)
      }.toSeq

      val opts = options.appName match {
        case Some(_) => options
        case _       => options.copy(appName = Some(s"${supervisorName}/${nm}"))
      }

      lazy val dbsystem: MongoDBSystem = opts.authenticationMechanism match {
        case X509Authentication => new StandardDBSystemWithX509(
          supervisorName, nm, nodes, authentications, opts)

        case ScramSha256Authentication => new StandardDBSystemWithScramSha256(
          supervisorName, nm, nodes, authentications, opts)

        case _ => new StandardDBSystem(
          supervisorName, nm, nodes, authentications, opts)
      }

      val mongosystem = system.actorOf(Props(dbsystem), nm)

      def timeout = if (opts.connectTimeoutMS > 0) {
        Timeout(opts.connectTimeoutMS.toLong, MILLISECONDS)
      } else {
        Timeout(10000L, MILLISECONDS) // 10s
      }

      def connection = (supervisorActor ? AddConnection(
        nm, nodes, opts, mongosystem))(timeout)

      logger.info(s"[$supervisorName] Creating connection: $nm")

      import system.dispatcher

      connection.mapTo[MongoConnection].map { c =>
        c.history = () => dbsystem.internalState()
        c
      }
    }
  }

  // ---

  private case class AddConnection(
    name: String,
    nodes: Seq[String],
    options: MongoConnectionOptions,
    mongosystem: ActorRef)

  // For testing only
  @SuppressWarnings(Array("MethodReturningAny"))
  private[api] def addConnectionMsg(
    name: String,
    nodes: Seq[String],
    options: MongoConnectionOptions,
    mongosystem: ActorRef): Any =
    AddConnection(name, nodes, options, mongosystem)

  private final class SupervisorActor(driver: AsyncDriver) extends Actor {
    @inline def isEmpty = driver.connectionMonitors.isEmpty

    val receive: Receive = {
      case AddConnection(name, _, opts, sys) => {
        logger.debug(
          s"[$supervisorName] Add connection to the supervisor: $name")

        val connection = new MongoConnection(
          supervisorName, name, driver.system, sys, opts)
        //connection.nodes = nodes

        driver.connectionMonitors.put(connection.monitor, connection)

        context.watch(connection.monitor)

        sender() ! connection
      }

      case term: Terminated => {
        import term.actor

        logger.debug(
          s"[$supervisorName] Connection is terminated: ${actor.path}")

        driver.connectionMonitors.remove(actor)
        ()
      }

      case close @ Close(src) => {
        logger.debug(s"[$supervisorName] Close the supervisor for $src")

        if (isEmpty) {
          context.stop(self)
          sender() ! Closed
        } else {
          context.become(closing)

          implicit def timeout: FiniteDuration = close.timeout
          implicit def ec: ExecutionContext = context.dispatcher

          Future.sequence(driver.connectionMonitors.values.map(_.close()))

          ()
        }
      }
    }

    def closing: Receive = {
      val waitingForClose = mutable.Queue[ActorRef](sender())

      {
        case AddConnection(name, _, _, _) =>
          logger.warn(s"[$supervisorName] Refusing to add connection while the driver is closing: $name")

        case term: Terminated =>
          import term.actor

          driver.connectionMonitors.remove(actor).foreach { con =>
            logger.debug(s"[$supervisorName] Connection is terminated: ${con.name}")

            if (isEmpty) {
              context.stop(self)
              waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
            }
          }

        case Close(src) if isEmpty => {
          logger.debug(s"[$supervisorName] Close the supervisor for $src")
          sender() ! Closed
        }

        case Close(src) => {
          logger.warn(s"[$supervisorName] Close request received from $src, but already closing.")
          waitingForClose += sender()
          ()
        }
      }
    }

    override def postStop(): Unit = {
      logger.info(s"[$supervisorName] Stopping the monitor...")

      ()
    }
  }
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

  // ---

  private val logger = reactivemongo.util.LazyLogger("reactivemongo.api.Driver")

  private[api] val counter = new AtomicLong(0)
}
