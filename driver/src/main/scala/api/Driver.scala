package reactivemongo.api

import java.util.concurrent.atomic.AtomicLong

import scala.util.{ Failure, Success }

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }

import com.typesafe.config.Config

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }
import akka.pattern.ask

import reactivemongo.core.actors.{
  Close,
  Closed,
  LegacyDBSystem,
  MongoDBSystem,
  StandardDBSystem,
  StandardDBSystemWithX509,
  StandardDBSystemWithScramSha256
}
import reactivemongo.core.nodeset.Authenticate
import reactivemongo.util.LazyLogger

/**
 * @define connectionNameParam the name for the connection pool
 * @define optionsParam the options for the new connection pool
 * @define nodesParam The list of node names (e.g. ''node1.foo.com:27017''); Port is optional (27017 is used by default)
 * @define authParam the list of authentication instructions
 * @define seeConnectDBTutorial See [[http://reactivemongo.org/releases/0.12/documentation/tutorial/connect-database.html how to connect to the database]]
 * @define uriStrictParam the strict URI, that will be parsed by [[reactivemongo.api.MongoConnection.parseURI]]
 */
private[api] trait Driver {
  /**
   * The custom configuration (otherwise the default options are used)
   */
  protected def config: Option[Config]

  /**
   * The classloader used to load the actor system
   */
  protected def classLoader: Option[ClassLoader]

  import scala.collection.mutable.{ Map => MutableMap }
  import Driver.logger
  import reactivemongo.core.{
    AsyncSystemControl,
    SystemControl,
    TimedSystemControl
  }

  /* Driver always uses its own ActorSystem
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

  private var closedBy = Array.empty[StackTraceElement]

  protected final val supervisorName =
    s"Supervisor-${Driver.counter.incrementAndGet()}"

  private[reactivemongo] final val supervisorActor =
    system.actorOf(Props(new SupervisorActor(this)), supervisorName)

  protected final val connectionMonitors =
    MutableMap.empty[ActorRef, MongoConnection]

  private val connectionCounter = new AtomicLong(0)

  /**
   * Closes this driver (and all its connections and resources).
   * Will wait until the timeout for proper closing of connections before forcing hard shutdown.
   */
  protected final def askClose(timeout: FiniteDuration)(implicit @deprecatedName(Symbol("executionContext")) ec: ExecutionContext): Future[Unit] = {
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
      (supervisorActor ? Close("Driver.askClose"))(Timeout(timeout)).recover {
        case err =>
          err.setStackTrace(callerSTE)

          logger.warn(s"[$supervisorName] Fails to close connections within timeout. Continuing closing of ReactiveMongo driver anyway.", err)
      }.flatMap { _ =>
        // ... and then shut down the ActorSystem as it is exiting.
        systemClose(Some(timeout))
      }
    }
  }

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes $nodesParam
   * @param options $optionsParam
   * @param name $connectionNameParam
   */
  protected final def askConnection(
    nodes: Seq[String], // TODO: Check nodes is empty
    options: MongoConnectionOptions,
    name: Option[String]): Future[MongoConnection] = {

    val nm = name.getOrElse(
      s"Connection-${connectionCounter.incrementAndGet()}")

    val authentications = options.credentials.map {
      case (db, c) => Authenticate(db, c.user, c.password)
    }.toSeq

    val opts = options.appName match {
      case Some(_) => options
      case _       => options.withAppName(s"${supervisorName}/${nm}")
    }

    // TODO: Passing ref to MongoDBSystem.history to AddConnection
    // TODO: pass opts.credentials.fallback

    lazy val dbsystem: MongoDBSystem = opts.authMode match {
      case CrAuthentication => new LegacyDBSystem(
        supervisorName, nm, nodes, authentications, opts)

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
      // TODO: Review
      c.history = () => dbsystem.internalState
      c
    }
  }

  private case class AddConnection(
    name: String,
    nodes: Seq[String],
    options: MongoConnectionOptions,
    mongosystem: ActorRef)

  // For testing only
  private[api] def addConnectionMsg(
    name: String,
    nodes: Seq[String],
    options: MongoConnectionOptions,
    mongosystem: ActorRef): Any =
    AddConnection(name, nodes, options, mongosystem)

  final class SupervisorActor(driver: Driver) extends Actor {
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

        sender ! connection
      }

      case Terminated(actor) => {
        logger.debug(
          s"[$supervisorName] Connection is terminated: ${actor.path}")

        driver.connectionMonitors.remove(actor)
        ()
      }

      case Close(src) => {
        logger.debug(s"[$supervisorName] Close the supervisor for $src")

        if (isEmpty) {
          context.stop(self)
          sender ! Closed
        } else {
          context.become(closing)

          // TODO
          implicit def timeout = FiniteDuration(10, "seconds")
          implicit def ec: ExecutionContext = context.dispatcher

          Future.sequence(driver.connectionMonitors.values.map(_.askClose()))

          ()
        }
      }
    }

    def closing: Receive = {
      val waitingForClose = mutable.Queue[ActorRef](sender)

      {
        case AddConnection(name, _, _, _) =>
          logger.warn(s"[$supervisorName] Refusing to add connection while the driver is closing: $name")

        case Terminated(actor) =>
          driver.connectionMonitors.remove(actor).foreach { con =>
            logger.debug(s"[$supervisorName] Connection is terminated: ${con.name}")

            if (isEmpty) {
              context.stop(self)
              waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
            }
          }

        case Close(src) if isEmpty => {
          logger.debug(s"[$supervisorName] Close the supervisor for $src")
          sender ! Closed
        }

        case Close(src) => {
          logger.warn(s"[$supervisorName] Close request received from $src, but already closing.")
          waitingForClose += sender
          ()
        }
      }
    }

    override def postStop: Unit = {
      logger.info(s"[$supervisorName] Stopping the monitor...")

      ()
    }
  }
}

/** The driver factory */
object Driver {
  private val logger = LazyLogger("reactivemongo.api.Driver")

  private[api] val counter = new AtomicLong(0)
}
