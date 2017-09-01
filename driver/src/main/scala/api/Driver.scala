package reactivemongo.api

import java.util.concurrent.atomic.AtomicLong

import scala.util.{ Failure, Success }

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ FiniteDuration, SECONDS }

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

  protected final val supervisorName = s"Supervisor-${Driver.nextCounter}"
  private[reactivemongo] final val supervisorActor =
    system.actorOf(Props(new SupervisorActor(this)), supervisorName)

  protected final val connectionMonitors =
    MutableMap.empty[ActorRef, MongoConnection]

  /**
   * Closes this driver (and all its connections and resources).
   * Will wait until the timeout for proper closing of connections before forcing hard shutdown.
   */
  protected final def askClose(timeout: FiniteDuration)(implicit executionContext: ExecutionContext): Future[Unit] = {
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
   * @param nodes $nodesParam
   * @param authentications $authParam
   * @param name $connectionNameParam
   * @param options $optionsParam
   */
  protected final def askConnection(
    nodes: Seq[String],
    options: MongoConnectionOptions,
    authentications: Seq[Authenticate],
    name: Option[String]): Future[MongoConnection] = {

    val nm = name.getOrElse(s"Connection-${+Driver.nextCounter}")

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
      val waitingForClose = mutable.Queue[ActorRef](sender)

      {
        case AddConnection(name, _, _, _) =>
          logger.warn(s"[$supervisorName] Refusing to add connection while the driver is closing: $name")

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
object Driver {
  private val logger = LazyLogger("reactivemongo.api.Driver")

  private[api] val _counter = new AtomicLong(0)
  private[api] def nextCounter: Long = _counter.incrementAndGet()
}
