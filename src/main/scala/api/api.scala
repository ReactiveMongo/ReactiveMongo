package reactivemongo.api

import akka.actor.{ActorRef, ActorSystem, Props}
import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.iteratee._
import reactivemongo.api.indexes._
import reactivemongo.core.actors._
import reactivemongo.bson._
import reactivemongo.bson.handlers._
import reactivemongo.core.protocol._
import reactivemongo.core.commands.{Command, GetLastError, LastError, SuccessfulAuthentication}
import reactivemongo.utils.EitherMappableFuture._
import reactivemongo.utils.DebuggingPromise
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * A helper that sends the given message to the given actor, following a failover strategy.
 * This helper holds a future reference that is completed with a response, after 1 or more attempts (specified in the given strategy).
 * If the all the tryouts configured by the given strategy were unsuccessful, the future reference is completed with a Throwable.
 *
 * Should not be used directly for most use cases.
 *
 * @tparam T Type of the message to send.
 * @param message The message to send to the given actor. This message will be wrapped into an ExpectingResponse message by the `expectingResponseMaker` function.
 * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
 * @param strategy The Failover strategy.
 * @param expectingResponseMaker A function that takes a message of type `T` and wraps it into an ExpectingResponse message.
 */
class Failover[T](message: T, actorRef: ActorRef, strategy: FailoverStrategy)(expectingResponseMaker: T => ExpectingResponse)(implicit ec: ExecutionContext) {
  import Failover.logger
  private val promise = DebuggingPromise(scala.concurrent.Promise[Response]())

  /** A future that is completed with a response, after 1 or more attempts (specified in the given strategy). */
  val future: Future[Response] = promise.future

  private def send(n: Int) {
    val expectingResponse = expectingResponseMaker(message)
    actorRef ! expectingResponse
    expectingResponse.future.onComplete {
      case Failure(e) =>
        if(n < strategy.retries) {
          val `try` = n + 1
          val delayFactor = strategy.delayFactor(`try`)
          val delay = Duration.unapply(strategy.initialDelay * delayFactor).map(t => FiniteDuration(t._1, t._2)).getOrElse(strategy.initialDelay)
          logger.warn("Got an error, retrying... (try #" + `try` + " is scheduled in " + delay.toMillis + " ms)", e)
          MongoConnection.system.scheduler.scheduleOnce(delay)(send(`try`))
        } else {
          logger.error("Got an error, no more attempts to do. Completing with an error...")
          promise.failure(e)
        }
      case Success(response) =>
        logger.debug("Got a successful result, completing...")
        promise.success(response)
    }
  }

  send(0)
}

object Failover {
  private val logger = LoggerFactory.getLogger("Failover")
  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param checkedWriteRequest The checkedWriteRequest to send to the given actor.
   * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  def apply(checkedWriteRequest: CheckedWriteRequest, actorRef: ActorRef, strategy: FailoverStrategy)(implicit ec: ExecutionContext) :Failover[CheckedWriteRequest] =
    new Failover(checkedWriteRequest, actorRef, strategy)(CheckedWriteRequestExpectingResponse.apply)

  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param requestMaker The requestMaker to send to the given actor.
   * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  def apply(requestMaker: RequestMaker, actorRef: ActorRef, strategy: FailoverStrategy)(implicit ec: ExecutionContext) :Failover[RequestMaker] =
    new Failover(requestMaker, actorRef, strategy)(RequestMakerExpectingResponse.apply)
}

/**
 * A failover strategy for sending requests.
 *
 * @param initialDelay the initial delay between the first failed attempt and the next one.
 * @param retries the number of retries to do before giving up.
 * @param delayFactor a function that takes the current iteration and returns a factor to be applied to the initialDelay.
 */
case class FailoverStrategy(
  initialDelay: FiniteDuration = 500 milliseconds,
  retries: Int = 5,
  delayFactor: Int => Double = n => 1)



/**
 * A Mongo Connection.
 *
 * This is a wrapper around a reference to a [[reactivemongo.core.actors.MongoDBSystem]] Actor.
 * Connection here does not mean that there is one open channel to the server.
 * Behind the scene, many connections (channels) are open on all the available servers in the replica set.
 *
 * Example:
{{{
import reactivemongo.api._

val connection = MongoConnection( List( "localhost:27016" ) )
val db = connection("plugin")
val collection = db("acoll")

// more explicit way
val db2 = connection.db("plugin")
val collection2 = db2.collection("plugin")
}}}
 *
 * @param mongosystem A reference to a [[reactivemongo.core.actors.MongoDBSystem]] Actor.
 */
class MongoConnection(
  val mongosystem: ActorRef,
  monitor: ActorRef
) {
  import akka.pattern.{ask => akkaAsk}
  import akka.util.Timeout
  /**
   * Returns a DefaultDB reference using this connection.
   *
   * @param name The database name.
   * @param failoverStrategy a failover strategy for sending requests.
   */
  def apply(name: String, failoverStrategy: FailoverStrategy = FailoverStrategy())(implicit context: ExecutionContext) :DefaultDB = DefaultDB(name, this, failoverStrategy)

  /**
   * Returns a DefaultDB reference using this connection (alias for the `apply` method).
   *
   * @param name The database name.
   * @param failoverStrategy a failover strategy for sending requests.
   */
  def db(name: String, failoverStrategy: FailoverStrategy = FailoverStrategy())(implicit context: ExecutionContext) :DefaultDB = apply(name, failoverStrategy)

  /**
   * Get a future that will be successful when a primary node is available or times out.
   */
  def waitForPrimary(implicit waitForAvailability: FiniteDuration) :Future[_] =
    akkaAsk(monitor, reactivemongo.core.actors.WaitForPrimary)(Timeout(waitForAvailability))

  /**
   * Writes a request and wait for a response.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(message: RequestMaker) :Future[Response] = {
    val msg = RequestMakerExpectingResponse(message)
    mongosystem ! msg
    msg.future
  }

  /**
   * Writes a checked write request and wait for a response.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(checkedWriteRequest: CheckedWriteRequest) = {
    val msg = CheckedWriteRequestExpectingResponse(checkedWriteRequest)
    mongosystem ! msg
    msg.future
  }

  /**
   * Writes a request and drop the response if any.
   *
   * @param message The request maker.
   */
  def send(message: RequestMaker) = mongosystem ! message

  /** Authenticates the connection on the given database. */
  def authenticate(db: String, user: String, password: String)(implicit timeout: FiniteDuration) :Future[SuccessfulAuthentication] =
    akkaAsk(mongosystem, Authenticate(db, user, password))(Timeout(timeout)).mapTo[SuccessfulAuthentication]

  /** Closes this MongoConnection (closes all the channels and ends the actors) */
  def askClose()(implicit timeout: FiniteDuration) :Future[_] =
    akkaAsk(monitor, Close)(Timeout(timeout))

  /** Closes this MongoConnection (closes all the channels and ends the actors) */
  def close() :Unit = monitor ! Close
}

object MongoConnection {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load()

  /**
   * The actor system that creates all the required actors.
   */
  val system = ActorSystem("mongodb", config.getConfig("mongo-async-driver"))

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes A list of node names, like ''node1.foo.com:27017''. Port is optional, it is 27017 by default.
   * @param authentications A list of Authenticates.
   * @param nbChannelsPerNode Number of channels to open per node. Defaults to 10.
   * @param name The name of the newly created [[reactivemongo.core.actors.MongoDBSystem]] actor, if needed.
   */
  def apply(nodes: List[String], authentications :List[Authenticate] = List.empty, nbChannelsPerNode :Int = 10, name: Option[String] = None) = {
    val props = Props(new MongoDBSystem(nodes, authentications, nbChannelsPerNode))
    val mongosystem = if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props)
    val monitor = system.actorOf(Props(new MonitorActor(mongosystem)))
    new MongoConnection(mongosystem, monitor)
  }
}