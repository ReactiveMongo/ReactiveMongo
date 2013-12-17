/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.api

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import org.jboss.netty.buffer.ChannelBuffer
import play.api.libs.iteratee._
import reactivemongo.api.indexes._
import reactivemongo.core.actors._
import reactivemongo.core.nodeset.Authenticate
import reactivemongo.bson._
import reactivemongo.core.protocol._
import reactivemongo.core.commands.{ Command, GetLastError, LastError, SuccessfulAuthentication }
import reactivemongo.utils.LazyLogger
import reactivemongo.utils.EitherMappableFuture._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

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
class Failover[T](message: T, connection: MongoConnection, strategy: FailoverStrategy)(expectingResponseMaker: T => ExpectingResponse)(implicit ec: ExecutionContext) {
  import Failover.logger
  import reactivemongo.core.errors._
  import reactivemongo.core.actors.Exceptions._
  private val promise = Promise[Response]()

  /** A future that is completed with a response, after 1 or more attempts (specified in the given strategy). */
  val future: Future[Response] = promise.future

  private def send(n: Int) {
    val expectingResponse = expectingResponseMaker(message)
    connection.mongosystem ! expectingResponse
    expectingResponse.future.onComplete {
      case Failure(e) if isRetryable(e) =>
        if (n < strategy.retries) {
          val `try` = n + 1
          val delayFactor = strategy.delayFactor(`try`)
          val delay = Duration.unapply(strategy.initialDelay * delayFactor).map(t => FiniteDuration(t._1, t._2)).getOrElse(strategy.initialDelay)
          logger.debug("Got an error, retrying... (try #" + `try` + " is scheduled in " + delay.toMillis + " ms)", e)
          connection.actorSystem.scheduler.scheduleOnce(delay)(send(`try`))
        } else {
          // generally that means that the primary is not available or the nodeset is unreachable
          logger.error("Got an error, no more attempts to do. Completing with a failure...", e)
          promise.failure(e)
        }
      case Failure(e) =>
        logger.trace("Got an non retryable error, completing with a failure...", e)
        promise.failure(e)
      case Success(response) =>
        logger.trace("Got a successful result, completing...")
        promise.success(response)
    }
  }

  private def isRetryable(throwable: Throwable) = throwable match {
    case PrimaryUnavailableException | NodeSetNotReachable => true
    case e: DatabaseException if e.isNotAPrimaryError || e.isUnauthorized => true
    case _: ConnectionException => true
    case _ => false
  }

  send(0)
}

object Failover {
  private val logger = LazyLogger("reactivemongo.api.Failover")
  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param checkedWriteRequest The checkedWriteRequest to send to the given actor.
   * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  def apply(checkedWriteRequest: CheckedWriteRequest, connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext): Failover[CheckedWriteRequest] =
    new Failover(checkedWriteRequest, connection, strategy)(CheckedWriteRequestExpectingResponse.apply)

  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param requestMaker The requestMaker to send to the given actor.
   * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  def apply(requestMaker: RequestMaker, connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext): Failover[RequestMaker] =
    new Failover(requestMaker, connection, strategy)(RequestMakerExpectingResponse.apply)
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
 * {{{
 * import reactivemongo.api._
 *
 * val connection = MongoConnection( List( "localhost" ) )
 * val db = connection("plugin")
 * val collection = db("acoll")
 *
 * // more explicit way
 * val db2 = connection.db("plugin")
 * val collection2 = db2.collection("plugin")
 * }}}
 *
 * @param mongosystem A reference to a [[reactivemongo.core.actors.MongoDBSystem]] Actor.
 */
class MongoConnection(
    val actorSystem: ActorSystem,
    val mongosystem: ActorRef,
    val monitor: ActorRef) {
  import akka.pattern.{ ask => akkaAsk }
  import akka.util.Timeout
  /**
   * Returns a DefaultDB reference using this connection.
   *
   * @param name The database name.
   * @param failoverStrategy a failover strategy for sending requests.
   */
  def apply(name: String, failoverStrategy: FailoverStrategy = FailoverStrategy())(implicit context: ExecutionContext): DefaultDB = DefaultDB(name, this, failoverStrategy)

  /**
   * Returns a DefaultDB reference using this connection (alias for the `apply` method).
   *
   * @param name The database name.
   * @param failoverStrategy a failover strategy for sending requests.
   */
  def db(name: String, failoverStrategy: FailoverStrategy = FailoverStrategy())(implicit context: ExecutionContext): DefaultDB = apply(name, failoverStrategy)

  /**
   * Get a future that will be successful when a primary node is available or times out.
   */
  def waitForPrimary(implicit waitForAvailability: FiniteDuration): Future[_] =
    akkaAsk(monitor, reactivemongo.core.actors.WaitForPrimary)(Timeout(waitForAvailability))

  /**
   * Writes a request and wait for a response.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(message: RequestMaker): Future[Response] = {
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
  def authenticate(db: String, user: String, password: String): Future[SuccessfulAuthentication] = {
    val req = AuthRequest(Authenticate(db, user, password))
    mongosystem ! req
    req.future
  }

  /** Closes this MongoConnection (closes all the channels and ends the actors) */
  def askClose()(implicit timeout: FiniteDuration): Future[_] =
    akkaAsk(monitor, Close)(Timeout(timeout))

  /** Closes this MongoConnection (closes all the channels and ends the actors) */
  def close(): Unit = monitor ! Close
}

class MongoDriver(systemOption: Option[ActorSystem] = None) {

  def this(system: ActorSystem) = this(Some(system))

  @volatile private var _connections = List[MongoConnection]()

  /** Keep a list of all connections so that we can terminate the actors */
  def connections: Seq[MongoConnection] = _connections

  val system = systemOption.getOrElse(MongoDriver.defaultSystem)

  def close() = systemOption match {
    // Non default actor system -- terminate actors used by MongoConnections 
    case Some(_) =>
      connections.foreach { connection =>
        connection.monitor ! Close
      }
    // Default actor system -- just shut it down
    case None => system.shutdown()
  }

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes A list of node names, like ''node1.foo.com:27017''. Port is optional, it is 27017 by default.
   * @param authentications A list of Authenticates.
   * @param nbChannelsPerNode Number of channels to open per node. Defaults to 10.
   * @param name The name of the newly created [[reactivemongo.core.actors.MongoDBSystem]] actor, if needed.
   */
  def connection(nodes: Seq[String], authentications: Seq[Authenticate] = Seq.empty, nbChannelsPerNode: Int = 10, name: Option[String] = None) = {
    val props = Props(new MongoDBSystem(nodes, authentications, nbChannelsPerNode))
    val mongosystem = if (name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props)
    val monitor = system.actorOf(Props(new MonitorActor(mongosystem)))
    val connection = new MongoConnection(system, mongosystem, monitor)
    this.synchronized {
      _connections = connection :: _connections
    }
    connection
  }
}

object MongoDriver {

  /** Default ActorSystem used in the default MongoDriver constructor. */
  private def defaultSystem = {
    import com.typesafe.config.ConfigFactory
    val config = ConfigFactory.load()
    ActorSystem("reactivemongo", config.getConfig("mongo-async-driver"))
  }

  /** Creates a new MongoDriver with a new ActorSystem. */
  def apply() = new MongoDriver

  /**
   * Creates a new MongoDriver with specified ActorSystem.
   *
   * @param system An ActorSystem for ReactiveMongo to use.
   */
  def apply(system: ActorSystem) = new MongoDriver(system)

}