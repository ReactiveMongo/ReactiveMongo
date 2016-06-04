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

import java.util.concurrent.{ TimeoutException, atomic }, atomic.AtomicLong

import scala.util.{ Try, Failure, Success }
import scala.util.control.{ NonFatal, NoStackTrace }

import scala.concurrent.{
  Await,
  ExecutionContext,
  Future,
  Promise
}
import scala.concurrent.duration.{ Duration, FiniteDuration, SECONDS }

import com.typesafe.config.Config

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }
import akka.pattern.{ after, ask }

import reactivemongo.core.actors.{
  AuthRequest,
  CheckedWriteRequestExpectingResponse,
  Close,
  Closed,
  Exceptions,
  ExpectingResponse,
  LegacyDBSystem,
  MongoDBSystem,
  PrimaryAvailable,
  PrimaryUnavailable,
  RegisterMonitor,
  RequestMakerExpectingResponse,
  SetAvailable,
  SetUnavailable,
  StandardDBSystem
}
import reactivemongo.core.errors.ConnectionException
import reactivemongo.core.nodeset.{ Authenticate, ProtocolMetadata }
import reactivemongo.core.protocol.{
  CheckedWriteRequest,
  MongoWireVersion,
  RequestMaker,
  Response
}
import reactivemongo.core.commands.SuccessfulAuthentication
import reactivemongo.api.commands.WriteConcern
import reactivemongo.util.LazyLogger

/**
 * A helper that sends the given message to the given actor,
 * following a failover strategy.
 * This helper holds a future reference that is completed with a response,
 * after 1 or more attempts (specified in the given strategy).
 * If the all the tryouts configured by the given strategy were unsuccessful,
 * the future reference is completed with a Throwable.
 *
 * Should not be used directly for most use cases.
 *
 * @tparam T Type of the message to send.
 * @param message The message to send to the given actor. This message will be wrapped into an ExpectingResponse message by the `expectingResponseMaker` function.
 * @param connection The reference to the MongoConnection the given message will be sent to.
 * @param strategy The Failover strategy.
 * @param expectingResponseMaker A function that takes a message of type `T` and wraps it into an ExpectingResponse message.
 */
class Failover[T](message: T, connection: MongoConnection, strategy: FailoverStrategy)(expectingResponseMaker: T => ExpectingResponse)(implicit ec: ExecutionContext) {
  import Failover2.logger
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
          logger.debug(s"Got an error, retrying... (try #${`try`} is scheduled in ${delay.toMillis} ms)", e)
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
    case _: ConnectionNotInitialized => true
    case _ => false
  }

  send(0)
}

@deprecated(message = "Will be made private", since = "0.11.11")
class Failover2[A](producer: () => Future[A], connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext) {
  import Failover2.logger, logger.trace
  import reactivemongo.core.errors._
  import reactivemongo.core.actors.Exceptions._

  /**
   * A future that is completed with a response,
   * after 1 or more attempts (specified in the given strategy).
   */
  val future: Future[A] = send(0) //promise.future

  // Wraps any exception from the producer
  // as a result Future.failed that can be recovered.
  private def next(): Future[A] = try {
    producer()
  } catch {
    case producerErr: Throwable => Future.failed[A](producerErr)
  }

  private def _next(): Future[A] = producer()

  private val mutex = {
    val sem = new java.util.concurrent.Semaphore(1, true)
    sem.drainPermits()
    sem
  }

  private def send(n: Int): Future[A] =
    next().map[Try[A]](Success(_)).recover[Try[A]] {
      case err => Failure(err)
    }.flatMap {
      case Failure(e) if isRetryable(e) => {
        if (n < strategy.retries) {
          val `try` = n + 1
          val delayFactor = strategy.delayFactor(`try`)
          val delay = Duration.unapply(strategy.initialDelay * delayFactor).
            fold(strategy.initialDelay)(t => FiniteDuration(t._1, t._2))

          logger.debug(s"Got an error, retrying... (try #${`try`} is scheduled in ${delay.toMillis} ms)", e)

          after(delay, connection.actorSystem.scheduler)(send(`try`))
        } else {
          // generally that means that the primary is not available
          // or the nodeset is unreachable
          logger.error("Got an error, no more attempts to do. Completing with a failure... ", e)

          Future.failed(e)
        }
      }

      case Failure(e) => {
        trace("Got an non retryable error, completing with a failure... ", e)
        Future.failed(e)
      }

      case Success(response) => {
        trace("Got a successful result, completing...")
        Future.successful(response)
      }
    }

  private def isRetryable(throwable: Throwable) = throwable match {
    case PrimaryUnavailableException | NodeSetNotReachable => true
    case e: DatabaseException if e.isNotAPrimaryError || e.isUnauthorized => true
    case _: ConnectionException => true
    case _: ConnectionNotInitialized => true
    case _ => false
  }

  //send(0)
}

object Failover2 {
  private[api] val logger = LazyLogger("reactivemongo.api.Failover2")

  def apply[A](connection: MongoConnection, strategy: FailoverStrategy)(producer: () => Future[A])(implicit ec: ExecutionContext): Failover2[A] =
    new Failover2(producer, connection, strategy)
}

@deprecated(message = "Unused", since = "0.11.10")
object Failover {
  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param checkedWriteRequest The checkedWriteRequest to send to the given actor.
   * @param connection The reference to the MongoConnection the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  @deprecated(message = "Unused", since = "0.11.10")
  def apply(checkedWriteRequest: CheckedWriteRequest, connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext): Failover[CheckedWriteRequest] =
    new Failover(checkedWriteRequest, connection, strategy)(CheckedWriteRequestExpectingResponse.apply)

  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param requestMaker The requestMaker to send to the given actor.
   * @param connection The reference to the MongoConnection actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  @deprecated(message = "Unused", since = "0.11.10")
  def apply(requestMaker: RequestMaker, connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext): Failover[RequestMaker] =
    new Failover(requestMaker, connection, strategy)(RequestMakerExpectingResponse(_, false))
}

/**
 * A failover strategy for sending requests.
 * The default uses 8 retries:
 * 125ms, 250ms, 375ms, 500ms, 625ms, 750ms, 875ms, 1s
 *
 * @param initialDelay the initial delay between the first failed attempt and the next one.
 * @param retries the number of retries to do before giving up.
 * @param delayFactor a function that takes the current iteration and returns a factor to be applied to the initialDelay.
 */
case class FailoverStrategy(
  initialDelay: FiniteDuration = FiniteDuration(100, "ms"),
  retries: Int = 8,
  delayFactor: Int => Double = _ * 1.25D)

object FailoverStrategy {
  /** The default strategy */
  val default = FailoverStrategy()

  /** The strategy when the MongoDB nodes are remote */
  val remote = FailoverStrategy(retries = 16)
}

/**
 * A pool of MongoDB connections.
 *
 * Connection here does not mean that there is one open channel to the server:
 * behind the scene, many connections (channels) are open on all the available servers in the replica set.
 *
 * Example:
 * {{{
 * import reactivemongo.api._
 *
 * val connection = MongoConnection(List("localhost"))
 * val db = connection.database("plugin")
 * val collection = db.map(_.("acoll"))
 * }}}
 *
 * @param mongosystem the reference to the internal [[reactivemongo.core.actors.MongoDBSystem]] Actor.
 */
class MongoConnection(
    val actorSystem: ActorSystem,
    val mongosystem: ActorRef,
    val options: MongoConnectionOptions) {

  private[api] val logger = LazyLogger("reactivemongo.api.Failover2")

  /**
   * Returns a DefaultDB reference using this connection.
   *
   * @param name the database name
   * @param failoverStrategy the failover strategy for sending requests.
   */
  @deprecated(message = "Use [[database]]", since = "0.11.8")
  def apply(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit context: ExecutionContext): DefaultDB = {
    metadata.foreach {
      case ProtocolMetadata(_, MongoWireVersion.V24AndBefore, _, _, _) =>
        throw ConnectionException("unsupported MongoDB version < 2.6")

      case meta => ()
    }

    DefaultDB(name, this, failoverStrategy)
  }

  /**
   * Returns a DefaultDB reference using this connection
   * (alias for the `apply` method).
   *
   * @param name the database name
   * @param failoverStrategy the failover strategy for sending requests.
   */
  @deprecated(message = "Must use [[apply]]", since = "0.11.8")
  def db(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit context: ExecutionContext): DefaultDB = apply(name, failoverStrategy)

  /**
   * Returns a DefaultDB reference using this connection.
   * The failover strategy is also used to wait for the node set to be ready,
   * before returning an available DB.
   *
   * @param name the database name
   * @param failoverStrategy the failover strategy for sending requests.
   */
  def database(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit context: ExecutionContext): Future[DefaultDB] =
    waitIsAvailable(failoverStrategy).map(_ => apply(name, failoverStrategy))

  /** Returns a future that will be successful when node set is available. */
  private[api] def waitIsAvailable(failoverStrategy: FailoverStrategy)(implicit ec: ExecutionContext): Future[Unit] = {
    @inline def nextTimeout(i: Int): Duration = {
      val delayFactor = failoverStrategy.delayFactor(i)
      failoverStrategy.initialDelay * delayFactor
    }

    def wait(iteration: Int, attempt: Int, timeout: Duration): Future[Unit] = {
      if (attempt == 0) Future.failed(Exceptions.NodeSetNotReachable)
      else {
        Future { // TODO: Refactor
          val ms = timeout.toMillis

          try {
            val before = System.currentTimeMillis
            val result = Await.result(isAvailable, timeout)
            val duration = System.currentTimeMillis - before

            if (result) true
            else {
              Thread.sleep(ms - duration)
              false
            }
          } catch {
            case e: Throwable =>
              Thread.sleep(ms)
              throw e
          }
        }.flatMap {
          case false if (attempt > 0) => Future.failed[Unit](
            new scala.RuntimeException("Got an error, no more attempt to do."))

          case _ => Future.successful({})
        }.recoverWith {
          case error =>
            val nextIt = iteration + 1
            wait(nextIt, attempt - 1, nextTimeout(nextIt))
        }
      }
    }

    wait(0, 1 + failoverStrategy.retries, failoverStrategy.initialDelay).
      flatMap { _ =>
        metadata match {
          case Some(ProtocolMetadata(
            _, MongoWireVersion.V24AndBefore, _, _, _)) =>
            Future.failed[Unit](ConnectionException(
              "unsupported MongoDB version < 2.6"))

          case Some(_) => Future successful {}
          case _ => Future.failed[Unit](ConnectionException(
            "protocol metadata not available"))
        }
      }
  }

  /**
   * Writes a request and drop the response if any.
   *
   * @param message The request maker.
   */
  private[api] def send(message: RequestMaker): Unit = mongosystem ! message

  private def whenActive[T](f: => Future[T]): Future[T] = {
    if (killed) {
      logger.debug("cannot send request when the connection is killed")
      Future.failed(Exceptions.ClosedException)
    } else f
  }

  private[api] def sendExpectingResponse(checkedWriteRequest: CheckedWriteRequest)(implicit ec: ExecutionContext): Future[Response] = whenActive {
    val expectingResponse =
      CheckedWriteRequestExpectingResponse(checkedWriteRequest)

    mongosystem ! expectingResponse
    expectingResponse.future
  }

  private[api] def sendExpectingResponse(requestMaker: RequestMaker, isMongo26WriteOp: Boolean)(implicit ec: ExecutionContext): Future[Response] = whenActive {
    lazy val expectingResponse =
      RequestMakerExpectingResponse(requestMaker, isMongo26WriteOp)

    mongosystem ! expectingResponse
    expectingResponse.future
  }

  /** Authenticates the connection on the given database. */
  def authenticate(db: String, user: String, password: String): Future[SuccessfulAuthentication] = whenActive {
    val req = AuthRequest(Authenticate(db, user, password))
    mongosystem ! req
    req.future
  }

  /**
   * Closes this MongoConnection (closes all the channels and ends the actors).
   */
  def askClose()(implicit timeout: FiniteDuration): Future[_] =
    whenActive { ask(monitor, Close)(Timeout(timeout)) }

  /**
   * Closes this MongoConnection
   * (closes all the channels and ends the actors)
   */
  def close(): Unit = monitor ! Close

  @volatile private[api] var killed: Boolean = false

  private case class IsAvailable(result: Promise[Boolean]) {
    override val toString = "IsAvailable?"
  }
  private case class IsPrimaryAvailable(result: Promise[Boolean]) {
    override val toString = "IsPrimaryAvailable?"
  }

  private def isAvailable: Future[Boolean] = {
    if (killed) Future.successful(false)
    else {
      val p = Promise[Boolean]()
      val check = {
        if (options.readPreference.slaveOk) IsAvailable(p)
        else IsPrimaryAvailable(p)
      }

      monitor ! check
      p.future
    }
  }

  private[api] val monitor = actorSystem.actorOf(
    Props(new MonitorActor), "Monitor-" + MongoDriver.nextCounter)

  @volatile private[api] var metadata: Option[ProtocolMetadata] = None

  private class MonitorActor extends Actor {
    import scala.collection.mutable.Queue

    mongosystem ! RegisterMonitor

    private val waitingForPrimary = Queue[ActorRef]()
    private var primaryAvailable = false

    private val waitingForClose = Queue[ActorRef]()

    private var setAvailable = false

    override val receive: Receive = {
      case pa @ PrimaryAvailable(metadata) => {
        logger.debug("set: a primary is available")
        primaryAvailable = true
        waitingForPrimary.dequeueAll(_ => true).foreach(_ ! pa)
      }

      case PrimaryUnavailable =>
        logger.debug("set: no primary available")
        primaryAvailable = false

      case SetAvailable(meta) => {
        logger.debug(s"set: a node is available: $meta")
        setAvailable = true
        metadata = Some(meta)
      }

      case SetUnavailable =>
        setAvailable = false
        logger.debug("set: no node seems to be available")

      /* TODO: Remove 
      case WaitForPrimary => {
        if (killed) {
          sender ! Failure(ConnectionException(
            "MongoDBSystem actor shutting down or no longer active"))

        } else if (primaryAvailable && metadata.isDefined) {
          logger.debug(s"$sender is waiting for a primary... available right now, go!")
          sender ! PrimaryAvailable(metadata.get)
        } else {
          logger.debug(s"$sender is waiting for a primary...  not available, warning as soon a primary is available.")
          waitingForPrimary += sender
        }
      }
         */

      case Close => {
        logger.debug("Monitor received Close")

        killed = true
        primaryAvailable = false
        setAvailable = false

        mongosystem ! Close
        waitingForClose += sender
        waitingForPrimary.dequeueAll(_ => true).foreach(
          _ ! Failure(new scala.RuntimeException(
            "MongoDBSystem actor shutting down or no longer active")))
      }

      case Closed => {
        logger.debug(s"Monitor $self closed, stopping...")
        waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
        context.stop(self)
      }

      case IsAvailable(result)        => result success setAvailable
      case IsPrimaryAvailable(result) => result success primaryAvailable
    }

    override def postStop = logger.debug(s"Monitor $self stopped.")
  }

  object MonitorActor {
    private val logger = LazyLogger("reactivemongo.core.actors.MonitorActor")
  }
}

object MongoConnection {
  val DefaultHost = "localhost"
  val DefaultPort = 27017

  final class URIParsingException(message: String)
      extends Exception with NoStackTrace {
    override def getMessage() = message
  }

  /**
   * @param hosts the hosts of the servers of the MongoDB replica set
   * @param options the connection options
   * @param ignoredOptions the options ignored from the parsed URI
   * @param db the name of the database
   * @param authenticate the authenticate information (see [[MongoConnectionOptions.authMode]])
   */
  final case class ParsedURI(
    hosts: List[(String, Int)],
    options: MongoConnectionOptions,
    ignoredOptions: List[String],
    db: Option[String],
    authenticate: Option[Authenticate])

  /**
   * Parses a MongoURI.
   *
   * @param uri the connection URI (see [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information)
   */
  def parseURI(uri: String): Try[ParsedURI] = {
    val prefix = "mongodb://"

    Try {
      val useful = uri.replace(prefix, "")
      def opts = makeOptions(parseOptions(useful))

      if (useful.indexOf("@") == -1) {
        val (db, hosts) = parseHostsAndDbName(useful)
        val (unsupportedKeys, options) = opts
        ParsedURI(hosts, options, unsupportedKeys, db, None)
      } else {
        val WithAuth = """([^:]+):([^@]*)@(.+)""".r

        useful match {
          case WithAuth(user, pass, hostsPortsAndDbName) => {
            val (db, hosts) = parseHostsAndDbName(hostsPortsAndDbName)

            db.fold[ParsedURI](throw new URIParsingException(s"Could not parse URI '$uri': authentication information found but no database name in URI")) { database =>
              val (unsupportedKeys, options) = opts

              ParsedURI(hosts, options, unsupportedKeys, Some(database), Some(Authenticate.apply(options.authSource.getOrElse(database), user, pass)))
            }
          }

          case _ => throw new URIParsingException(s"Could not parse URI '$uri'")
        }
      }
    }
  }

  private def parseHosts(hosts: String) = hosts.split(",").toList.map { host =>
    host.split(':').toList match {
      case host :: port :: Nil => host -> {
        try {
          val p = port.toInt
          if (p > 0 && p < 65536) p
          else throw new URIParsingException(s"Could not parse hosts '$hosts' from URI: invalid port '$port'")
        } catch {
          case _: NumberFormatException => throw new URIParsingException(s"Could not parse hosts '$hosts' from URI: invalid port '$port'")
          case NonFatal(e)              => throw e
        }
      }
      case host :: Nil => host -> DefaultPort
      case _           => throw new URIParsingException(s"Could not parse hosts from URI: invalid definition '$hosts'")
    }
  }

  private def parseHostsAndDbName(hostsPortAndDbName: String): (Option[String], List[(String, Int)]) = hostsPortAndDbName.split("/").toList match {
    case hosts :: Nil           => None -> parseHosts(hosts.takeWhile(_ != '?'))
    case hosts :: dbName :: Nil => Some(dbName.takeWhile(_ != '?')) -> parseHosts(hosts)
    case _ =>
      throw new URIParsingException(s"Could not parse hosts and database from URI: '$hostsPortAndDbName'")
  }

  private def parseOptions(uriAndOptions: String): Map[String, String] =
    uriAndOptions.split('?').toList match {
      case uri :: options :: Nil => options.split("&").map { option =>
        option.split("=").toList match {
          case key :: value :: Nil => (key -> value)
          case _                   => throw new URIParsingException(s"Could not parse URI '$uri': invalid options '$options'")
        }
      }.toMap
      case _ => Map.empty
    }

  val IntRe = "^([0-9]+)$".r
  val FailoverRe = "^([^:]+):([0-9]+)x([0-9.]+)$".r

  private def makeOptions(opts: Map[String, String]): (List[String], MongoConnectionOptions) = {
    val (remOpts, step1) = opts.iterator.foldLeft(
      Map.empty[String, String] -> MongoConnectionOptions()) {
        case ((unsupported, result), kv) => kv match {
          case ("authSource", v)           => unsupported -> result.copy(authSource = Some(v))

          case ("authMode", "scram-sha1")  => unsupported -> result.copy(authMode = ScramSha1Authentication)
          case ("authMode", _)             => unsupported -> result.copy(authMode = CrAuthentication)

          case ("connectTimeoutMS", v)     => unsupported -> result.copy(connectTimeoutMS = v.toInt)
          case ("socketTimeoutMS", v)      => unsupported -> result.copy(socketTimeoutMS = v.toInt)
          case ("sslEnabled", v)           => unsupported -> result.copy(sslEnabled = v.toBoolean)
          case ("sslAllowsInvalidCert", v) => unsupported -> result.copy(sslAllowsInvalidCert = v.toBoolean)

          case ("rm.tcpNoDelay", v)        => unsupported -> result.copy(tcpNoDelay = v.toBoolean)
          case ("rm.keepAlive", v)         => unsupported -> result.copy(keepAlive = v.toBoolean)
          case ("rm.nbChannelsPerNode", v) => unsupported -> result.copy(nbChannelsPerNode = v.toInt)

          case ("writeConcern", "unacknowledged") => unsupported -> result.
            copy(writeConcern = WriteConcern.Unacknowledged)

          case ("writeConcern", "acknowledged") => unsupported -> result.
            copy(writeConcern = WriteConcern.Acknowledged)

          case ("writeConcern", "journaled") => unsupported -> result.
            copy(writeConcern = WriteConcern.Journaled)

          case ("writeConcern", "default") => unsupported -> result.
            copy(writeConcern = WriteConcern.Default)

          case ("readPreference", "primary") => unsupported -> result.
            copy(readPreference = ReadPreference.primary)

          case ("readPreference", "primaryPreferred") =>
            unsupported -> result.copy(
              readPreference = ReadPreference.primaryPreferred)

          case ("readPreference", "secondary") => unsupported -> result.copy(
            readPreference = ReadPreference.secondary)

          case ("readPreference", "secondaryPreferred") =>
            unsupported -> result.copy(
              readPreference = ReadPreference.secondaryPreferred)

          case ("readPreference", "nearest") => unsupported -> result.copy(
            readPreference = ReadPreference.nearest)

          case ("rm.failover", "default") => unsupported -> result
          case ("rm.failover", "remote") => unsupported -> result.copy(
            failoverStrategy = FailoverStrategy.remote)

          case ("rm.failover", opt @ FailoverRe(d, r, f)) => (for {
            (time, unit) <- Try(Duration(d)).toOption.flatMap(Duration.unapply)
            delay <- Some(FiniteDuration(time, unit))
            retry <- Try(r.toInt).toOption
            factor <- Try(f.toDouble).toOption
          } yield FailoverStrategy(delay, retry, _ * factor)) match {
            case Some(strategy) =>
              unsupported -> result.copy(failoverStrategy = strategy)

            case _ => (unsupported + ("rm.failover" -> opt)) -> result
          }

          case ("rm.monitorRefreshMS", opt @ IntRe(ms)) =>
            Try(ms.toInt).filter(_ >= 100 /* ms */ ).toOption match {
              case Some(interval) => unsupported -> result.copy(
                monitorRefreshMS = interval)

              case _ => (unsupported + ("rm.monitorRefreshMS" -> opt)) -> result
            }

          case kv => (unsupported + kv) -> result
        }
      }

    // Overriding options
    remOpts.iterator.foldLeft(List.empty[String] -> step1) {
      case ((unsupported, result), kv) => kv match {
        case ("writeConcernW", "majority") => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(w = WriteConcern.Majority))

        case ("writeConcernW", IntRe(str)) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(w = WriteConcern.WaitForAknowledgments(str.toInt)))

        case ("writeConcernW", tag) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(w = WriteConcern.TagSet(tag)))

        case ("writeConcernJ", journaled) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(j = journaled.toBoolean))

        case ("writeConcernTimeout", t @ IntRe(ms)) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(wtimeout = Some(ms.toInt)))

        case (k, _) => (k :: unsupported) -> result
      }
    }
  }
}

/**
 * @param config a custom configuration (otherwise the default options are used)
 */
class MongoDriver(config: Option[Config] = None) {
  import scala.collection.mutable.{ Map => MutableMap }

  import MongoDriver.logger

  /* MongoDriver always uses its own ActorSystem so it can have complete control separate from other
   * Actor Systems in the application
   */
  val system = {
    import com.typesafe.config.ConfigFactory
    val reference = config getOrElse ConfigFactory.load()

    if (!reference.hasPath("mongo-async-driver")) {
      ActorSystem("reactivemongo")
    } else ActorSystem("reactivemongo",
      reference.getConfig("mongo-async-driver"))
  }

  private val supervisorActor = system.actorOf(Props(new SupervisorActor(this)), s"Supervisor-${MongoDriver.nextCounter}")

  private val connectionMonitors = MutableMap.empty[ActorRef, MongoConnection]

  /** Keep a list of all connections so that we can terminate the actors */
  def connections: Iterable[MongoConnection] = connectionMonitors.values

  def numConnections: Int = connectionMonitors.size

  /**
   * Closes this driver (and all its connections and resources).
   * Awaits the termination until the timeout is expired.
   */
  def close(timeout: FiniteDuration = FiniteDuration(2, SECONDS)): Unit = {
    // Terminate actors used by MongoConnections
    connections.foreach(_.close())

    // Tell the supervisor to close.
    // It will shut down all the connections and monitors
    // and then shut down the ActorSystem as it is exiting.
    supervisorActor ! Close

    if (!system.isTerminated) {
      // When the actorSystem is shutdown,
      // it means that supervisorActor has exited (run its postStop).
      // So, wait for that event.

      try {
        system.awaitTermination(timeout)
      } catch {
        case e: Throwable if !system.isTerminated => throw e
      }
    }
  }

  /**
   * Creates a new MongoConnection.
   *
   * See [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information.
   *
   * @param nodes A list of node names, like ''node1.foo.com:27017''. Port is optional, it is 27017 by default.
   * @param authentications A list of Authenticates.
   * @param nbChannelsPerNode Number of channels to open per node. Defaults to 10.
   * @param name The name of the newly created [[reactivemongo.core.actors.MongoDBSystem]] actor, if needed.
   * @param options Options for the new connection pool.
   */
  @deprecated(message = "Must use `connection` with `nbChannelsPerNode` set in the `options`.", since = "0.11.3")
  def connection(nodes: Seq[String], options: MongoConnectionOptions, authentications: Seq[Authenticate], nbChannelsPerNode: Int, name: Option[String]): MongoConnection = connection(nodes, options, authentications, name)

  /**
   * Creates a new MongoConnection.
   *
   * See [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information.
   *
   * @param nodes A list of node names, like ''node1.foo.com:27017''. Port is optional, it is 27017 by default.
   * @param authentications A list of Authenticates.
   * @param name The name of the newly created [[reactivemongo.core.actors.MongoDBSystem]] actor, if needed.
   * @param options Options for the new connection pool.
   */
  def connection(nodes: Seq[String], options: MongoConnectionOptions = MongoConnectionOptions(), authentications: Seq[Authenticate] = Seq.empty, name: Option[String] = None): MongoConnection = {
    def dbsystem: MongoDBSystem = options.authMode match {
      case ScramSha1Authentication =>
        new StandardDBSystem(nodes, authentications, options)()

      case _ =>
        new LegacyDBSystem(nodes, authentications, options)()
    }

    val props = Props(dbsystem)
    val mongosystem = name match {
      case Some(nm) => system.actorOf(props, nm);
      case None =>
        system.actorOf(props, s"Connection-${+MongoDriver.nextCounter}")
    }
    val connection = (supervisorActor ? AddConnection(options, mongosystem))(Timeout(10, SECONDS))
    Await.result(connection.mapTo[MongoConnection], Duration.Inf)
    // TODO: Returns Future[MongoConnection]
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * See [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information.
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param nbChannelsPerNode Number of channels to open per node.
   * @param name The name of the newly created [[reactivemongo.core.actors.MongoDBSystem]] actor, if needed.
   */
  @deprecated(message = "Must you reactivemongo.api.MongoDriver.connection(reactivemongo.api.MongoConnection.ParsedURI,Option[String]):reactivemongo.api.MongoConnection connection(..)]] with `nbChannelsPerNode` set in the `parsedURI`.", since = "0.11.3")
  def connection(parsedURI: MongoConnection.ParsedURI, nbChannelsPerNode: Int, name: Option[String]): MongoConnection = connection(parsedURI, name)

  /**
   * Creates a new MongoConnection from URI.
   *
   * See [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information.
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param name The name of the newly created [[reactivemongo.core.actors.MongoDBSystem]] actor, if needed.
   */
  def connection(parsedURI: MongoConnection.ParsedURI, name: Option[String]): MongoConnection = {
    if (!parsedURI.ignoredOptions.isEmpty)
      logger.warn(s"Some options were ignored because they are not supported (yet): ${parsedURI.ignoredOptions.mkString(", ")}")
    connection(parsedURI.hosts.map(h => h._1 + ':' + h._2), parsedURI.options, parsedURI.authenticate.toSeq, name)
  }

  /**
   * Creates a new MongoConnection from URI.
   *
   * See [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information.
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   * @param nbChannelsPerNode Number of channels to open per node.
   */
  @deprecated(message = "Must you `connection` with `nbChannelsPerNode` set in the options of the `parsedURI`.", since = "0.11.3")
  def connection(parsedURI: MongoConnection.ParsedURI, nbChannelsPerNode: Int): MongoConnection = connection(parsedURI)

  /**
   * Creates a new MongoConnection from URI.
   *
   * See [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information.
   *
   * @param parsedURI The URI parsed by [[reactivemongo.api.MongoConnection.parseURI]]
   */
  def connection(parsedURI: MongoConnection.ParsedURI): MongoConnection =
    connection(parsedURI, None)

  private case class AddConnection(options: MongoConnectionOptions, mongosystem: ActorRef)

  //private case class CloseWithTimeout(timeout: FiniteDuration)

  private case class SupervisorActor(driver: MongoDriver) extends Actor {
    def isEmpty = driver.connectionMonitors.isEmpty

    override def receive = {
      case AddConnection(opts, sys) =>
        val connection = new MongoConnection(driver.system, sys, opts)
        driver.connectionMonitors.put(connection.monitor, connection)
        context.watch(connection.monitor)
        sender ! connection

      case Terminated(actor) => driver.connectionMonitors.remove(actor)

      /*
      case CloseWithTimeout(timeout) =>
        if (isEmpty) context.stop(self)
        else context.become(closing(timeout))
         */

      case Close =>
        if (isEmpty) context.stop(self)
        else context.become(closing(Duration.Zero))
    }

    def closing(shutdownTimeout: FiniteDuration): Receive = {
      case AddConnection(_, _) =>
        logger.warn("Refusing to add connection while MongoDriver is closing.")

      case Terminated(actor) => {
        driver.connectionMonitors.remove(actor)
        if (isEmpty) context.stop(self)
      }

      /*
      case CloseWithTimeout(timeout) =>
        logger.warn("CloseWithTimeout ignored, already closing.")
         */

      case Close => logger.warn("Close ignored, already closing.")
    }

    override def postStop: Unit = driver.system.shutdown()
  }
}

object MongoDriver {
  private val logger = LazyLogger("reactivemongo.api.MongoDriver")

  /** Creates a new [[MongoDriver]] with a new ActorSystem. */
  def apply(): MongoDriver = new MongoDriver

  /** Creates a new [[MongoDriver]] with the given `config`. */
  def apply(config: Config): MongoDriver = new MongoDriver(Some(config))

  private[api] val _counter = new AtomicLong(0)
  private[api] def nextCounter: Long = _counter.incrementAndGet()
}
