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

import java.util.concurrent.{
  TimeUnit,
  atomic
}, TimeUnit.MILLISECONDS, atomic.AtomicLong

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

  /**
   * A future that is completed with a response,
   * after 1 or more attempts (specified in the given strategy).
   */
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

      case Failure(e) => {
        logger.trace(
          "Got an non retryable error, completing with a failure...", e
        )
        promise.failure(e)
      }

      case Success(response) => {
        logger.trace("Got a successful result, completing...")
        promise.success(response)
      }
    }
  }

  private def isRetryable(throwable: Throwable) = throwable match {
    case e: ChannelNotFound             => e.retriable
    case _: PrimaryUnavailableException => true
    case _: NodeSetNotReachable         => true
    case _: ConnectionException         => true
    case _: ConnectionNotInitialized    => true
    case e: DatabaseException =>
      e.isNotAPrimaryError || e.isUnauthorized

    case _ => false
  }

  send(0)
}

@deprecated(message = "Will be made private", since = "0.11.11")
class Failover2[A](producer: () => Future[A], connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext) {
  import Failover2.logger, logger.trace
  import reactivemongo.core.errors._
  import reactivemongo.core.actors.Exceptions._

  // TODO: Pass an explicit stack trace, to be able to raise with possible err

  private val lnm = s"${connection.supervisor}/${connection.name}" // log name

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

          trace(s"[$lnm] Got an error, retrying... (try #${`try`} is scheduled in ${delay.toMillis} ms)", e)

          after(delay, connection.actorSystem.scheduler)(send(`try`))
        } else {
          // generally that means that the primary is not available
          // or the nodeset is unreachable
          logger.error(s"[$lnm] Got an error, no more attempts to do. Completing with a failure... ", e)

          Future.failed(e)
        }
      }

      case Failure(e) => {
        trace(s"[$lnm] Got an non retryable error, completing with a failure... ", e)
        Future.failed(e)
      }

      case Success(response) => {
        trace(s"[$lnm] Got a successful result, completing...")
        Future.successful(response)
      }
    }

  private def isRetryable(throwable: Throwable) = throwable match {
    case e: ChannelNotFound             => e.retriable
    case _: PrimaryUnavailableException => true
    case _: NodeSetNotReachable         => true
    case _: ConnectionException         => true
    case _: ConnectionNotInitialized    => true
    case e: DatabaseException =>
      e.isNotAPrimaryError || e.isUnauthorized

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
 * A pool of MongoDB connections, obtained from a [[reactivemongo.api.MongoDriver]].
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
 * @param supervisor the name of the supervisor
 * @param name the unique name for the connection pool
 * @param mongosystem the reference to the internal [[reactivemongo.core.actors.MongoDBSystem]] Actor.
 *
 * @define dbName the database name
 * @define failoverStrategy the failover strategy for sending requests
 */
class MongoConnection(
    val supervisor: String,
    val name: String,
    val actorSystem: ActorSystem,
    val mongosystem: ActorRef,
    val options: MongoConnectionOptions
) {
  import Exceptions._

  @deprecated("Create with an explicit supervisor and connection names", "0.11.14")
  def this(actorSys: ActorSystem, mongoSys: ActorRef, opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode mongoSys}", s"unknown-${System identityHashCode mongoSys}", actorSys, mongoSys, opts)

  private[api] val logger = LazyLogger("reactivemongo.api.MongoConnection")

  private val lnm = s"$supervisor/$name" // log name

  // TODO: Review
  private[api] var history = () => InternalState.empty

  @volatile private[api] var killed: Boolean = false

  /**
   * Returns a DefaultDB reference using this connection.
   *
   * @param name $dbName
   * @param failoverStrategy $failoverStrategy
   */
  @deprecated(message = "Use [[database]]", since = "0.11.8")
  def apply(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit context: ExecutionContext): DefaultDB = DefaultDB(name, this, failoverStrategy)

  /**
   * Returns a DefaultDB reference using this connection
   * (alias for the `apply` method).
   *
   * @param name $dbName
   * @param failoverStrategy $failoverStrategy
   */
  @deprecated(message = "Must use [[database]]", since = "0.11.8")
  def db(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit context: ExecutionContext): DefaultDB = apply(name, failoverStrategy)

  /**
   * Returns a DefaultDB reference using this connection.
   * The failover strategy is also used to wait for the node set to be ready,
   * before returning an available DB.
   *
   * @param name $dbName
   * @param failoverStrategy $failoverStrategy
   */
  def database(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit context: ExecutionContext): Future[DefaultDB] =
    waitIsAvailable(failoverStrategy).map(_ => apply(name, failoverStrategy))

  private val databaseSTE = new StackTraceElement(
    "reactivemongo.api.MongoConnection", "database", "api.scala", -1
  )

  /** Returns a future that will be successful when node set is available. */
  private[api] def waitIsAvailable(failoverStrategy: FailoverStrategy)(implicit ec: ExecutionContext): Future[Unit] = {
    logger.debug(s"[$lnm] Waiting is available...")

    @inline def nextTimeout(i: Int): FiniteDuration = {
      val delayFactor: Double = failoverStrategy.delayFactor(i)

      Duration.unapply(failoverStrategy.initialDelay * delayFactor).
        fold(failoverStrategy.initialDelay)(t => FiniteDuration(t._1, t._2))
    }

    type Retry = (FiniteDuration, Throwable)

    @inline def finalErr(lastErr: Throwable): Throwable = {
      val error = if (lastErr == null) {
        new NodeSetNotReachable(supervisor, name, history())
      } else lastErr

      error.setStackTrace(databaseSTE +: error.getStackTrace)

      error
    }

    def wait(iteration: Int, attempt: Int, timeout: FiniteDuration, lastErr: Throwable = null): Future[Unit] = {
      logger.trace(
        s"[$lnm] Wait is available: $attempt @ ${System.currentTimeMillis}"
      )

      if (attempt == 0) {
        Future.failed(finalErr(lastErr))
      } else {
        @inline def res: Either[Retry, Unit] = try {
          val before = System.currentTimeMillis
          val unavail = Await.result(probe, timeout)
          val duration = System.currentTimeMillis - before

          unavail match {
            case Some(reason) => Left(FiniteDuration(
              timeout.toMillis - duration, MILLISECONDS
            ) -> reason)

            case _ => Right({})
          }
        } catch {
          case e: Throwable => Left(timeout -> e)
        }

        @inline def doRetry(delay: FiniteDuration, reason: Throwable): Future[Unit] = after(delay, actorSystem.scheduler) {
          val nextIt = iteration + 1
          wait(nextIt, attempt - 1, nextTimeout(nextIt), reason)
        }

        res match {
          case Left((delay, error)) => {
            logger.trace(s"[$lnm] Got an error, retrying", error)
            // TODO: Keep an explicit stacktrace accross the retries
            // TODO: Transform error into a single StackTraceElement to add it

            doRetry(delay, error)
          }

          case _ => Future.successful({})
        }
      }
    }

    wait(0, 1 + failoverStrategy.retries, failoverStrategy.initialDelay).
      flatMap { _ =>
        metadata match {
          case Some(ProtocolMetadata(
            _, MongoWireVersion.V24AndBefore, _, _, _)) =>
            Future.failed[Unit](ConnectionException(
              s"unsupported MongoDB version < 2.6 ($lnm)"
            ))

          case Some(_) => Future successful {}
          case _ => Future.failed[Unit](ConnectionException(
            s"protocol metadata not available ($lnm)"
          ))
        }
      }
  }

  /** Returns true if the connection has not been killed. */
  def active: Boolean = !killed

  private def whenActive[T](f: => Future[T]): Future[T] = {
    if (killed) {
      logger.debug(s"[$lnm] Cannot send request when the connection is killed")
      Future.failed(new ClosedException(supervisor, name, history()))
    } else f
  }

  /**
   * Writes a request and drop the response if any.
   *
   * @param message The request maker.
   */
  private[api] def send(message: RequestMaker): Unit = {
    if (killed) throw new ClosedException(supervisor, name, history())
    else mongosystem ! message
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

  /**
   * Authenticates the connection on the given database.
   *
   * @param db $dbName
   * @param user the user name
   * @param password the user password
   */
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

  private case class IsAvailable(result: Promise[Boolean]) {
    override val toString = "IsAvailable?"
  }
  private case class IsPrimaryAvailable(result: Promise[Boolean]) {
    override val toString = "IsPrimaryAvailable?"
  }

  /**
   * Checks whether is unavailable.
   *
   * @return Future(None) if available
   */
  private[api] def probe: Future[Option[Exception]] = whenActive {
    val p = Promise[Boolean]()
    val check = {
      if (options.readPreference.slaveOk) IsAvailable(p)
      else IsPrimaryAvailable(p)
    }

    monitor ! check

    import actorSystem.dispatcher

    p.future.map {
      case true => Option.empty[Exception] // is available - no error

      case _ => {
        if (options.readPreference.slaveOk) {
          Some(new NodeSetNotReachable(supervisor, name, history()))
        } else {
          Some(new PrimaryUnavailableException(supervisor, name, history()))
        }
      }
    }
  }

  private[api] val monitor = actorSystem.actorOf(
    Props(new MonitorActor), s"Monitor-$name"
  )

  @volatile private[api] var metadata: Option[ProtocolMetadata] = None

  private class MonitorActor extends Actor {
    import scala.collection.mutable.Queue

    mongosystem ! RegisterMonitor

    //private val waitingForPrimary = Queue[ActorRef]()
    private var primaryAvailable = false

    private val waitingForClose = Queue[ActorRef]()

    private var setAvailable = false

    override val receive: Receive = {
      case pa @ PrimaryAvailable(meta) => {
        logger.debug(s"[$lnm] A primary is available: $meta")
        metadata = Some(meta)
        primaryAvailable = true
        //waitingForPrimary.dequeueAll(_ => true).foreach(_ ! pa)
      }

      case PrimaryUnavailable => {
        logger.debug(s"[$lnm] There is no primary available")
        primaryAvailable = false
      }

      case SetAvailable(meta) => {
        logger.debug(s"[$lnm] A node is available: $meta")
        metadata = Some(meta)
        setAvailable = true
      }

      case SetUnavailable =>
        setAvailable = false
        logger.debug(s"[$lnm] No node seems to be available")

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
        logger.debug(s"[$lnm] Monitor received Close")

        killed = true
        primaryAvailable = false
        setAvailable = false

        mongosystem ! Close
        waitingForClose += sender

        /*
        waitingForPrimary.dequeueAll(_ => true).foreach(
          _ ! Failure(new scala.RuntimeException(
            s"MongoDBSystem actor shutting down or no longer active ($lnm)")))
         */

        ()
      }

      case Closed => {
        logger.debug(s"[$lnm] Monitor ${self.path} is now closed")

        waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
        context.stop(self)
        ()
      }

      case IsAvailable(result)        => { result success setAvailable; () }
      case IsPrimaryAvailable(result) => { result success primaryAvailable; () }
    }

    override def postStop = logger.debug(s"Monitor $self stopped ($lnm)")
  }

  object MonitorActor {
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
    authenticate: Option[Authenticate]
  )
  // TODO: Type for URI with required DB name

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

      if (opts._2.maxIdleTimeMS != 0 &&
        opts._2.maxIdleTimeMS < opts._2.monitorRefreshMS) {

        throw new URIParsingException(s"Invalid URI options: maxIdleTimeMS(${opts._2.maxIdleTimeMS}) < monitorRefreshMS(${opts._2.monitorRefreshMS})")
      }

      // ---

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
      throw new URIParsingException(
        s"Could not parse hosts and database from URI: '$hostsPortAndDbName'"
      )
  }

  private def parseOptions(uriAndOptions: String): Map[String, String] =
    uriAndOptions.split('?').toList match {
      case uri :: options :: Nil => options.split("&").map { option =>
        option.split("=").toList match {
          case key :: value :: Nil => (key -> value)
          case _ => throw new URIParsingException(
            s"Could not parse URI '$uri': invalid options '$options'"
          )
        }
      }.toMap

      case _ => Map.empty
    }

  val IntRe = "^([0-9]+)$".r
  val FailoverRe = "^([^:]+):([0-9]+)x([0-9.]+)$".r

  private def makeOptions(opts: Map[String, String]): (List[String], MongoConnectionOptions) = {
    val (remOpts, step1) = opts.iterator.foldLeft(
      Map.empty[String, String] -> MongoConnectionOptions()
    ) {
        case ((unsupported, result), kv) => kv match {
          case ("authSource", v) => unsupported -> result.
            copy(authSource = Some(v))

          case ("authMode", "mongocr") => unsupported -> result.
            copy(authMode = CrAuthentication)

          case ("authMode", _) => unsupported -> result.
            copy(authMode = ScramSha1Authentication)

          case ("connectTimeoutMS", v) => unsupported -> result.
            copy(connectTimeoutMS = v.toInt)

          case ("maxIdleTimeMS", v) => unsupported -> result.
            copy(maxIdleTimeMS = v.toInt)

          case ("sslEnabled", v) => unsupported -> result.
            copy(sslEnabled = v.toBoolean)

          case ("sslAllowsInvalidCert", v) => unsupported -> result.
            copy(sslAllowsInvalidCert = v.toBoolean)

          case ("rm.tcpNoDelay", v) => unsupported -> result.
            copy(tcpNoDelay = v.toBoolean)

          case ("rm.keepAlive", v) => unsupported -> result.
            copy(keepAlive = v.toBoolean)

          case ("rm.nbChannelsPerNode", v) => unsupported -> result.
            copy(nbChannelsPerNode = v.toInt)

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
              readPreference = ReadPreference.primaryPreferred
            )

          case ("readPreference", "secondary") => unsupported -> result.copy(
            readPreference = ReadPreference.secondary
          )

          case ("readPreference", "secondaryPreferred") =>
            unsupported -> result.copy(
              readPreference = ReadPreference.secondaryPreferred
            )

          case ("readPreference", "nearest") => unsupported -> result.copy(
            readPreference = ReadPreference.nearest
          )

          case ("rm.failover", "default") => unsupported -> result
          case ("rm.failover", "remote") => unsupported -> result.copy(
            failoverStrategy = FailoverStrategy.remote
          )

          case ("rm.failover", "strict") => unsupported -> result.copy(
            failoverStrategy = FailoverStrategy.strict
          )

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
                monitorRefreshMS = interval
              )

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
    classLoader: Option[ClassLoader] = None
) {
  @deprecated("Use the constructor with the classloader", "0.12-RC6")
  def this(config: Option[Config]) = this(config, None)

  import scala.collection.mutable.{ Map => MutableMap }

  import MongoDriver.logger

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
  def close(timeout: FiniteDuration = FiniteDuration(2, SECONDS)): Unit = {
    logger.info(s"[$supervisorName] Closing instance of ReactiveMongo driver")

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
    val nm = name.getOrElse(s"Connection-${+MongoDriver.nextCounter}")

    // TODO: Passing ref to MongoDBSystem.history to AddConnection
    lazy val dbsystem: MongoDBSystem = options.authMode match {
      case CrAuthentication => new LegacyDBSystem(
        supervisorName, nm, nodes, authentications, options
      )

      case _ => new StandardDBSystem(
        supervisorName, nm, nodes, authentications, options
      )
    }

    val mongosystem = system.actorOf(Props(dbsystem), nm)

    def connection = (supervisorActor ? AddConnection(
      nm, nodes, options, mongosystem
    ))(Timeout(10, SECONDS))

    logger.info(s"[$supervisorName] Creating connection: $nm")

    import system.dispatcher

    Await.result(connection.mapTo[MongoConnection].map { c =>
      // TODO: Review
      c.history = () => dbsystem.internalState
      c
    }, Duration.Inf)
    // TODO: Returns Future[MongoConnection]
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
   * @param uriStrict $uriStrictParam
   * @param name $connectionNameParam
   */
  def connection(uriStrict: String, name: Option[String]): Try[MongoConnection] = connection(uriStrict, name, strictUri = true)

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
      Failure(new IllegalArgumentException(s"The connection URI contains unsupported options: ${parsedURI.ignoredOptions.mkString(", ")}"))
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

  private[reactivemongo] case class AddConnection(
    name: String,
    nodes: Seq[String],
    options: MongoConnectionOptions,
    mongosystem: ActorRef
  )

  //private case class CloseWithTimeout(timeout: FiniteDuration)

  private final class SupervisorActor(driver: MongoDriver) extends Actor {
    def isEmpty = driver.connectionMonitors.isEmpty

    override def receive = {
      case AddConnection(name, nodes, opts, sys) => {
        logger.debug(
          s"[$supervisorName] Add connection to the supervisor: $name"
        )

        val connection = new MongoConnection(
          supervisorName, name, driver.system, sys, opts
        )
        //connection.nodes = nodes

        driver.connectionMonitors.put(connection.monitor, connection)
        context.watch(connection.monitor)
        sender ! connection
      }

      case Terminated(actor) => {
        logger.debug(
          s"[$supervisorName] Pool actor is terminated: ${actor.path}"
        )

        driver.connectionMonitors.remove(actor)
        ()
      }

      /*
      case CloseWithTimeout(timeout) =>
        if (isEmpty) context.stop(self)
        else context.become(closing(timeout))
         */

      case Close => {
        logger.debug(s"[$supervisorName] Close the supervisor")

        if (isEmpty) context.stop(self)
        else context.become(closing(Duration.Zero))
      }
    }

    def closing(shutdownTimeout: FiniteDuration): Receive = {
      case AddConnection(name, _, _, _) =>
        logger.warn(s"[$supervisorName] Refusing to add connection while MongoDriver is closing: $name")

      case Terminated(actor) =>
        driver.connectionMonitors.remove(actor).foreach { con =>
          logger.debug(
            s"[$supervisorName] Connection is terminated: ${con.name}"
          )

          if (isEmpty) context.stop(self)
        }

      /*
      case CloseWithTimeout(timeout) =>
        logger.warn("CloseWithTimeout ignored, already closing.")
         */

      case Close =>
        logger.warn(s"[$supervisorName] Close ignored, already closing.")
    }

    override def postStop: Unit = {
      logger.info(s"[$supervisorName] Stopping the monitor")

      driver.system.shutdown()
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
