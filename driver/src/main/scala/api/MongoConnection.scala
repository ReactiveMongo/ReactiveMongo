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

//import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.util.Try
import scala.util.control.{ NonFatal, NoStackTrace }

import scala.concurrent.{
  Await,
  ExecutionContext,
  Future,
  Promise
}
import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.pattern.ask //{ after, ask }

import reactivemongo.core.actors.{
  AuthRequest,
  CheckedWriteRequestExpectingResponse,
  Close,
  Closed,
  Exceptions,
  PrimaryAvailable,
  PrimaryUnavailable,
  RegisterMonitor,
  RequestMakerExpectingResponse,
  SetAvailable,
  SetUnavailable
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
  val options: MongoConnectionOptions) {
  import Exceptions._

  @deprecated("Create with an explicit supervisor and connection names", "0.11.14")
  def this(actorSys: ActorSystem, mongoSys: ActorRef, opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode mongoSys}", s"unknown-${System identityHashCode mongoSys}", actorSys, mongoSys, opts)

  import MongoConnection.logger

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
    "reactivemongo.api.MongoConnection", "database",
    "MongoConnection.scala", -1)

  /** Returns a future that will be successful when node set is available. */
  private[api] def waitIsAvailable(failoverStrategy: FailoverStrategy)(implicit ec: ExecutionContext): Future[Unit] = {
    logger.debug(s"[$lnm] Waiting is available...")

    val timeoutFactor = 1.25D // TODO: Review
    val timeout: FiniteDuration = (1 to failoverStrategy.retries).
      foldLeft(failoverStrategy.initialDelay) { (d, i) =>
        d + (failoverStrategy.initialDelay * (
          (timeoutFactor * failoverStrategy.delayFactor(i)).toLong))
      }

    probe(timeout).recoverWith {
      case error: Throwable => {
        error.setStackTrace(databaseSTE +: error.getStackTrace)
        Future.failed[ProtocolMetadata](error)
      }
    }.flatMap {
      case ProtocolMetadata(
        _, MongoWireVersion.V24AndBefore, _, _, _) =>
        Future.failed[Unit](ConnectionException(
          s"unsupported MongoDB version < 2.6 ($lnm)"))

      case _ => Future successful {}
    }

    /*
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
        s"[$lnm] Wait is available: $attempt @ ${System.currentTimeMillis}")

      if (attempt == 0) {
        Future.failed(finalErr(lastErr))
      } else {
        @inline def res: Either[Retry, Unit] = try {
          val before = System.currentTimeMillis
          val unavail = Await.result(probe, timeout)
          val duration = System.currentTimeMillis - before

          unavail match {
            case Some(reason) => Left(FiniteDuration(
              timeout.toMillis - duration, MILLISECONDS) -> reason)

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
              s"unsupported MongoDB version < 2.6 ($lnm)"))

          case Some(_) => Future successful {}
          case _ => Future.failed[Unit](ConnectionException(
            s"protocol metadata not available ($lnm)"))
        }
      }
     */
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

  private[api] def sendExpectingResponse(checkedWriteRequest: CheckedWriteRequest): Future[Response] = whenActive {
    val expectingResponse =
      CheckedWriteRequestExpectingResponse(checkedWriteRequest)

    mongosystem ! expectingResponse
    expectingResponse.future
  }

  private[api] def sendExpectingResponse(requestMaker: RequestMaker, isMongo26WriteOp: Boolean): Future[Response] = whenActive {
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

  private case class IsAvailable(result: Promise[ProtocolMetadata]) {
    override val toString = "IsAvailable?"
  }
  private case class IsPrimaryAvailable(result: Promise[ProtocolMetadata]) {
    override val toString = "IsPrimaryAvailable?"
  }

  /**
   * Checks whether is unavailable.
   *
   * @return Future(None) if available
   */
  private[api] def probe(timeout: FiniteDuration): Future[ProtocolMetadata] =
    whenActive {
      val p = Promise[ProtocolMetadata]()
      val check = {
        if (options.readPreference.slaveOk) IsAvailable(p)
        else IsPrimaryAvailable(p)
      }

      monitor ! check

      import actorSystem.dispatcher

      def unavailResult = Future.failed[ProtocolMetadata] {
        if (options.readPreference.slaveOk) {
          new NodeSetNotReachable(supervisor, name, history())
        } else {
          new PrimaryUnavailableException(supervisor, name, history())
        }
      }

      def await() = Await.ready(p.future, timeout).recoverWith {
        case cause: Exception =>
          logger.warn(s"[$lnm] Fails to probe connection monitor", cause)
          unavailResult
      }

      try {
        await()
      } catch {
        case _: java.util.concurrent.TimeoutException =>
          logger.warn(s"[$lnm] Timeout while probing the connection monitor")
          unavailResult

        case cause: Exception => {
          logger.warn(s"[$lnm] Unexpected exception while probing connection monitor", cause)

          unavailResult
        }
      }
    }

  private[api] val monitor = actorSystem.actorOf(
    Props(new MonitorActor), s"Monitor-$name")

  // TODO: Remove (use probe)
  @volatile private[api] var metadata = Option.empty[ProtocolMetadata]

  private class MonitorActor extends Actor {
    import scala.collection.mutable.Queue

    mongosystem ! RegisterMonitor

    private var setAvailable = Promise[ProtocolMetadata]()
    private var primaryAvailable = Promise[ProtocolMetadata]()

    private val waitingForClose = Queue[ActorRef]()

    val receive: Receive = {
      case PrimaryAvailable(meta) => {
        logger.debug(s"[$lnm] A primary is available: $meta")

        metadata = Some(meta)

        if (!primaryAvailable.trySuccess(meta)) {
          primaryAvailable = Promise.successful(meta)
        }
      }

      case PrimaryUnavailable => {
        logger.debug(s"[$lnm] There is no primary available")

        if (primaryAvailable.isCompleted) {
          primaryAvailable = Promise[ProtocolMetadata]()
        }
      }

      case SetAvailable(meta) => {
        logger.debug(s"[$lnm] A node is available: $meta")

        metadata = Some(meta)

        if (!setAvailable.trySuccess(meta)) {
          setAvailable = Promise.successful(meta)
        }
      }

      case SetUnavailable => {
        logger.debug(s"[$lnm] No node seems to be available")

        if (setAvailable.isCompleted) {
          setAvailable = Promise[ProtocolMetadata]()
        }
      }

      case IsAvailable(result) => {
        result.completeWith(setAvailable.future)
        ()
      }

      case IsPrimaryAvailable(result) => {
        result.completeWith(primaryAvailable.future)
        ()
      }

      case Close => {
        logger.debug(s"[$lnm] Monitor received Close")

        killed = true
        primaryAvailable = Promise.failed[ProtocolMetadata](
          new Exception(s"[$lnm] Closing connection..."))

        setAvailable = Promise.failed[ProtocolMetadata](
          new Exception(s"[$lnm] Closing connection..."))

        mongosystem ! Close
        waitingForClose += sender

        ()
      }

      case Closed => {
        logger.debug(s"[$lnm] Monitor ${self.path} is now closed")

        waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
        context.stop(self)
        ()
      }
    }

    override def postStop = logger.debug(s"Monitor $self stopped ($lnm)")
  }
}

object MongoConnection {
  val DefaultHost = "localhost"
  val DefaultPort = 27017

  private[api] val logger = LazyLogger("reactivemongo.api.MongoConnection")

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

              ParsedURI(hosts, options, unsupportedKeys, Some(database),
                Some(Authenticate(options.authenticationDatabase.
                  getOrElse(database), user, pass)))
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
        s"Could not parse hosts and database from URI: '$hostsPortAndDbName'")
  }

  private def parseOptions(uriAndOptions: String): Map[String, String] =
    uriAndOptions.split('?').toList match {
      case uri :: options :: Nil => options.split("&").map { option =>
        option.split("=").toList match {
          case key :: value :: Nil => (key -> value)
          case _ => throw new URIParsingException(
            s"Could not parse URI '$uri': invalid options '$options'")
        }
      }(scala.collection.breakOut)

      case _ => Map.empty
    }

  val IntRe = "^([0-9]+)$".r
  val FailoverRe = "^([^:]+):([0-9]+)x([0-9.]+)$".r

  private def makeOptions(opts: Map[String, String]): (List[String], MongoConnectionOptions) = {
    val (remOpts, step1) = opts.iterator.foldLeft(
      Map.empty[String, String] -> MongoConnectionOptions()) {
        case ((unsupported, result), kv) => kv match {
          case ("authSource", v) => {
            logger.warn(s"Connection option 'authSource' deprecated: use option 'authenticationDatabase'")

            unsupported -> result.copy(authenticationDatabase = Some(v))
          }

          case ("authenticationDatabase", v) =>
            unsupported -> result.copy(authenticationDatabase = Some(v))

          case ("authMode", "mongocr") => unsupported -> result.
            copy(authMode = CrAuthentication)

          case ("authMode", _) => unsupported -> result.
            copy(authMode = ScramSha1Authentication)

          case ("connectTimeoutMS", v) => unsupported -> result.
            copy(connectTimeoutMS = v.toInt)

          case ("maxIdleTimeMS", v) => unsupported -> result.
            copy(maxIdleTimeMS = v.toInt)

          case ("sslEnabled", v) => {
            logger.warn(
              s"Connection option 'sslEnabled' deprecated: use option 'ssl'")
            unsupported -> result.copy(sslEnabled = v.toBoolean)
          }

          case ("ssl", v) =>
            unsupported -> result.copy(sslEnabled = v.toBoolean)

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

          case ("rm.failover", "strict") => unsupported -> result.copy(
            failoverStrategy = FailoverStrategy.strict)

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
            copy(w = WriteConcern.WaitForAcknowledgments(str.toInt)))

        case ("writeConcernW", tag) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(w = WriteConcern.TagSet(tag)))

        case ("writeConcernJ", journaled) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(j = journaled.toBoolean))

        case ("writeConcernTimeout", IntRe(ms)) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(wtimeout = Some(ms.toInt)))

        case (k, _) => (k :: unsupported) -> result
      }
    }
  }
}
