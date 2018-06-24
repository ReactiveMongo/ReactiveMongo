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

import scala.util.Try
import scala.util.control.{ NonFatal, NoStackTrace }

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.pattern.ask

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
  @deprecated("Will be private", "0.14.0") val actorSystem: ActorSystem,
  val mongosystem: ActorRef,
  val options: MongoConnectionOptions) { // TODO: toString as MongoURI
  import Exceptions._

  @deprecated("Create with an explicit supervisor and connection names", "0.11.14")
  def this(actorSys: ActorSystem, mongoSys: ActorRef, opts: MongoConnectionOptions) = this(s"unknown-${System identityHashCode mongoSys}", s"unknown-${System identityHashCode mongoSys}", actorSys, mongoSys, opts)

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
    waitIsAvailable(failoverStrategy, stackTrace()).
      map(_ => apply(name, failoverStrategy))

  @inline private def stackTrace() =
    Thread.currentThread.getStackTrace.tail.tail.take(2).reverse

  /** Returns a future that will be successful when node set is available. */
  private[api] def waitIsAvailable(
    failoverStrategy: FailoverStrategy,
    contextSTE: Array[StackTraceElement])(implicit ec: ExecutionContext): Future[Unit] = {
    debug("Waiting is available...")

    val timeoutFactor = 1.25D // TODO: Review
    val timeout: FiniteDuration = (1 to failoverStrategy.retries).
      foldLeft(failoverStrategy.initialDelay) { (d, i) =>
        d + (failoverStrategy.initialDelay * (
          (timeoutFactor * failoverStrategy.delayFactor(i)).toLong))
      }

    probe(timeout).recoverWith {
      case error: Throwable => {
        error.setStackTrace(contextSTE ++: error.getStackTrace)
        Future.failed[ProtocolMetadata](error)
      }
    }.flatMap {
      case ProtocolMetadata(
        _, MongoWireVersion.V24AndBefore, _, _, _) =>
        Future.failed[Unit](ConnectionException(
          s"unsupported MongoDB version < 2.6 ($lnm)"))

      case _ => Future successful {}
    }
  }

  /** Returns true if the connection has not been killed. */
  def active: Boolean = !killed

  private def whenActive[T](f: => Future[T]): Future[T] = {
    if (killed) {
      debug("Cannot send request when the connection is killed")
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

  @deprecated("Use `authenticate` with `failoverStrategy`", "0.14.0")
  def authenticate(db: String, user: String, password: String): Future[SuccessfulAuthentication] = authenticate(db, user, password, options.failoverStrategy)(actorSystem.dispatcher)

  /**
   * Authenticates the connection on the given database.
   *
   * @param db $dbName
   * @param user the user name
   * @param password the user password
   * @param failoverStrategy $failoverStrategy
   */
  def authenticate(
    db: String,
    user: String,
    password: String,
    failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit ec: ExecutionContext): Future[SuccessfulAuthentication] = waitIsAvailable(
    failoverStrategy, stackTrace()).flatMap { _ =>
    val req = AuthRequest(Authenticate(db, user, Option(password)))
    mongosystem ! req
    req.future
  }

  /**
   * Closes this MongoConnection (closes all the channels and ends the actors).
   */
  def askClose()(implicit timeout: FiniteDuration): Future[_] = whenActive {
    ask(monitor, Close("MongoConnection.askClose"))(Timeout(timeout))
  }

  /**
   * Closes this MongoConnection
   * (closes all the channels and ends the actors)
   */
  @deprecated("Use [[askClose]]", "0.13.0")
  def close(): Unit = monitor ! Close("MongoConnection.close")

  private case class IsAvailable(result: Promise[ProtocolMetadata]) {
    override val toString = s"IsAvailable#${System identityHashCode this}?"
  }
  private case class IsPrimaryAvailable(result: Promise[ProtocolMetadata]) {
    override val toString = s"IsPrimaryAvailable#${System identityHashCode this}?"
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

      import akka.pattern.after
      import actorSystem.dispatcher

      def unavailResult = Future.failed[ProtocolMetadata] {
        if (options.readPreference.slaveOk) {
          new NodeSetNotReachable(supervisor, name, history())
        } else {
          new PrimaryUnavailableException(supervisor, name, history())
        }
      }

      Future.firstCompletedOf(Seq(
        p.future.recoverWith {
          case cause: Exception =>
            warn(s"Fails to probe the connection monitor: $check", cause)

            unavailResult
        },
        after(timeout, actorSystem.scheduler)({
          if (p.future.isCompleted) {
            p.future // discard timeout as probing has completed
          } else {
            // TODO: Logging
            warn(s"Timeout after $timeout while probing the connection monitor: $check")
            unavailResult
          }
        })))
    }

  private[api] lazy val monitor = actorSystem.actorOf(
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
        debug(s"A primary is available: $meta")

        metadata = Some(meta)

        if (!primaryAvailable.trySuccess(meta)) {
          primaryAvailable = Promise.successful(meta)
        }
      }

      case PrimaryUnavailable => {
        debug("There is no primary available")

        if (primaryAvailable.isCompleted) {
          primaryAvailable = Promise[ProtocolMetadata]()
        }
      }

      case SetAvailable(meta) => {
        debug(s"A node is available: $meta")

        metadata = Some(meta)

        if (!setAvailable.trySuccess(meta)) {
          setAvailable = Promise.successful(meta)
        }
      }

      case SetUnavailable => {
        debug("No node seems to be available")

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

      case Close(src) => {
        debug(s"Monitor received Close request from $src")

        killed = true
        primaryAvailable = Promise.failed[ProtocolMetadata](
          new Exception(s"[$lnm] Closing connection..."))

        setAvailable = Promise.failed[ProtocolMetadata](
          new Exception(s"[$lnm] Closing connection..."))

        mongosystem ! Close("MonitorActor#Close")
        waitingForClose += sender

        ()
      }

      case Closed => {
        debug("Monitor is now closed")

        waitingForClose.dequeueAll(_ => true).foreach(_ ! Closed)
        context.stop(self)
        ()
      }
    }

    override def postStop = debug("Monitor is stopped")
  }

  // --- Logging ---

  import MongoConnection.logger

  private val lnm = s"$supervisor/$name" // log name

  //@inline private def _println(msg: => String) = println(s"[$lnm] $msg")

  @inline private def debug(msg: => String) = logger.debug(s"[$lnm] $msg")

  @inline private def warn(msg: => String) = logger.warn(s"[$lnm] $msg")

  @inline private def warn(msg: => String, cause: Exception) =
    logger.warn(s"[$lnm] $msg", cause)

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
   * @param hosts the host and port for the servers of the MongoDB replica set
   * @param options the connection options
   * @param ignoredOptions the options ignored from the parsed URI
   * @param db the name of the database
   * @param authenticate the authenticate information (see [[MongoConnectionOptions.authenticationMechanism]])
   */
  final case class ParsedURI(
    hosts: List[(String, Int)],
    options: MongoConnectionOptions,
    ignoredOptions: List[String],
    db: Option[String],
    @deprecated("Use `options.credentials`", "0.14.0") authenticate: Option[Authenticate])
  // TODO: Type for URI with required DB name

  /**
   * Parses a MongoURI.
   *
   * @param uri the connection URI (see [[http://docs.mongodb.org/manual/reference/connection-string/ the MongoDB URI documentation]] for more information)
   */
  def parseURI(uri: String): Try[ParsedURI] = {
    val seedList = uri.startsWith("mongodb+srv://")

    Try {
      val useful: String = {
        if (uri startsWith "mongodb://") uri.drop(10)
        else if (seedList) uri.drop(14)
        else throw new URIParsingException(s"Invalid scheme: $uri")
      }

      def opts = {
        val empty = MongoConnectionOptions()
        val initial = if (!seedList) empty else {
          empty.copy(sslEnabled = true)
        }

        makeOptions(parseOptions(useful), empty)
      }

      if (opts._2.maxIdleTimeMS != 0 &&
        opts._2.maxIdleTimeMS < opts._2.monitorRefreshMS) {

        throw new URIParsingException(s"Invalid URI options: maxIdleTimeMS(${opts._2.maxIdleTimeMS}) < monitorRefreshMS(${opts._2.monitorRefreshMS})")
      }

      // ---

      val (unsupportedKeys, options) = opts

      if (useful.indexOf("@") == -1) {
        val (db, hosts) = parseHostsAndDbName(useful)

        options.authenticationMechanism match {
          case X509Authentication => {
            val dbName = db.getOrElse(throw new URIParsingException(s"Could not parse URI '$uri': authentication information found but no database name in URI"))

            val optsWithX509 = options.copy(credentials = Map(
              dbName -> MongoConnectionOptions.Credential("", None)))

            ParsedURI(hosts, optsWithX509, unsupportedKeys, db,
              Some(Authenticate(dbName, "", None)))
          }

          case _ => ParsedURI(hosts, options, unsupportedKeys, db, None)
        }
      } else {
        val WithAuth = """([^:]+)(|:[^@]*)@(.+)""".r

        useful match {
          case WithAuth(user, p, hostsPortsAndDbName) => {
            val pass = p.stripPrefix(":")
            val (db, hosts) = parseHostsAndDbName(hostsPortsAndDbName)

            db.fold[ParsedURI](throw new URIParsingException(s"Could not parse URI '$uri': authentication information found but no database name in URI")) { database =>

              if (options.authenticationMechanism == X509Authentication && pass.nonEmpty) {
                throw new URIParsingException("You should not provide a password when authenticating with X509 authentication")
              }

              val password = {
                if (options.authenticationMechanism != X509Authentication) {
                  Option(pass)
                } else Option.empty[String]
              }

              val authDb = options.authenticationDatabase.getOrElse(database)
              val optsWithCred = options.copy(
                credentials = options.credentials + (
                  authDb -> MongoConnectionOptions.Credential(
                    user, password)))

              ParsedURI(hosts, optsWithCred, unsupportedKeys, Some(database),
                Some(Authenticate(authDb, user, password)))
            }
          }

          case _ => throw new URIParsingException(s"Could not parse URI '$uri'")
        }
      }
    }
  }

  private def parseHosts(hosts: String) = hosts.split(",").toList.map { host =>
    host.trim.split(':').toList match {
      case host :: port :: Nil => host -> {
        try {
          val p = port.toInt
          if (p > 0 && p < 65536) p
          else throw new URIParsingException(
            s"Could not parse hosts '$hosts' from URI: invalid port '$port'")

        } catch {
          case _: NumberFormatException => throw new URIParsingException(
            s"Could not parse hosts '$hosts' from URI: invalid port '$port'")

          case NonFatal(e) => throw e
        }
      }

      case "" :: _ => throw new URIParsingException(
        s"No valid host in the URI: '$hosts'")

      case host :: Nil => host -> DefaultPort

      case _ => throw new URIParsingException(
        s"Could not parse hosts from URI: invalid definition '$hosts'")
    }
  }

  private def parseHostsAndDbName(input: String): (Option[String], List[(String, Int)]) = input.takeWhile(_ != '?').split("/").toList match {
    case hosts :: Nil =>
      None -> parseHosts(hosts)

    case hosts :: dbName :: Nil =>
      Some(dbName) -> parseHosts(hosts)

    case _ =>
      throw new URIParsingException(
        s"Could not parse hosts and database from URI: '$input'")
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

  private def makeOptions(
    opts: Map[String, String],
    initial: MongoConnectionOptions): (List[String], MongoConnectionOptions) = {

    val (remOpts, step1) = opts.iterator.foldLeft(
      Map.empty[String, String] -> initial) {
        case ((unsupported, result), kv) => kv match {
          case ("authSource", v) => {
            logger.warn(s"Connection option 'authSource' deprecated: use option 'authenticationDatabase'")

            unsupported -> result.copy(authenticationDatabase = Some(v))
          }

          case ("authenticationMechanism", "x509") => unsupported -> result.
            copy(authenticationMechanism = X509Authentication)

          case ("authenticationMechanism", "mongocr") => unsupported -> result.
            copy(authenticationMechanism = CrAuthentication)

          case ("authenticationMechanism", _) => unsupported -> result.
            copy(authenticationMechanism = ScramSha1Authentication)

          // deprecated
          case ("authMode", "x509") => {
            logger.warn(s"Connection option 'authMode' is deprecated; Use option 'authenticationMechanism'")

            unsupported -> result.copy(
              authenticationMechanism = X509Authentication)
          }

          case ("authMode", "mongocr") => {
            logger.warn(s"Connection option 'authMode' is deprecated; Use option 'authenticationMechanism'")

            unsupported -> result.copy(
              authenticationMechanism = CrAuthentication)
          }

          case ("authMode", _) => {
            logger.warn(s"Connection option 'authMode' is deprecated; Use option 'authenticationMechanism'")

            unsupported -> result.copy(
              authenticationMechanism = ScramSha1Authentication)
          }
          // __deprecated

          case ("authenticationDatabase", v) =>
            unsupported -> result.copy(authenticationDatabase = Some(v))

          case ("connectTimeoutMS", v) => unsupported -> result.
            copy(connectTimeoutMS = v.toInt)

          case ("maxIdleTimeMS", v) => unsupported -> result.
            copy(maxIdleTimeMS = v.toInt)

          case ("sslEnabled", v) => {
            logger.warn(
              s"Connection option 'sslEnabled' is deprecated; Use option 'ssl'")
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

    val step2 = remOpts.get("keyStore").fold(step1) { uri =>
      val keyStore = MongoConnectionOptions.KeyStore(
        resource = new java.net.URI(uri),
        password = remOpts.get("keyStorePassword").map(_.toCharArray),
        storeType = remOpts.get("keyStoreType").getOrElse("PKCS12"))

      step1.copy(keyStore = Some(keyStore))
    }

    val remOpts2 = remOpts - "keyStore" - "keyStorePassword" - "keyStoreType"

    // Overriding options
    remOpts2.iterator.foldLeft(List.empty[String] -> step2) {
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
