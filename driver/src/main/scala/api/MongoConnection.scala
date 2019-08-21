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

import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.pattern.ask

import reactivemongo.core.protocol.Response

import reactivemongo.core.actors.{
  AuthRequest,
  Close,
  Closed,
  Exceptions,
  PickNode,
  PrimaryAvailable,
  PrimaryUnavailable,
  RegisterMonitor,
  RequestMakerExpectingResponse,
  SetAvailable,
  SetUnavailable
}
import reactivemongo.core.nodeset.{ Authenticate, ProtocolMetadata }
import reactivemongo.core.commands.SuccessfulAuthentication
import reactivemongo.api.commands.{ WriteConcern => WC }

import reactivemongo.util, util.{ LazyLogger, SRVRecordResolver, TXTResolver }

private[api] case class ConnectionState(
  metadata: ProtocolMetadata,
  setName: Option[String],
  isMongos: Boolean)

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
  @deprecated("Internal: will be made private", "0.17.0") val supervisor: String,
  @deprecated("Internal: will be made private", "0.17.0") val name: String,
  @deprecated("Internal: will be made private", "0.14.0") val actorSystem: ActorSystem,
  @deprecated("Internal: will be made private", "0.17.0") val mongosystem: ActorRef,
  @deprecated("Internal: will be made private", "0.17.0") val options: MongoConnectionOptions) { // TODO: toString as MongoURI
  import Exceptions._

  /**
   * Returns a DefaultDB reference using this connection.
   * The failover strategy is also used to wait for the node set to be ready,
   * before returning an available DB.
   *
   * @param name $dbName
   * @param failoverStrategy $failoverStrategy
   */
  def database(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit @deprecatedName('context) ec: ExecutionContext): Future[DefaultDB] =
    waitIsAvailable(failoverStrategy, stackTrace()).map { state =>
      new DefaultDB(name, this, state, failoverStrategy)
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

  /** Returns true if the connection has not been killed. */
  @inline def active: Boolean = !killed

  // --- Internals ---

  // TODO: Review
  private[api] var history = () => InternalState.empty

  /** Whether this connection has been killed/is closed */
  @volatile private[api] var killed: Boolean = false

  @inline private def stackTrace() =
    Thread.currentThread.getStackTrace.tail.tail.take(2).reverse

  /** Returns a future that will be successful when node set is available. */
  private[api] def waitIsAvailable(
    failoverStrategy: FailoverStrategy,
    contextSTE: Array[StackTraceElement])(
    implicit
    ec: ExecutionContext): Future[ConnectionState] = {

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
        Future.failed[ConnectionState](error)
      }
    }
  }

  private def whenActive[T](f: => Future[T]): Future[T] = {
    if (killed) {
      debug("Cannot send request when the connection is killed")
      Future.failed(new ClosedException(supervisor, name, history()))
    } else f
  }

  private[api] def sendExpectingResponse(
    expectingResponse: RequestMakerExpectingResponse): Future[Response] =
    whenActive {
      mongosystem ! expectingResponse
      expectingResponse.future
    }

  /** The name of the picked node */
  private[api] def pickNode(readPreference: ReadPreference): Future[String] =
    whenActive {
      val req = PickNode(readPreference)

      mongosystem ! req

      req.future
    }

  private case class IsAvailable(result: Promise[ConnectionState]) {
    override val toString = s"IsAvailable#${System identityHashCode this}?"
  }

  private case class IsPrimaryAvailable(result: Promise[ConnectionState]) {
    override val toString = s"IsPrimaryAvailable#${System identityHashCode this}?"
  }

  /**
   * Checks whether is unavailable.
   *
   * @return Future(None) if available
   */
  private[api] def probe(timeout: FiniteDuration): Future[ConnectionState] =
    whenActive {
      val p = Promise[ConnectionState]()
      val check = {
        if (options.readPreference.slaveOk) IsAvailable(p)
        else IsPrimaryAvailable(p)
      }

      monitor ! check

      import akka.pattern.after
      import actorSystem.dispatcher

      def unavailResult = Future.failed[ConnectionState] {
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
            warn(s"Timeout after $timeout while probing the connection monitor: $check")
            unavailResult
          }
        })))
    }

  private[api] lazy val monitor = actorSystem.actorOf(
    Props(new MonitorActor), s"Monitor-$name")

  // TODO: Remove (use probe or DB.connectionState)
  @volatile private[api] var _metadata = Option.empty[ProtocolMetadata]

  private class MonitorActor extends Actor {
    import scala.collection.mutable.Queue

    mongosystem ! RegisterMonitor

    private var setAvailable = Promise[ConnectionState]()
    private var primaryAvailable = Promise[ConnectionState]()

    private val waitingForClose = Queue[ActorRef]()

    val receive: Receive = {
      case available: PrimaryAvailable => {
        debug(s"A primary is available: ${available.metadata}")

        _metadata = Some(available.metadata)

        val state = ConnectionState(
          available.metadata, available.setName, available.isMongos)

        if (!primaryAvailable.trySuccess(state)) {
          primaryAvailable = Promise.successful(state)
        }
      }

      case PrimaryUnavailable => {
        debug("There is no primary available")

        if (primaryAvailable.isCompleted) { // reset availability
          primaryAvailable = Promise[ConnectionState]()
        }
      }

      case available: SetAvailable => {
        debug(s"A node is available: ${available.metadata}")

        _metadata = Some(available.metadata)

        val state = ConnectionState(
          available.metadata, available.setName, available.isMongos)

        if (!setAvailable.trySuccess(state)) {
          setAvailable = Promise.successful(state)
        }
      }

      case SetUnavailable => {
        debug("No node seems to be available")

        if (setAvailable.isCompleted) {
          setAvailable = Promise[ConnectionState]()
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
        primaryAvailable = Promise.failed[ConnectionState](
          new Exception(s"[$lnm] Closing connection..."))

        setAvailable = Promise.failed[ConnectionState](
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
    hosts: List[(String, Int)], // TODO: ListSet
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
    import scala.concurrent.ExecutionContext.Implicits.global

    parseURI(
      uri,
      reactivemongo.util.dnsResolve(),
      reactivemongo.util.txtRecords())
  }

  private[reactivemongo] def parseURI(
    uri: String,
    srvRecResolver: SRVRecordResolver,
    txtResolver: TXTResolver): Try[ParsedURI] = {

    val seedList = uri.startsWith("mongodb+srv://")

    Try {
      val useful: String = {
        if (uri startsWith "mongodb://") uri.drop(10)
        else if (seedList) uri.drop(14)
        else throw new URIParsingException(s"Invalid scheme: $uri")
      }

      val setSpec = useful.takeWhile(_ != '?') // options already parsed
      val credentialEnd = setSpec.indexOf("@")

      def opts = {
        val empty = MongoConnectionOptions.default
        val initial = if (!seedList) empty else {
          empty.copy(sslEnabled = true)
        }

        def txtOptions: Map[String, String] = {
          if (!seedList) {
            Map.empty[String, String]
          } else {
            val serviceName = setSpec. // strip credentials before '@',
              drop(credentialEnd + 1).takeWhile(_ != '/') // and DB after '/'

            val records = Await.result(
              txtResolver(serviceName),
              reactivemongo.util.dnsTimeout)

            records.foldLeft(Map.empty[String, String]) { (o, r) =>
              o ++ parseOptions(r)
            }
          }
        }

        val optionStr = useful.drop(setSpec.size).stripPrefix("?")

        makeOptions(txtOptions ++ parseOptions(optionStr), initial)
      }

      if (opts._2.maxIdleTimeMS != 0 &&
        opts._2.maxIdleTimeMS < opts._2.heartbeatFrequencyMS) {

        throw new URIParsingException(s"Invalid URI options: maxIdleTimeMS(${opts._2.maxIdleTimeMS}) < heartbeatFrequencyMS(${opts._2.heartbeatFrequencyMS})")
      }

      // ---

      val (unsupportedKeys, options) = opts

      if (credentialEnd == -1) {
        val (db, hosts) = parseHostsAndDbName(seedList, setSpec, srvRecResolver)

        options.authenticationMechanism match {
          case X509Authentication => {
            val dbName = db.getOrElse(throw new URIParsingException(s"Could not parse URI '$uri': authentication information found but no database name in URI"))

            val optsWithX509 = options.copy(credentials = Map(
              dbName -> MongoConnectionOptions.Credential("", None)))

            ParsedURI(hosts.toList, optsWithX509, unsupportedKeys, db,
              Some(Authenticate(dbName, "", None)))
          }

          case _ => ParsedURI(hosts.toList, options, unsupportedKeys, db, None)
        }
      } else {
        val WithAuth = """([^:]+)(|:[^@]*)@(.+)""".r

        setSpec match {
          case WithAuth(user, p, hostsPortsAndDbName) => {
            val pass = p.stripPrefix(":")
            val (db, hosts) = parseHostsAndDbName(
              seedList, hostsPortsAndDbName, srvRecResolver)

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

              ParsedURI(hosts.toList, optsWithCred, unsupportedKeys,
                Some(database), Some(Authenticate(authDb, user, password)))
            }
          }

          case _ => throw new URIParsingException(s"Could not parse URI '$uri'")
        }
      }
    }
  }

  private def parseHosts(
    seedList: Boolean,
    hosts: String,
    srvRecResolver: SRVRecordResolver): List[(String, Int)] = {
    if (seedList) {
      import scala.concurrent.ExecutionContext.Implicits.global

      Await.result(
        reactivemongo.util.srvRecords(hosts)(srvRecResolver),
        reactivemongo.util.dnsTimeout).toList

    } else {
      hosts.split(",").map { h =>
        h.span(_ != ':') match {
          case ("", _) => throw new URIParsingException(
            s"No valid host in the URI: '$h'")

          case (host, "") => host -> DefaultPort

          case (host, port) => host -> {
            try {
              val p = port.drop(1).toInt
              if (p > 0 && p < 65536) p
              else throw new URIParsingException(
                s"Could not parse host '$h' from URI: invalid port '$port'")

            } catch {
              case _: NumberFormatException => throw new URIParsingException(
                s"Could not parse host '$h' from URI: invalid port '$port'")

              case NonFatal(e) => throw e
            }
          }

          case _ => throw new URIParsingException(
            s"Could not parse host from URI: invalid definition '$h'")
        }
      }.toList
    }
  }

  private def parseHostsAndDbName(
    seedList: Boolean,
    input: String,
    srvRecResolver: SRVRecordResolver): (Option[String], List[(String, Int)]) =
    input.span(_ != '/') match {
      case (hosts, "") =>
        None -> parseHosts(seedList, hosts, srvRecResolver)

      case (hosts, dbName) =>
        Some(dbName drop 1) -> parseHosts(seedList, hosts, srvRecResolver)

      case _ =>
        throw new URIParsingException(
          s"Could not parse hosts and database from URI: '$input'")
    }

  private def parseOptions(options: String): Map[String, String] = {
    if (options.isEmpty) {
      Map.empty[String, String]
    } else {
      util.toMap(options.split("&")) { option =>
        option.split("=").toList match {
          case key :: value :: Nil => (key -> value)
          case _ => throw new URIParsingException(
            s"Could not parse invalid options '$options'")
        }
      }
    }
  }

  @deprecated("Internal: will be made private", "0.16.0")
  val IntRe = "^([0-9]+)$".r

  @deprecated("Internal: will be made private", "0.16.0")
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

          case ("authenticationMechanism", "scram-sha256") =>
            unsupported -> result.copy(
              authenticationMechanism = ScramSha256Authentication)

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

          case ("rm.reconnectDelayMS", opt @ IntRe(ms)) => {
            logger.warn(s"Connection option 'rm.reconnectDelayMS' deprecated: use option 'heartbeatFrequencyMS'")

            Try(ms.toInt).filter(_ >= 500 /* ms */ ).toOption match {
              case Some(interval) => unsupported -> result.copy(
                heartbeatFrequencyMS = interval)

              case _ => (unsupported + ("rm.reconnectDelayMS" -> opt)) -> result
            }
          }

          case ("rm.maxInFlightRequestsPerChannel", IntRe(max)) =>
            unsupported -> result.copy(
              maxInFlightRequestsPerChannel = Some(max.toInt))

          case ("writeConcern", "unacknowledged") => unsupported -> result.
            copy(writeConcern = WC.Unacknowledged)

          case ("writeConcern", "acknowledged") => unsupported -> result.
            copy(writeConcern = WC.Acknowledged)

          case ("writeConcern", "journaled") => unsupported -> result.
            copy(writeConcern = WC.Journaled)

          case ("writeConcern", "default") => unsupported -> result.
            copy(writeConcern = WC.Default)

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

          case ("rm.monitorRefreshMS", opt @ IntRe(ms)) => {
            logger.warn(s"Connection option 'rm.monitorRefreshMS' deprecated: use option 'heartbeatFrequencyMS'")

            Try(ms.toInt).filter(_ >= 500 /* ms */ ).toOption match {
              case Some(interval) => unsupported -> result.copy(
                heartbeatFrequencyMS = interval)

              case _ => (unsupported + ("rm.monitorRefreshMS" -> opt)) -> result
            }
          }

          case ("heartbeatFrequencyMS", opt @ IntRe(ms)) =>
            Try(ms.toInt).filter(_ >= 500 /* ms */ ).toOption match {
              case Some(interval) => unsupported -> result.copy(
                heartbeatFrequencyMS = interval)

              case _ => (unsupported + ("heartbeatFrequencyMS" -> opt)) -> result
            }

          case ("appName", nme) => Option(nme).map(_.trim).filter(v => {
            v.nonEmpty && v.getBytes("UTF-8").size < 128
          }) match {
            case Some(appName) => unsupported -> result.withAppName(appName)
            case _             => (unsupported + ("appName" -> nme)) -> result
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
            copy(w = WC.Majority))

        case ("writeConcernW", IntRe(str)) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(w = WC.WaitForAcknowledgments(str.toInt)))

        case ("writeConcernW", tag) => unsupported -> result.
          copy(writeConcern = result.writeConcern.
            copy(w = WC.TagSet(tag)))

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
