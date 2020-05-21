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

import scala.util.control.{ NonFatal, NoStackTrace }

import scala.collection.immutable.ListSet
import scala.collection.mutable.{ Map => MMap }

import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.util.Timeout
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.pattern.ask

import reactivemongo.core.protocol.{ ProtocolMetadata, Response }

import reactivemongo.core.actors.{
  AuthRequest,
  Close,
  Closed,
  Exceptions,
  PickNode,
  PrimaryAvailable,
  PrimaryUnavailable,
  RegisterMonitor,
  ExpectingResponse,
  SetAvailable,
  SetUnavailable
}
import reactivemongo.core.nodeset.Authenticate

import reactivemongo.api.commands.SuccessfulAuthentication

import reactivemongo.util, util.{ LazyLogger, SRVRecordResolver, TXTResolver }

private[api] case class ConnectionState(
  metadata: ProtocolMetadata,
  setName: Option[String],
  isMongos: Boolean)

/**
 * A pool of MongoDB connections, obtained from a [[reactivemongo.api.AsyncDriver]].
 *
 * Connection here does not mean that there is one open channel to the server:
 * behind the scene, many connections (channels) are open on all the available servers in the replica set.
 *
 * Example:
 * {{{
 * import scala.concurrent.ExecutionContext
 * import reactivemongo.api._
 *
 * def foo(driver: AsyncDriver)(implicit ec: ExecutionContext) = {
 *   val con = driver.connect(List("localhost"))
 *   val db = con.flatMap(_.database("plugin"))
 *   val _ = db.map(_("acoll")) // Collection reference
 * }
 * }}}
 *
 * @param supervisor the name of the supervisor
 * @param name the unique name for the connection pool
 * @param mongosystem the reference to the internal [[reactivemongo.core.actors.MongoDBSystem]] Actor.
 *
 * @define dbName the database name
 * @define failoverStrategy the failover strategy for sending requests
 */
final class MongoConnection private[reactivemongo] (
  private[api] val supervisor: String,
  private[reactivemongo] val name: String,
  private[api] val actorSystem: ActorSystem,
  private[api] val mongosystem: ActorRef,
  private[api] val options: MongoConnectionOptions) {
  import Exceptions._

  /**
   * Returns a [[DB]] reference using this connection.
   * The failover strategy is also used to wait for the node set to be ready,
   * before returning an valid database reference.
   *
   * @param name $dbName
   * @param failoverStrategy $failoverStrategy
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.{ DB, MongoConnection }
   *
   * def resolveDB(con: MongoConnection, name: String)(
   *   implicit ec: ExecutionContext): Future[DB] =
   *   con.database(name) // with configured failover
   * }}}
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def database(name: String, failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit ec: ExecutionContext): Future[DB] =
    waitIsAvailable(failoverStrategy, stackTrace()).map { state =>
      new DB(name, this, state, failoverStrategy, None)
    }

  /**
   * Authenticates the connection on the given database.
   *
   * @param db $dbName
   * @param user the user name
   * @param password the user password
   * @param failoverStrategy $failoverStrategy
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.MongoConnection
   *
   * def authDB(con: MongoConnection, user: String, pass: String)(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   con.authenticate("myDB", user, pass).map(_ => {})
   *   // with configured failover
   *
   * }}}
   *
   * @see [[MongoConnectionOptions.credentials]]
   * @see [[DB.authenticate]]
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def authenticate(
    db: String,
    user: String,
    password: String,
    failoverStrategy: FailoverStrategy = options.failoverStrategy)(implicit ec: ExecutionContext): Future[SuccessfulAuthentication] = waitIsAvailable(
    failoverStrategy, stackTrace()).flatMap { _ =>
    val req = new AuthRequest(Authenticate(db, user, Option(password)))
    mongosystem ! req
    req.future
  }

  /**
   * Closes this connection (closes all the channels and ends the actors).
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import scala.concurrent.duration._
   *
   * import reactivemongo.api.MongoConnection
   *
   * def afterClose(con: MongoConnection)(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   con.close()(5.seconds).map { res =>
   *     println("Close result: " + res)
   *   }
   * }}}
   */
  def close()(implicit timeout: FiniteDuration): Future[_] = whenActive {
    ask(monitor, Close("MongoConnection.askClose", timeout))(Timeout(timeout))
  }

  /** Returns true if the connection has not been killed. */
  @inline def active: Boolean = !killed

  // --- Internals ---

  override def toString: String = s"MongoConnection { ${MongoConnectionOptions.toStrings(options).mkString(", ")} }"

  private[api] var history = () => InternalState.empty

  /** Whether this connection has been killed/is closed */
  @volatile private[api] var killed: Boolean = false

  @inline private def stackTrace() =
    Thread.currentThread.getStackTrace.drop(2).take(2).reverse

  /** Returns a future that will be successful when node set is available. */
  private[api] def waitIsAvailable(
    failoverStrategy: FailoverStrategy,
    contextSTE: Array[StackTraceElement])(
    implicit
    ec: ExecutionContext): Future[ConnectionState] = {

    debug("Waiting is available...")

    val timeoutFactor = 1.25D
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
    expectingResponse: ExpectingResponse): Future[Response] =
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
          new NodeSetNotReachableException(supervisor, name, history())
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

  // TODO#1.1: Remove (use probe or DB.connectionState)
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

      case close @ Close(src) => {
        debug(s"Monitor received Close request from $src")

        killed = true
        primaryAvailable = Promise.failed[ConnectionState](
          new Exception(s"[$lnm] Closing connection..."))

        setAvailable = Promise.failed[ConnectionState](
          new Exception(s"[$lnm] Closing connection..."))

        mongosystem ! Close("MonitorActor#Close", close.timeout)
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

    override def postStop() = debug("Monitor is stopped")
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

  final class URIParsingException private[api] (message: String)
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
  final class ParsedURI private[api] (
    val hosts: ListSet[(String, Int)],
    val options: MongoConnectionOptions,
    val ignoredOptions: List[String],
    val db: Option[String]) {
    // TODO: Type for URI with required DB name ?

    override def equals(that: Any): Boolean = that match {
      case other: ParsedURI =>
        other.tupled == tupled

      case _ => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"ParsedURI${tupled.toString}"

    private[api] lazy val tupled =
      Tuple4(hosts.toList, options, ignoredOptions, db)

  }

  /**
   * Parses a [[http://docs.mongodb.org/manual/reference/connection-string/ connection URI]] from its string representation.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.{ AsyncDriver, MongoConnection }
   *
   * def connectFromUri(drv: AsyncDriver, uri: String)(
   *   implicit ec: ExecutionContext): Future[MongoConnection] = for {
   *   parsedUri <- MongoConnection.fromString(uri)
   *   con <- drv.connect(parsedUri)
   * } yield con
   * }}}
   *
   * @param uri the connection URI
   */
  def fromString(uri: String)(implicit ec: ExecutionContext): Future[ParsedURI] = fromString(uri, reactivemongo.util.dnsResolve(), reactivemongo.util.txtRecords())

  private[reactivemongo] def fromString(
    uri: String,
    srvRecResolver: SRVRecordResolver,
    txtResolver: TXTResolver)(
    implicit
    ec: ExecutionContext): Future[ParsedURI] = {

    val seedList = uri.startsWith("mongodb+srv://")

    for {
      useful <- {
        if (uri startsWith "mongodb://") {
          Future.successful(uri drop 10)
        } else if (seedList) {
          Future.successful(uri drop 14)
        } else {
          Future.failed(new URIParsingException(s"Invalid scheme: $uri"))
        }
      }
      setSpec = useful.takeWhile(_ != '?') // options already parsed
      credentialEnd = setSpec.indexOf("@")

      (unsupportedKeys, options) <- {
        val empty = MongoConnectionOptions.default
        val initial = if (!seedList) empty else {
          empty.copy(sslEnabled = true)
        }

        def txtOptions: Future[Map[String, String]] = {
          if (!seedList) {
            Future.successful(Map.empty[String, String])
          } else {
            val serviceName = setSpec. // strip credentials before '@',
              drop(credentialEnd + 1).takeWhile(_ != '/') // and DB after '/'

            for {
              records <- Await.ready(
                txtResolver(serviceName),
                reactivemongo.util.dnsTimeout)
              res <- records.foldLeft(
                Future.successful(Map.empty[String, String])) { (o, r) =>
                  for {
                    prev <- o
                    cur <- parseOptions(r)
                  } yield prev ++ cur
                }
            } yield res
          }
        }

        def optionStr = useful.drop(setSpec.size).stripPrefix("?")

        for {
          txt <- txtOptions
          os <- parseOptions(optionStr)
          opts = makeOptions(txt ++ os, initial)
          res <- {
            if (opts._2.maxIdleTimeMS != 0 &&
              opts._2.maxIdleTimeMS < opts._2.heartbeatFrequencyMS) {

              Future.failed[(List[String], MongoConnectionOptions)](new URIParsingException(s"Invalid URI options: maxIdleTimeMS(${opts._2.maxIdleTimeMS}) < heartbeatFrequencyMS(${opts._2.heartbeatFrequencyMS})"))
            } else {
              Future.successful(opts)
            }
          }
        } yield res
      }

      parsedUri <- {
        if (credentialEnd == -1) {
          parseHostsAndDbName(seedList, setSpec, srvRecResolver).flatMap {
            case (db, hosts) => options.authenticationMechanism match {
              case X509Authentication => db match {
                case Some(dbName) => {
                  val optsWithX509 = options.copy(credentials = Map(
                    dbName -> MongoConnectionOptions.Credential("", None)))

                  Future.successful(new ParsedURI(
                    hosts, optsWithX509, unsupportedKeys, db))
                }

                case _ =>
                  Future.failed[ParsedURI](new URIParsingException(s"Could not parse URI '$uri': authentication information found but no database name in URI"))
              }

              case _ => Future.successful(
                new ParsedURI(hosts, options, unsupportedKeys, db))
            }
          }
        } else {
          val WithAuth = """([^:]+)(|:[^@]*)@(.+)""".r

          setSpec match {
            case WithAuth(user, p, hostsPortsAndDbName) => {
              val pass = p.stripPrefix(":")

              parseHostsAndDbName(
                seedList, hostsPortsAndDbName, srvRecResolver).flatMap {
                case (Some(database), hosts) => {
                  if (options.authenticationMechanism == X509Authentication && pass.nonEmpty) {
                    Future.failed[ParsedURI](new URIParsingException("You should not provide a password when authenticating with X509 authentication"))
                  } else {
                    val password = {
                      if (options.authenticationMechanism != X509Authentication) {
                        Option(pass)
                      } else Option.empty[String]
                    }

                    val authDb = options.
                      authenticationDatabase.getOrElse(database)

                    val optsWithCred = options.copy(
                      credentials = options.credentials + (
                        authDb -> MongoConnectionOptions.Credential(
                          user, password)))

                    Future.successful(new ParsedURI(
                      hosts, optsWithCred, unsupportedKeys,
                      Some(database)))
                  }
                }

                case _ =>
                  Future.failed[ParsedURI](new URIParsingException(s"Could not parse URI '$uri': authentication information found but no database name in URI"))
              }
            }

            case _ => Future.failed[ParsedURI](
              new URIParsingException(s"Could not parse URI '$uri'"))
          }
        }
      }
    } yield parsedUri
  }

  private def parseHosts(
    seedList: Boolean,
    hosts: String,
    srvRecResolver: SRVRecordResolver)(implicit ec: ExecutionContext): Future[ListSet[(String, Int)]] = {
    if (seedList) {
      Await.ready(
        reactivemongo.util.srvRecords(hosts)(srvRecResolver),
        reactivemongo.util.dnsTimeout).map {
          ListSet.empty ++ _
        }
    } else {
      val buf = ListSet.newBuilder[(String, Int)]

      @annotation.tailrec
      def parse(input: Iterable[String]): Future[ListSet[(String, Int)]] =
        input.headOption match {
          case Some(h) => h.span(_ != ':') match {
            case ("", _) => Future.failed(
              new URIParsingException(s"No valid host in the URI: '$h'"))

            case (host, "") => {
              buf += host -> DefaultPort
              parse(input.drop(1))
            }

            case (host, port) => {
              val res: Either[Throwable, (String, Int)] = try {
                val p = port.drop(1).toInt

                if (p <= 0 || p >= 65536) {
                  Left(new URIParsingException(s"Could not parse host '$h' from URI: invalid port '$port'"))
                } else {
                  Right(host -> p)
                }
              } catch {
                case _: NumberFormatException =>
                  Left(new URIParsingException(s"Could not parse host '$h' from URI: invalid port '$port'"))

                case NonFatal(cause) => Left(cause)
              }

              res match {
                case Left(cause) =>
                  Future.failed(cause)

                case Right(node) => {
                  buf += node
                  parse(input.drop(1))
                }
              }
            }

            case _ => Future.failed(new URIParsingException(
              s"Could not parse host from URI: invalid definition '$h'"))
          }

          case _ =>
            Future.successful(buf.result())
        }

      parse(hosts split ",")
    }
  }

  private def parseHostsAndDbName(
    seedList: Boolean,
    input: String,
    srvRecResolver: SRVRecordResolver)(
    implicit
    ec: ExecutionContext): Future[(Option[String], ListSet[(String, Int)])] =
    input.span(_ != '/') match {
      case (hosts, "") =>
        parseHosts(seedList, hosts, srvRecResolver).map(None -> _)

      case (hosts, dbName) =>
        parseHosts(seedList, hosts, srvRecResolver).map {
          Some(dbName drop 1) -> _
        }

      case _ =>
        Future.failed(new URIParsingException(
          s"Could not parse hosts and database from URI: '$input'"))
    }

  private def parseOptions(options: String): Future[Map[String, String]] = {
    if (options.isEmpty) {
      Future.successful(Map.empty[String, String])
    } else {
      val buf = MMap.empty[String, String]

      @annotation.tailrec
      def parse(input: Iterable[String]): Future[Map[String, String]] =
        input.headOption match {
          case Some(option) => option.span(_ != '=') match {
            case (_, "") => Future.failed[Map[String, String]](
              new URIParsingException(
                s"Could not parse invalid options '$options'"))

            case (key, v) => {
              buf.put(key, v.drop(1))
              parse(input.drop(1))
            }
          }

          case _ =>
            Future.successful(buf.toMap)
        }

      parse(options split "&")
    }
  }

  private val IntRe = "^([0-9]+)$".r

  private val FailoverRe = "^([^:]+):([0-9]+)x([0-9.]+)$".r

  private def makeOptions(
    opts: Map[String, String],
    initial: MongoConnectionOptions): (List[String], MongoConnectionOptions) = {

    @inline def make(name: String, input: String, unsupported: Map[String, String], parsed: MongoConnectionOptions)(f: => MongoConnectionOptions): (Map[String, String], MongoConnectionOptions) = try {
      unsupported -> f
    } catch {
      case NonFatal(cause) =>
        logger.debug(s"Invalid option '$name': $input", cause)

        (unsupported + (name -> input)) -> parsed
    }

    val (remOpts, step1) = opts.iterator.foldLeft(
      Map.empty[String, String] -> initial) {
        case ((unsupported, result), kv) => kv match {
          case ("replicaSet", _) => {
            logger.info("Connection option 'replicaSet' is ignored: determined from servers response")

            unsupported -> result
          }

          case ("authenticationMechanism", "x509") => unsupported -> result.
            copy(authenticationMechanism = X509Authentication)

          case ("authenticationMechanism", "scram-sha256") =>
            unsupported -> result.copy(
              authenticationMechanism = ScramSha256Authentication)

          case ("authenticationMechanism", _) => unsupported -> result.
            copy(authenticationMechanism = ScramSha1Authentication)

          case ("authenticationDatabase", v) =>
            unsupported -> result.copy(authenticationDatabase = Some(v))

          case ("connectTimeoutMS", v) => unsupported -> result.
            copy(connectTimeoutMS = v.toInt)

          case ("maxIdleTimeMS", v) => unsupported -> result.
            copy(maxIdleTimeMS = v.toInt)

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

          case ("rm.maxInFlightRequestsPerChannel", IntRe(max)) =>
            unsupported -> result.copy(
              maxInFlightRequestsPerChannel = Some(max.toInt))

          case ("rm.minIdleChannelsPerNode", IntRe(min)) =>
            unsupported -> result.copy(minIdleChannelsPerNode = min.toInt)

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

          case ("readConcernLevel", ReadConcern(c)) =>
            unsupported -> result.copy(readConcern = c)

          case ("rm.failover", "default") => unsupported -> result
          case ("rm.failover", "remote") => unsupported -> result.copy(
            failoverStrategy = FailoverStrategy.remote)

          case ("rm.failover", "strict") => unsupported -> result.copy(
            failoverStrategy = FailoverStrategy.strict)

          case ("rm.failover", opt @ FailoverRe(d, r, f)) =>
            make("rm.failover", opt, unsupported, result) {
              val (time, unit) = Duration.unapply(Duration(d)) match {
                case Some(dur) => dur
                case _ =>
                  throw new URIParsingException(
                    s"Invalid duration 'rm.failover': $opt")
              }

              val delay = FiniteDuration(time, unit)
              val retry = r.toInt
              val factor = f.toDouble
              val strategy = FailoverStrategy(delay, retry, _ * factor)

              result.copy(failoverStrategy = strategy)
            }

          case ("heartbeatFrequencyMS", IntRe(ms)) =>
            make("heartbeatFrequencyMS", ms, unsupported, result) {
              val millis = ms.toInt

              if (millis < 500) {
                throw new URIParsingException(
                  "'heartbeatFrequencyMS' must be >= 500 milliseconds")
              }

              result.copy(heartbeatFrequencyMS = millis)
            }

          case ("appName", nme) => Option(nme).map(_.trim).filter(v => {
            v.nonEmpty && v.getBytes("UTF-8").size < 128
          }) match {
            case Some(appName) =>
              unsupported -> result.copy(appName = Some(appName))

            case _ =>
              (unsupported + ("appName" -> nme)) -> result
          }

          case kv => (unsupported + kv) -> result
        }
      }

    val step2 = remOpts.get("keyStore").fold(step1) { uri =>
      val keyStore = MongoConnectionOptions.KeyStore(
        resource = new java.net.URI(uri),
        password = remOpts.get("keyStorePassword").map(_.toCharArray),
        storeType = remOpts.getOrElse("keyStoreType", "PKCS12"),
        trust = true)

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
