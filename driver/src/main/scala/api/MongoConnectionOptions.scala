package reactivemongo.api

import java.net.URI

import reactivemongo.api.commands.{ WriteConcern => WC }

/**
 * Options for MongoConnection.
 *
 * @param connectTimeoutMS The number of milliseconds to wait for a connection to be established before giving up.
 * @param authenticationDatabase the name of the database used for authentication
 * @param sslEnabled Enable SSL connection (required to be accepted on server-side).
 * @param sslAllowsInvalidCert If `sslEnabled` is true, this one indicates whether to accept invalid certificates (e.g. self-signed).
 * @param authMode Either [[CrAuthentication]] or [[ScramSha1Authentication]] or [[X509Authentication]]
 * @param tcpNoDelay TCPNoDelay flag (ReactiveMongo-specific option). The default value is false (see [[http://docs.oracle.com/javase/8/docs/api/java/net/StandardSocketOptions.html#TCP_NODELAY TCP_NODELAY]]).
 * @param keepAlive TCP KeepAlive flag (ReactiveMongo-specific option). The default value is false (see [[http://docs.oracle.com/javase/8/docs/api/java/net/StandardSocketOptions.html#SO_KEEPALIVE SO_KEEPALIVE]]).
 * @param nbChannelsPerNode Number of channels (connections) per node (ReactiveMongo-specific option).
 * @param writeConcern the default [[https://docs.mongodb.com/manual/reference/write-concern/ write concern]]
 * @param readPreference the default read preference
 * @param failoverStrategy the default failover strategy
 * @param heartbeatFrequencyMS the interval in milliseconds used by monitor to refresh the node set (default: 10000 aka 10s)
 * @param maxIdleTimeMS the maximum number of milliseconds that a [[https://docs.mongodb.com/manual/reference/connection-string/#urioption.maxIdleTimeMS channel can remain idle]] in the connection pool before being removed and closed (default: 0 to disable, as implemented using [[http://netty.io/4.1/api/io/netty/handler/timeout/IdleStateHandler.html Netty IdleStateHandler]]); If not 0, must be greater or equal to [[#heartbeatFrequencyMS]]
 * @param maxHistorySize the maximum size of the pool history (default: 25)
 * @param credentials the credentials per authentication database names
 * @param keyStore an optional key store
 * @param readConcern the default [[https://docs.mongodb.com/manual/reference/read-concern/ read concern]]
 */
class MongoConnectionOptions private[reactivemongo] (
  // canonical options - connection
  val connectTimeoutMS: Int = 0,

  // canonical options - authentication options
  val authenticationDatabase: Option[String] = None,
  val sslEnabled: Boolean = false,
  val sslAllowsInvalidCert: Boolean = false,
  val authenticationMechanism: AuthenticationMode = ScramSha1Authentication,

  // reactivemongo specific options
  val tcpNoDelay: Boolean = false,
  val keepAlive: Boolean = false,
  val nbChannelsPerNode: Int = 10,
  val maxInFlightRequestsPerChannel: Option[Int] = Some(200),

  // read and write preferences
  val writeConcern: WC = WC.Default,
  val readPreference: ReadPreference = ReadPreference.primary,

  val failoverStrategy: FailoverStrategy = FailoverStrategy.default,

  val heartbeatFrequencyMS: Int = 10000,
  val maxIdleTimeMS: Int = 0,
  val maxHistorySize: Int = 25,
  val credentials: Map[String, MongoConnectionOptions.Credential] = Map.empty,
  val keyStore: Option[MongoConnectionOptions.KeyStore] = Option.empty,
  val readConcern: ReadConcern = ReadConcern.default) extends Product with Serializable {

  /**
   * The database source for authentication credentials (corresponds to the `authenticationMechanism` with MongoShell).
   */
  @inline def authMode: AuthenticationMode = authenticationMechanism

  /**
   * The database source for authentication credentials (corresponds to the `authenticationDatabase` with MongoShell).
   */
  @deprecated("Use [[authenticationDatabase]]", "0.12.7")
  @inline def authSource: Option[String] = authenticationDatabase

  @deprecated("Use heartbeatFrequencyMS", "0.16.4")
  @inline def monitorRefreshMS = heartbeatFrequencyMS

  // TODO: Expose?
  @inline private[reactivemongo] def minIdleChannelsPerNode: Int = 1

  @deprecated("Use the other `copy`", "0.17.0")
  def copy(
    connectTimeoutMS: Int,
    @deprecatedName(Symbol("authSource")) authenticationDatabase: Option[String],
    sslEnabled: Boolean,
    sslAllowsInvalidCert: Boolean,
    @deprecatedName(Symbol("authMode")) authenticationMechanism: AuthenticationMode,
    tcpNoDelay: Boolean,
    keepAlive: Boolean,
    nbChannelsPerNode: Int,
    @deprecated("Unused, see heartbeatFrequencyMS", "0.16.4") reconnectDelayMS: Int,
    writeConcern: WC,
    readPreference: ReadPreference,
    failoverStrategy: FailoverStrategy,
    @deprecatedName(Symbol("monitorRefreshMS")) heartbeatFrequencyMS: Int,
    maxIdleTimeMS: Int,
    maxHistorySize: Int,
    credentials: Map[String, MongoConnectionOptions.Credential],
    keyStore: Option[MongoConnectionOptions.KeyStore],
    readConcern: ReadConcern): MongoConnectionOptions = {
    val copied = MongoConnectionOptions(
      connectTimeoutMS, authenticationDatabase, sslEnabled,
      sslAllowsInvalidCert, authenticationMechanism, tcpNoDelay, keepAlive,
      nbChannelsPerNode, reconnectDelayMS, writeConcern, readPreference,
      failoverStrategy: FailoverStrategy, heartbeatFrequencyMS, maxIdleTimeMS,
      maxHistorySize, credentials, keyStore, readConcern)

    copied._appName = this.appName
    copied
  }

  def copy(
    connectTimeoutMS: Int = this.connectTimeoutMS,
    authenticationDatabase: Option[String] = this.authenticationDatabase,
    sslEnabled: Boolean = this.sslEnabled,
    sslAllowsInvalidCert: Boolean = this.sslAllowsInvalidCert,
    authenticationMechanism: AuthenticationMode = this.authenticationMechanism,
    tcpNoDelay: Boolean = this.tcpNoDelay,
    keepAlive: Boolean = this.keepAlive,
    nbChannelsPerNode: Int = this.nbChannelsPerNode,
    maxInFlightRequestsPerChannel: Option[Int] = this.maxInFlightRequestsPerChannel,
    writeConcern: WC = this.writeConcern,
    readPreference: ReadPreference = this.readPreference,
    failoverStrategy: FailoverStrategy = this.failoverStrategy,
    heartbeatFrequencyMS: Int = this.heartbeatFrequencyMS,
    maxIdleTimeMS: Int = this.maxIdleTimeMS,
    maxHistorySize: Int = this.maxHistorySize,
    credentials: Map[String, MongoConnectionOptions.Credential] = this.credentials,
    keyStore: Option[MongoConnectionOptions.KeyStore] = this.keyStore,
    readConcern: ReadConcern = this.readConcern): MongoConnectionOptions = {
    val copied = new MongoConnectionOptions(
      connectTimeoutMS = connectTimeoutMS,
      authenticationDatabase = authenticationDatabase,
      sslEnabled = sslEnabled,
      sslAllowsInvalidCert = sslAllowsInvalidCert,
      authenticationMechanism = authenticationMechanism,
      tcpNoDelay = tcpNoDelay,
      keepAlive = keepAlive,
      nbChannelsPerNode = nbChannelsPerNode,
      maxInFlightRequestsPerChannel = maxInFlightRequestsPerChannel,
      writeConcern = writeConcern,
      readPreference = readPreference,
      failoverStrategy = failoverStrategy,
      heartbeatFrequencyMS = heartbeatFrequencyMS,
      maxIdleTimeMS = maxIdleTimeMS,
      maxHistorySize = maxHistorySize,
      credentials = credentials,
      keyStore = keyStore,
      readConcern = readConcern)

    copied._appName = this.appName
    copied
  }

  @deprecated("Will no longer be a `Product`", "0.17.0")
  val productArity = 18

  @deprecated("Will no longer be a `Product`", "0.17.0")
  def productElement(n: Int): Any = (n: @annotation.switch) match {
    case 0  => connectTimeoutMS
    case 1  => authenticationDatabase
    case 2  => sslEnabled
    case 3  => sslAllowsInvalidCert
    case 4  => authenticationMechanism
    case 5  => tcpNoDelay
    case 6  => keepAlive
    case 7  => nbChannelsPerNode
    case 8  => maxInFlightRequestsPerChannel
    case 9  => writeConcern
    case 10 => readPreference
    case 11 => failoverStrategy
    case 12 => heartbeatFrequencyMS
    case 13 => maxIdleTimeMS
    case 14 => maxHistorySize
    case 15 => credentials
    case 16 => keyStore
    case 17 => readConcern
    case 18 => _appName
  }

  def canEqual(that: Any): Boolean = that match {
    case _: MongoConnectionOptions => true
    case _                         => false
  }

  override def toString = s"""MongoConnectionOptions { ${MongoConnectionOptions.toStrings(this).mkString(", ")} }"""

  override def hashCode: Int = tupled.hashCode

  override def equals(that: Any): Boolean = that match {
    case other: MongoConnectionOptions =>
      other.tupled == this.tupled

    case _ =>
      false
  }

  private[api] lazy val tupled = Tuple19(
    connectTimeoutMS,
    authenticationDatabase,
    sslEnabled,
    sslAllowsInvalidCert,
    authenticationMechanism,
    tcpNoDelay,
    keepAlive,
    nbChannelsPerNode,
    maxInFlightRequestsPerChannel,
    writeConcern,
    readPreference,
    failoverStrategy,
    heartbeatFrequencyMS,
    maxIdleTimeMS,
    maxHistorySize,
    credentials,
    keyStore,
    readConcern,
    _appName)

  /** The application name (custom or default) */
  @inline def appName: Option[String] = _appName

  private var _appName = Option.empty[String]

  private[reactivemongo] def withAppName(name: String): MongoConnectionOptions = {
    val opts = copy()
    opts._appName = Option(name)
    opts
  }
}

object MongoConnectionOptions {
  /** The default options */
  @inline def default: MongoConnectionOptions = new MongoConnectionOptions()

  @deprecated("Use `MongoConnectionOptions` constructor", "0.17.0")
  def apply(
    connectTimeoutMS: Int = 0,
    @deprecatedName(Symbol("authSource")) authenticationDatabase: Option[String] = None,
    sslEnabled: Boolean = false,
    sslAllowsInvalidCert: Boolean = false,
    @deprecatedName(Symbol("authMode")) authenticationMechanism: AuthenticationMode = ScramSha1Authentication,
    tcpNoDelay: Boolean = false,
    keepAlive: Boolean = false,
    nbChannelsPerNode: Int = 10,
    @deprecated("Unused, see heartbeatFrequencyMS", "0.16.4") reconnectDelayMS: Int = 1000,
    writeConcern: WC = WC.Default,
    readPreference: ReadPreference = ReadPreference.primary,
    failoverStrategy: FailoverStrategy = FailoverStrategy.default,
    @deprecatedName(Symbol("monitorRefreshMS")) heartbeatFrequencyMS: Int = 10000,
    maxIdleTimeMS: Int = 0,
    maxHistorySize: Int = 25,
    credentials: Map[String, MongoConnectionOptions.Credential] = Map.empty,
    keyStore: Option[MongoConnectionOptions.KeyStore] = Option.empty,
    readConcern: ReadConcern = ReadConcern.default): MongoConnectionOptions =
    new MongoConnectionOptions(
      connectTimeoutMS = connectTimeoutMS,
      authenticationDatabase = authenticationDatabase,
      sslEnabled = sslEnabled,
      sslAllowsInvalidCert = sslAllowsInvalidCert,
      authenticationMechanism = authenticationMechanism,
      tcpNoDelay = tcpNoDelay,
      keepAlive = keepAlive,
      nbChannelsPerNode = nbChannelsPerNode,
      writeConcern = writeConcern,
      readPreference = readPreference,
      failoverStrategy = failoverStrategy,
      heartbeatFrequencyMS = heartbeatFrequencyMS,
      maxIdleTimeMS = maxIdleTimeMS,
      maxHistorySize = maxHistorySize,
      credentials = credentials,
      keyStore = keyStore,
      readConcern = readConcern)

  // ---

  /**
   * @param user the name (or subject) of the user
   * @param password the associated password if some
   */
  case class Credential(user: String, password: Option[String])

  final class KeyStore(
    val resource: URI,
    val password: Option[Array[Char]],
    val storeType: String,
    val trust: Boolean) extends Product {
    @deprecated("Use constructor with `trust` parameter", "0.18.2")
    def this(
      resource: URI,
      password: Option[Array[Char]],
      storeType: String) = this(resource, password, storeType, true)

    override def toString = s"KeyStore#${storeType}{$resource}"

    import java.util.Arrays

    def canEqual(that: Any): Boolean = that match {
      case _: KeyStore => true
      case _           => false
    }

    val productArity = 4

    def productElement(n: Int): Any = (n: @annotation.switch) match {
      case 0 => resource
      case 1 => password
      case 2 => storeType
      case _ => trust
    }

    override def equals(that: Any): Boolean = that match {
      case KeyStore(`resource`, Some(p), `storeType`, `trust`) =>
        password.exists(pwd => Arrays.equals(p, pwd))

      case KeyStore(`resource`, None, `storeType`, `trust`) =>
        password.isEmpty

      case _ => false
    }

    override def hashCode: Int =
      (resource, storeType, password.map(Arrays.hashCode(_))).hashCode

    override protected def finalize(): Unit = {
      password.foreach { p =>
        p.indices.foreach { p(_) = '\u0000' }
      }
    }
  }

  object KeyStore {
    @inline def apply(
      resource: URI,
      password: Option[Array[Char]],
      storeType: String): KeyStore = apply(resource, password, storeType, true)

    /**
     * @param resource the ressource to load as key store
     * @param password the password to load the store
     * @param storeType the type of the key store (e.g. `PKCS12`)
     * @param trust whether the store defines a certificate to be trusted
     */
    def apply(
      resource: URI,
      password: Option[Array[Char]],
      storeType: String,
      trust: Boolean): KeyStore =
      new KeyStore(resource, password, storeType, trust)

    def unapply(keyStore: KeyStore): Option[(URI, Option[Array[Char]], String, Boolean)] = Option(keyStore).map { ks =>
      Tuple4(ks.resource, ks.password, ks.storeType, ks.trust)
    }
  }

  // ---

  @inline private def ms(duration: Int): String = s"${duration}ms"

  private[reactivemongo] def toStrings(options: MongoConnectionOptions): List[(String, String)] = options.authenticationDatabase.toList.map(
    "authenticationDatabase" -> _.toString) ++ List(
      "authenticationMechanism" -> options.authMode.toString,
      "nbChannelsPerNode" -> options.nbChannelsPerNode.toString,
      "maxInFlightRequestsPerChannel" -> options.maxInFlightRequestsPerChannel.fold("<unlimited>")(_.toString),
      "heartbeatFrequencyMS" -> ms(options.heartbeatFrequencyMS),
      "connectTimeoutMS" -> ms(options.connectTimeoutMS),
      "maxIdleTimeMS" -> ms(options.maxIdleTimeMS), // TODO: Review
      "tcpNoDelay" -> options.tcpNoDelay.toString,
      "keepAlive" -> options.keepAlive.toString,
      "sslEnabled" -> options.sslEnabled.toString,
      "sslAllowsInvalidCert" -> options.sslAllowsInvalidCert.toString,
      "writeConcern" -> options.writeConcern.toString,
      "readPreference" -> options.readPreference.toString,
      "readConcern" -> options.readConcern.level)

}
