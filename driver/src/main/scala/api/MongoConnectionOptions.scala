package reactivemongo.api

import java.net.URI

/**
 * Options for [[MongoConnection]] (see [[http://reactivemongo.org/releases/0.1x/documentation/tutorial/connect-database.html#connection-options more documentation]]).
 *
 * @param connectTimeoutMS The number of milliseconds to wait for a connection to be established before giving up.
 * @param authenticationDatabase the name of the database used for authentication
 * @param sslEnabled Enable SSL connection (required to be accepted on server-side).
 * @param sslAllowsInvalidCert If `sslEnabled` is true, this one indicates whether to accept invalid certificates (e.g. self-signed).
 * @param authenticationMechanism Either [[ScramSha1Authentication]] or [[X509Authentication]]
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
 * @param appName the application name (custom or default)
 */
final class MongoConnectionOptions private[reactivemongo] (
  // canonical options - connection
  val connectTimeoutMS: Int,

  // canonical options - authentication options
  val authenticationDatabase: Option[String],
  val sslEnabled: Boolean,
  val sslAllowsInvalidCert: Boolean,
  val authenticationMechanism: AuthenticationMode,

  // reactivemongo specific options
  val tcpNoDelay: Boolean,
  val keepAlive: Boolean,
  val nbChannelsPerNode: Int,
  val maxInFlightRequestsPerChannel: Option[Int],
  val minIdleChannelsPerNode: Int,

  // read and write preferences
  val writeConcern: WriteConcern,
  val readPreference: ReadPreference,

  val failoverStrategy: FailoverStrategy,

  val heartbeatFrequencyMS: Int,
  val maxIdleTimeMS: Int,
  val maxHistorySize: Int,
  val credentials: Map[String, MongoConnectionOptions.Credential],
  val keyStore: Option[MongoConnectionOptions.KeyStore],
  val readConcern: ReadConcern,

  val appName: Option[String]) {

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
    minIdleChannelsPerNode: Int = this.minIdleChannelsPerNode,
    writeConcern: WriteConcern = this.writeConcern,
    readPreference: ReadPreference = this.readPreference,
    failoverStrategy: FailoverStrategy = this.failoverStrategy,
    heartbeatFrequencyMS: Int = this.heartbeatFrequencyMS,
    maxIdleTimeMS: Int = this.maxIdleTimeMS,
    maxHistorySize: Int = this.maxHistorySize,
    credentials: Map[String, MongoConnectionOptions.Credential] = this.credentials,
    keyStore: Option[MongoConnectionOptions.KeyStore] = this.keyStore,
    readConcern: ReadConcern = this.readConcern,
    appName: Option[String] = this.appName): MongoConnectionOptions =
    new MongoConnectionOptions(
      connectTimeoutMS = connectTimeoutMS,
      authenticationDatabase = authenticationDatabase,
      sslEnabled = sslEnabled,
      sslAllowsInvalidCert = sslAllowsInvalidCert,
      authenticationMechanism = authenticationMechanism,
      tcpNoDelay = tcpNoDelay,
      keepAlive = keepAlive,
      nbChannelsPerNode = nbChannelsPerNode,
      maxInFlightRequestsPerChannel = maxInFlightRequestsPerChannel,
      minIdleChannelsPerNode = minIdleChannelsPerNode,
      writeConcern = writeConcern,
      readPreference = readPreference,
      failoverStrategy = failoverStrategy,
      heartbeatFrequencyMS = heartbeatFrequencyMS,
      maxIdleTimeMS = maxIdleTimeMS,
      maxHistorySize = maxHistorySize,
      credentials = credentials,
      keyStore = keyStore,
      readConcern = readConcern,
      appName = appName)

  override def toString = s"""MongoConnectionOptions { ${MongoConnectionOptions.toStrings(this).map { case (k, v) => k + ": " + v }.mkString(", ")} }"""

  override def hashCode: Int = tupled.hashCode

  override def equals(that: Any): Boolean = that match {
    case other: MongoConnectionOptions =>
      other.tupled == this.tupled

    case _ =>
      false
  }

  private[api] lazy val tupled = Tuple20(
    connectTimeoutMS,
    authenticationDatabase,
    sslEnabled,
    sslAllowsInvalidCert,
    authenticationMechanism,
    tcpNoDelay,
    keepAlive,
    nbChannelsPerNode,
    maxInFlightRequestsPerChannel,
    minIdleChannelsPerNode,
    writeConcern,
    readPreference,
    failoverStrategy,
    heartbeatFrequencyMS,
    maxIdleTimeMS,
    maxHistorySize,
    credentials,
    keyStore,
    readConcern,
    appName)

}

/**
 * [[MongoConnectionOptions]] factory.
 *
 * {{{
 * reactivemongo.api.MongoConnectionOptions(nbChannelsPerNode = 10)
 * }}}
 */
object MongoConnectionOptions {
  /** The default options */
  @inline def default: MongoConnectionOptions = MongoConnectionOptions()

  def apply(
    connectTimeoutMS: Int = 0,
    authenticationDatabase: Option[String] = None,
    sslEnabled: Boolean = false,
    sslAllowsInvalidCert: Boolean = false,
    authenticationMechanism: AuthenticationMode = ScramSha1Authentication,
    tcpNoDelay: Boolean = false,
    keepAlive: Boolean = false,
    nbChannelsPerNode: Int = 10,
    maxInFlightRequestsPerChannel: Option[Int] = Some(200),
    minIdleChannelsPerNode: Int = 1,
    writeConcern: WriteConcern = WriteConcern.Default,
    readPreference: ReadPreference = ReadPreference.primary,
    failoverStrategy: FailoverStrategy = FailoverStrategy.default,
    heartbeatFrequencyMS: Int = 10000,
    maxIdleTimeMS: Int = 0,
    maxHistorySize: Int = 25,
    credentials: Map[String, MongoConnectionOptions.Credential] = Map.empty,
    keyStore: Option[MongoConnectionOptions.KeyStore] = Option.empty,
    readConcern: ReadConcern = ReadConcern.default,
    appName: Option[String] = None): MongoConnectionOptions =
    new MongoConnectionOptions(
      connectTimeoutMS = connectTimeoutMS,
      authenticationDatabase = authenticationDatabase,
      sslEnabled = sslEnabled,
      sslAllowsInvalidCert = sslAllowsInvalidCert,
      authenticationMechanism = authenticationMechanism,
      tcpNoDelay = tcpNoDelay,
      keepAlive = keepAlive,
      nbChannelsPerNode = nbChannelsPerNode,
      maxInFlightRequestsPerChannel = maxInFlightRequestsPerChannel,
      minIdleChannelsPerNode = minIdleChannelsPerNode,
      writeConcern = writeConcern,
      readPreference = readPreference,
      failoverStrategy = failoverStrategy,
      heartbeatFrequencyMS = heartbeatFrequencyMS,
      maxIdleTimeMS = maxIdleTimeMS,
      maxHistorySize = maxHistorySize,
      credentials = credentials,
      keyStore = keyStore,
      readConcern = readConcern,
      appName = appName)

  // ---

  /**
   * @param user the name (or subject) of the user
   * @param password the associated password if some
   */
  final class Credential private[api] (
    val user: String,
    val password: Option[String]) {

    private[api] lazy val tupled = user -> password

    override def equals(that: Any): Boolean = that match {
      case other: Credential =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"Credential${tupled.toString}"
  }

  object Credential {
    def apply(user: String, password: Option[String]): Credential =
      new Credential(user, password)

  }

  final class KeyStore(
    val resource: URI,
    val password: Option[Array[Char]],
    val storeType: String,
    val trust: Boolean) {

    override def toString = s"KeyStore#${storeType}{$resource}"

    import java.util.Arrays

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
      trust: Boolean = true): KeyStore =
      new KeyStore(resource, password, storeType, trust)

    private[api] def unapply(keyStore: KeyStore): Option[(URI, Option[Array[Char]], String, Boolean)] = Option(keyStore).map { ks =>
      Tuple4(ks.resource, ks.password, ks.storeType, ks.trust)
    }
  }

  // ---

  @inline private def ms(duration: Int): String = s"${duration}ms"

  private[reactivemongo] def toStrings(options: MongoConnectionOptions): List[(String, String)] = options.authenticationDatabase.toList.map(
    "authenticationDatabase" -> _.toString) ++ List(
      "appName" -> options.appName.getOrElse("<undefined>"),
      "authenticationMechanism" -> options.authenticationMechanism.toString,
      "nbChannelsPerNode" -> options.nbChannelsPerNode.toString,
      "maxInFlightRequestsPerChannel" -> options.maxInFlightRequestsPerChannel.fold("<unlimited>")(_.toString),
      "minIdleChannelsPerNode" -> options.minIdleChannelsPerNode.toString,
      "heartbeatFrequencyMS" -> ms(options.heartbeatFrequencyMS),
      "connectTimeoutMS" -> ms(options.connectTimeoutMS),
      "maxIdleTimeMS" -> ms(options.maxIdleTimeMS),
      "tcpNoDelay" -> options.tcpNoDelay.toString,
      "keepAlive" -> options.keepAlive.toString,
      "sslEnabled" -> options.sslEnabled.toString,
      "sslAllowsInvalidCert" -> options.sslAllowsInvalidCert.toString,
      "writeConcern" -> options.writeConcern.toString,
      "readPreference" -> options.readPreference.toString,
      "readConcern" -> options.readConcern.level)

}
