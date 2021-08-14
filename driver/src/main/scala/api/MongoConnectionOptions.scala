package reactivemongo.api

import java.net.URI

// TODO: Make sure documentation of param properties is visible
/**
 * Options for [[MongoConnection]] (see [[http://reactivemongo.org/releases/0.1x/documentation/tutorial/connect-database.html#connection-options more documentation]]).
 */
@SuppressWarnings(Array("VariableShadowing"))
final class MongoConnectionOptions private[reactivemongo] (
  // canonical options - connection
  _connectTimeoutMS: Int,

  // canonical options - authentication options
  _authenticationDatabase: Option[String],
  _sslEnabled: Boolean,
  _sslAllowsInvalidCert: Boolean,
  _authenticationMechanism: AuthenticationMode,

  // reactivemongo specific options
  _tcpNoDelay: Boolean,
  _keepAlive: Boolean,
  _nbChannelsPerNode: Int,
  _maxInFlightRequestsPerChannel: Option[Int],
  _minIdleChannelsPerNode: Int,

  // read and write preferences
  _writeConcern: WriteConcern,
  _readPreference: ReadPreference,

  _failoverStrategy: FailoverStrategy,

  _heartbeatFrequencyMS: Int,
  _maxIdleTimeMS: Int,
  _maxNonQueryableHeartbeats: Int,
  _maxHistorySize: Int,
  _credentials: Map[String, MongoConnectionOptions.Credential],
  _keyStore: Option[MongoConnectionOptions.KeyStore],
  _readConcern: ReadConcern,

  _appName: Option[String],
  _compressors: Seq[Compressor] /* TODO: Parse */ ) {

  /**
   * The number of milliseconds to wait for a connection
   * to be established before giving up.
   */
  @inline def connectTimeoutMS: Int = _connectTimeoutMS

  // --- Canonical options - authentication options

  /** The name of the database used for authentication */
  @inline def authenticationDatabase: Option[String] = _authenticationDatabase

  /** Enable SSL connection (required to be accepted on server-side) */
  @inline def sslEnabled: Boolean = _sslEnabled

  /**
   * If `sslEnabled` is true, this one indicates whether
   * to accept invalid certificates (e.g. self-signed).
   */
  @inline def sslAllowsInvalidCert: Boolean = _sslAllowsInvalidCert

  /** Either [[ScramSha1Authentication]] or [[X509Authentication]] */
  @inline def authenticationMechanism: AuthenticationMode =
    _authenticationMechanism

  /** An optional [[https://docs.mongodb.com/manual/reference/connection-string/#urioption.appName application name]] */
  @inline def appName: Option[String] = _appName

  /** The list of allowed [[https://docs.mongodb.com/manual/reference/connection-string/#mongodb-urioption-urioption.compressors compressors]] (e.g. `snappy`) */
  @inline def compressors: Seq[Compressor] = _compressors

  // --- Reactivemongo specific options

  /**
   * TCP NoDelay flag (ReactiveMongo-specific option).
   * The default value is false (see [[http://docs.oracle.com/javase/8/docs/api/java/net/StandardSocketOptions.html#TCP_NODELAY TCP_NODELAY]]).
   */
  @inline def tcpNoDelay: Boolean = _tcpNoDelay

  /**
   * TCP KeepAlive flag (ReactiveMongo-specific option).
   * The default value is false (see [[http://docs.oracle.com/javase/8/docs/api/java/net/StandardSocketOptions.html#SO_KEEPALIVE SO_KEEPALIVE]]).
   */
  @inline def keepAlive: Boolean = _keepAlive

  /**
   * The number of channels (connections)
   * per node (ReactiveMongo-specific option)
   */
  @inline def nbChannelsPerNode: Int = _nbChannelsPerNode

  /** The maximum number of requests processed per channel */
  @inline def maxInFlightRequestsPerChannel: Option[Int] =
    _maxInFlightRequestsPerChannel

  /** The minimum number of idle channels per node */
  @inline def minIdleChannelsPerNode: Int = _minIdleChannelsPerNode

  // --- Read and write preferences

  /** The default [[https://docs.mongodb.com/manual/reference/write-concern/ write concern]] */
  @inline def writeConcern: WriteConcern = _writeConcern

  /** The default read preference */
  @inline def readPreference: ReadPreference = _readPreference

  /** The default failover strategy */
  @inline def failoverStrategy: FailoverStrategy = _failoverStrategy

  /**
   * The interval in milliseconds used by monitor to refresh the node set
   * (default: 10000 aka 10s)
   */
  @inline def heartbeatFrequencyMS: Int = _heartbeatFrequencyMS

  /**
   * The maximum number of milliseconds that a [[https://docs.mongodb.com/manual/reference/connection-string/#urioption.maxIdleTimeMS channel can remain idle]] in the connection pool before being removed and closed (default: 0 to disable, as implemented using [[http://netty.io/4.1/api/io/netty/handler/timeout/IdleStateHandler.html Netty IdleStateHandler]]); If not 0, must be greater or equal to [[heartbeatFrequencyMS]].
   */
  @inline def maxIdleTimeMS: Int = _maxIdleTimeMS

  /** The maximum size of the pool history (default: 25) */
  @inline def maxHistorySize: Int = _maxHistorySize

  /** The credentials per authentication database names */
  @inline def credentials: Map[String, MongoConnectionOptions.Credential] =
    _credentials

  /** An optional key store */
  @inline def keyStore: Option[MongoConnectionOptions.KeyStore] = _keyStore

  /** The default [[https://docs.mongodb.com/manual/reference/read-concern/ read concern]] */
  @inline def readConcern: ReadConcern = _readConcern

  /** '''EXPERIMENTAL:''' */
  def maxNonQueryableHeartbeats = _maxNonQueryableHeartbeats

  private[reactivemongo] def withMaxNonQueryableHeartbeats(max: Int) =
    new MongoConnectionOptions(
      _connectTimeoutMS = this.connectTimeoutMS,
      _authenticationDatabase = this.authenticationDatabase,
      _sslEnabled = this.sslEnabled,
      _sslAllowsInvalidCert = this.sslAllowsInvalidCert,
      _authenticationMechanism = this.authenticationMechanism,
      _tcpNoDelay = this.tcpNoDelay,
      _keepAlive = this.keepAlive,
      _nbChannelsPerNode = this.nbChannelsPerNode,
      _maxInFlightRequestsPerChannel = this.maxInFlightRequestsPerChannel,
      _minIdleChannelsPerNode = this.minIdleChannelsPerNode,
      _writeConcern = this.writeConcern,
      _readPreference = this.readPreference,
      _failoverStrategy = this.failoverStrategy,
      _heartbeatFrequencyMS = this.heartbeatFrequencyMS,
      _maxIdleTimeMS = this.maxIdleTimeMS,
      _maxNonQueryableHeartbeats = max,
      _maxHistorySize = this.maxHistorySize,
      _credentials = this.credentials,
      _keyStore = this.keyStore,
      _readConcern = this.readConcern,
      _appName = this.appName,
      _compressors = this.compressors)

  def withCompressors(compressors: Seq[Compressor]) =
    new MongoConnectionOptions(
      _connectTimeoutMS = this.connectTimeoutMS,
      _authenticationDatabase = this.authenticationDatabase,
      _sslEnabled = this.sslEnabled,
      _sslAllowsInvalidCert = this.sslAllowsInvalidCert,
      _authenticationMechanism = this.authenticationMechanism,
      _tcpNoDelay = this.tcpNoDelay,
      _keepAlive = this.keepAlive,
      _nbChannelsPerNode = this.nbChannelsPerNode,
      _maxInFlightRequestsPerChannel = this.maxInFlightRequestsPerChannel,
      _minIdleChannelsPerNode = this.minIdleChannelsPerNode,
      _writeConcern = this.writeConcern,
      _readPreference = this.readPreference,
      _failoverStrategy = this.failoverStrategy,
      _heartbeatFrequencyMS = this.heartbeatFrequencyMS,
      _maxIdleTimeMS = this.maxIdleTimeMS,
      _maxNonQueryableHeartbeats = this._maxNonQueryableHeartbeats, // TODO
      _maxHistorySize = this.maxHistorySize,
      _credentials = this.credentials,
      _keyStore = this.keyStore,
      _readConcern = this.readConcern,
      _appName = this.appName,
      _compressors = compressors)

  // ---

  @SuppressWarnings(Array("MaxParameters"))
  def copy( // TODO: Add _maxNonQueryableHeartbeats, _compressors
    connectTimeoutMS: Int = _connectTimeoutMS,
    authenticationDatabase: Option[String] = _authenticationDatabase,
    sslEnabled: Boolean = _sslEnabled,
    sslAllowsInvalidCert: Boolean = _sslAllowsInvalidCert,
    authenticationMechanism: AuthenticationMode = _authenticationMechanism,
    tcpNoDelay: Boolean = _tcpNoDelay,
    keepAlive: Boolean = _keepAlive,
    nbChannelsPerNode: Int = _nbChannelsPerNode,
    maxInFlightRequestsPerChannel: Option[Int] = _maxInFlightRequestsPerChannel,
    minIdleChannelsPerNode: Int = _minIdleChannelsPerNode,
    writeConcern: WriteConcern = _writeConcern,
    readPreference: ReadPreference = _readPreference,
    failoverStrategy: FailoverStrategy = _failoverStrategy,
    heartbeatFrequencyMS: Int = _heartbeatFrequencyMS,
    maxIdleTimeMS: Int = _maxIdleTimeMS,
    maxHistorySize: Int = _maxHistorySize,
    credentials: Map[String, MongoConnectionOptions.Credential] = _credentials,
    keyStore: Option[MongoConnectionOptions.KeyStore] = _keyStore,
    readConcern: ReadConcern = _readConcern,
    appName: Option[String] = _appName): MongoConnectionOptions =
    new MongoConnectionOptions(
      _connectTimeoutMS = connectTimeoutMS,
      _authenticationDatabase = authenticationDatabase,
      _sslEnabled = sslEnabled,
      _sslAllowsInvalidCert = sslAllowsInvalidCert,
      _authenticationMechanism = authenticationMechanism,
      _tcpNoDelay = tcpNoDelay,
      _keepAlive = keepAlive,
      _nbChannelsPerNode = nbChannelsPerNode,
      _maxInFlightRequestsPerChannel = maxInFlightRequestsPerChannel,
      _minIdleChannelsPerNode = minIdleChannelsPerNode,
      _writeConcern = writeConcern,
      _readPreference = readPreference,
      _failoverStrategy = failoverStrategy,
      _heartbeatFrequencyMS = heartbeatFrequencyMS,
      _maxIdleTimeMS = maxIdleTimeMS,
      _maxNonQueryableHeartbeats = this._maxNonQueryableHeartbeats, // TODO
      _maxHistorySize = maxHistorySize,
      _credentials = credentials,
      _keyStore = keyStore,
      _readConcern = readConcern,
      _appName = appName,
      _compressors = this._compressors)

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

  @SuppressWarnings(Array("MaxParameters"))
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
      _connectTimeoutMS = connectTimeoutMS,
      _authenticationDatabase = authenticationDatabase,
      _sslEnabled = sslEnabled,
      _sslAllowsInvalidCert = sslAllowsInvalidCert,
      _authenticationMechanism = authenticationMechanism,
      _tcpNoDelay = tcpNoDelay,
      _keepAlive = keepAlive,
      _nbChannelsPerNode = nbChannelsPerNode,
      _maxInFlightRequestsPerChannel = maxInFlightRequestsPerChannel,
      _minIdleChannelsPerNode = minIdleChannelsPerNode,
      _writeConcern = writeConcern,
      _readPreference = readPreference,
      _failoverStrategy = failoverStrategy,
      _heartbeatFrequencyMS = heartbeatFrequencyMS,
      _maxIdleTimeMS = maxIdleTimeMS,
      _maxNonQueryableHeartbeats = 30, // TODO
      _maxHistorySize = maxHistorySize,
      _credentials = credentials,
      _keyStore = keyStore,
      _readConcern = readConcern,
      _appName = appName,
      _compressors = Seq.empty /* TODO */ )

  // ---

  /**
   * Connection credentials
   * @see [[MongoConnectionOptions]]
   */
  final class Credential private[api] (
    _user: String,
    _password: Option[String]) {

    /** The name (or subject) of the user */
    @inline def user: String = _user

    /** The associated password if some */
    @inline def password: Option[String] = _password

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

  /** [[Credential]] factory */
  object Credential {
    /**
     * Prepares credentials for [[MongoConnectionOptions]] usage
     */
    def apply(user: String, password: Option[String]): Credential =
      new Credential(user, password)

  }

  /**
   * Connection key store
   * @see [[MongoConnectionOptions]]
   */
  final class KeyStore private[api] (
    _resource: URI,
    _password: Option[Array[Char]],
    _storeType: String,
    _trust: Boolean) {

    /** The resource URI for this key store */
    @inline def resource: URI = _resource

    /** An optional password to load this store */
    @inline def password: Option[Array[Char]] = _password

    /** The store type (e.g. PKCS12) */
    @inline def storeType: String = _storeType

    /** The flag to indicate whether to trust this store */
    @inline def trust: Boolean = _trust

    override def toString = s"KeyStore#${storeType}{$resource}"

    import java.util.Arrays

    override def equals(that: Any): Boolean = that match {
      case KeyStore(`_resource`, Some(p), `_storeType`, `_trust`) =>
        password.exists(pwd => Arrays.equals(p, pwd))

      case KeyStore(`_resource`, None, `_storeType`, `_trust`) =>
        password.isEmpty

      case _ => false
    }

    override def hashCode: Int =
      (resource, storeType, password.map(Arrays.hashCode(_))).hashCode

    @SuppressWarnings(Array("RedundantFinalizer"))
    @com.github.ghik.silencer.silent("finalize\\ .*deprecated")
    override protected def finalize(): Unit = {
      password.foreach { p =>
        p.indices.foreach { p(_) = '\u0000' }
      }

      super.finalize()
    }
  }

  /** [[KeyStore]] factory */
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
    "authenticationDatabase" -> _) ++ List(
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
      "readConcern" -> options.readConcern.level,
      "compressors" -> options.compressors.mkString("[", ", ", "]"))

}
