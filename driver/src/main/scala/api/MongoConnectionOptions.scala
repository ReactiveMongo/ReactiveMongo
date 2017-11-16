package reactivemongo.api

import reactivemongo.api.commands.WriteConcern

/** Then mode of authentication against the replica set. */
sealed trait AuthenticationMode

/** MongoDB-CR authentication */
case object CrAuthentication extends AuthenticationMode

/** SCRAM-SHA-1 authentication (see MongoDB 3.0) */
case object ScramSha1Authentication extends AuthenticationMode

/** X509 authentication */
case object X509Authentication extends AuthenticationMode

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
 * @param writeConcern the default write concern
 * @param readPreference the default read preference
 * @param failoverStrategy the default failover strategy
 * @param monitorRefreshMS the interval in milliseconds used by monitor to refresh the node set (default: 10000 aka 10s)
 * @param maxIdleTimeMS the maximum number of milliseconds that a [[https://docs.mongodb.com/manual/reference/connection-string/#urioption.maxIdleTimeMS channel can remain idle]] in the connection pool before being removed and closed (default: 0 to disable, as implemented using [[http://netty.io/4.1/api/io/netty/handler/timeout/IdleStateHandler.html Netty IdleStateHandler]]); If not 0, must be greater or equal to [[#monitorRefreshMS]]
 */
case class MongoConnectionOptions(
  // canonical options - connection
  connectTimeoutMS: Int = 0,
  // canonical options - authentication options
  @deprecatedName('authSource) authenticationDatabase: Option[String] = None,
  sslEnabled: Boolean = false,
  sslAllowsInvalidCert: Boolean = false,
  authMode: AuthenticationMode = ScramSha1Authentication,

  // reactivemongo specific options
  tcpNoDelay: Boolean = false,
  keepAlive: Boolean = false,
  nbChannelsPerNode: Int = 10,

  // read and write preferences
  writeConcern: WriteConcern = WriteConcern.Default,
  readPreference: ReadPreference = ReadPreference.primary,

  failoverStrategy: FailoverStrategy = FailoverStrategy.default,

  monitorRefreshMS: Int = 10000,
  maxIdleTimeMS: Int = 0) {

  /**
   * The database source for authentication credentials (corresponds to the `authenticationDatabase` with MongoShell).
   */
  @deprecated("Use [[authenticationDatabase]]", "0.12.7")
  def authSource: Option[String] = authenticationDatabase
}

object MongoConnectionOptions {
  @inline private def ms(duration: Int): String = s"${duration}ms"

  private[reactivemongo] def toStrings(options: MongoConnectionOptions): List[(String, String)] = options.authenticationDatabase.toList.map(
    "authenticationDatabase" -> _.toString) ++ List(
      "authMode" -> options.authMode.toString,
      "nbChannelsPerNode" -> options.nbChannelsPerNode.toString,
      "monitorRefreshMS" -> ms(options.monitorRefreshMS),
      "connectTimeoutMS" -> ms(options.connectTimeoutMS),
      "maxIdleTimeMS" -> ms(options.maxIdleTimeMS), // TODO: Review
      "tcpNoDelay" -> options.tcpNoDelay.toString,
      "keepAlive" -> options.keepAlive.toString,
      "sslEnabled" -> options.sslEnabled.toString,
      "sslAllowsInvalidCert" -> options.sslAllowsInvalidCert.toString,
      "writeConcern" -> options.writeConcern.toString,
      "readPreference" -> options.readPreference.toString)

}
