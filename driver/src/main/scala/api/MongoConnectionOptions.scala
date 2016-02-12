package reactivemongo.api

import reactivemongo.api.commands.WriteConcern

/** Then mode of authentication against the replica set. */
sealed trait AuthenticationMode

/** MongoDB-CR authentication */
case object CrAuthentication extends AuthenticationMode

/** SCRAM-SHA-1 authentication (see MongoDB 3.0) */
case object ScramSha1Authentication extends AuthenticationMode

/**
 * Options for MongoConnection.
 *
 * @param connectTimeoutMS The number of milliseconds to wait for a connection to be established before giving up.
 * @param authSource The database source for authentication credentials.
 * @param sslEnabled Enable SSL connection (required to be accepted on server-side).
 * @param sslAllowsInvalidCert If `sslEnabled` is true, this one indicates whether to accept invalid certificates (e.g. self-signed).
 * @param authMode Either [[CrAuthentication]] or [[ScramSha1Authentication]]
 * @param tcpNoDelay TCPNoDelay flag (ReactiveMongo-specific option). The default value is false (see [[http://docs.oracle.com/javase/8/docs/api/java/net/StandardSocketOptions.html#TCP_NODELAY TCP_NODELAY]]).
 * @param keepAlive TCP KeepAlive flag (ReactiveMongo-specific option). The default value is false (see [[http://docs.oracle.com/javase/8/docs/api/java/net/StandardSocketOptions.html#SO_KEEPALIVE SO_KEEPALIVE]]).
 * @param nbChannelsPerNode Number of channels (connections) per node (ReactiveMongo-specific option).
 * @param writeConcern the default write concern
 * @param readPreference the default read preference
 * @param socketTimeoutMS The number of milliseconds a socket can be idle before it is closed
 */
case class MongoConnectionOptions(
  // canonical options - connection
  connectTimeoutMS: Int = 0,
  // canonical options - authentication options
  authSource: Option[String] = None,
  sslEnabled: Boolean = false,
  sslAllowsInvalidCert: Boolean = false,
  authMode: AuthenticationMode = CrAuthentication,

  // reactivemongo specific options
  tcpNoDelay: Boolean = false,
  keepAlive: Boolean = false,
  nbChannelsPerNode: Int = 10,

  // read and write preferences
  writeConcern: WriteConcern = WriteConcern.Default,
  readPreference: ReadPreference = ReadPreference.primary,
  socketTimeoutMS: Int = 0)
