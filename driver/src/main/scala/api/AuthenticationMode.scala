package reactivemongo.api

/** Then mode of authentication against the replica set. */
sealed trait AuthenticationMode {
  def name: String

  final override def toString = name
}

private[reactivemongo] object AuthenticationMode {
  type Scram = AuthenticationMode with ScramAuthentication with Singleton
}

/** MongoDB-CR authentication */
case object CrAuthentication extends AuthenticationMode {
  val name = "CR"
}

// SCRAM

private[reactivemongo] sealed trait ScramAuthentication {
  _: AuthenticationMode =>
}

/** SCRAM-SHA-1 authentication (since MongoDB 3.6) */
case object ScramSha1Authentication
  extends AuthenticationMode with ScramAuthentication {

  val name = "SCRAM-SHA-1"
}

/** SCRAM-SHA-256 authentication (see MongoDB 4.0) */
case object ScramSha256Authentication
  extends AuthenticationMode with ScramAuthentication {

  val name = "SCRAM-SHA-256"
}

// ---

/** X509 authentication */
case object X509Authentication extends AuthenticationMode {
  val name = "X509"
}
