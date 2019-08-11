package reactivemongo.core.nodeset

sealed trait Authentication {
  def user: String
  def db: String
}

/**
 * @param db the name of the database
 * @param user the name (or subject for X509) of the user
 * @param password the password for the [[user]] (`None` for X509)
 */
@deprecated("Internal: will be made private", "0.14.0")
case class Authenticate(
  db: String,
  user: String,
  password: Option[String]) extends Authentication {

  override def toString = s"Authenticate($db, $user)"
}

sealed trait Authenticating extends Authentication {
  @deprecated("Will be removed", "0.14.0")
  def password: String
}

object Authenticating {
  def unapply(auth: Authenticating): Option[(String, String, Option[String])] =
    auth match {
      case CrAuthenticating(db, user, pass, _) =>
        Some((db, user, Some(pass)))

      case ScramAuthenticating(db, user, pass, _, _, _, _, _) =>
        Some((db, user, Some(pass)))

      case X509Authenticating(db, user) =>
        Some((db, user, Option.empty[String]))

      case _ =>
        None
    }
}

case class CrAuthenticating(db: String, user: String, password: String, nonce: Option[String]) extends Authenticating {
  override def toString: String =
    s"Authenticating($db, $user, ${nonce.map(_ => "<nonce>").getOrElse("<>")})"
}

sealed trait ScramAuthenticating extends Authenticating {
  def db: String
  def user: String
  def password: String
  def randomPrefix: String
  def saslStart: String
  def conversationId: Option[Int]
  def serverSignature: Option[Array[Byte]]
  def step: Int

  override def toString: String =
    s"Authenticating($db, $user})"

  def copy(
    db: String = this.db,
    user: String = this.user,
    password: String = this.password,
    randomPrefix: String = this.randomPrefix,
    saslStart: String = this.saslStart,
    conversationId: Option[Int] = this.conversationId,
    serverSignature: Option[Array[Byte]] = this.serverSignature,
    step: Int = this.step): ScramAuthenticating = ScramAuthenticating(db, user, password, randomPrefix, saslStart, conversationId, serverSignature, step)
}

private[reactivemongo] object ScramAuthenticating {
  def unapply(authing: ScramAuthenticating): Option[(String, String, String, String, String, Option[Int], Option[Array[Byte]], Int)] = Some((authing.db, authing.user, authing.password, authing.randomPrefix, authing.saslStart, authing.conversationId, authing.serverSignature, authing.step))

  def apply(
    db: String,
    user: String,
    password: String,
    randomPrefix: String,
    saslStart: String,
    conversationId: Option[Int] = None,
    serverSignature: Option[Array[Byte]] = None,
    step: Int = 0): ScramAuthenticating = Default(db, user, password, randomPrefix, saslStart, conversationId, serverSignature, step)

  private case class Default(
    db: String,
    user: String,
    password: String,
    randomPrefix: String,
    saslStart: String,
    conversationId: Option[Int],
    serverSignature: Option[Array[Byte]],
    step: Int) extends ScramAuthenticating
}

@deprecated("Use `ScramAuthenticating`", "0.18.4")
case class ScramSha1Authenticating(
  db: String,
  user: String,
  password: String,
  randomPrefix: String,
  saslStart: String,
  conversationId: Option[Int] = None,
  serverSignature: Option[Array[Byte]] = None,
  step: Int = 0) extends ScramAuthenticating

case class X509Authenticating(db: String, user: String) extends Authenticating {
  def password = "deprecated"
}

case class Authenticated(db: String, user: String) extends Authentication {
  val toShortString: String = s"${user}@${db}"
}
