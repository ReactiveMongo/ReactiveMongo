package reactivemongo.core.nodeset

import scala.language.higherKinds

sealed trait Authentication {
  def user: String
  def db: String
}

/**
 * @param db the name of the database
 * @param user the name of the user
 * @param password the password for the [[user]]
 */
case class Authenticate(
  db: String,
  user: String,
  password: String) extends Authentication {

  override def toString = s"Authenticate($db, $user)"
}

sealed trait Authenticating extends Authentication {
  def password: String
}

object Authenticating {
  @deprecated(message = "Use [[reactivemongo.core.nodeset.CrAuthenticating]]", since = "0.11.10")
  def apply(db: String, user: String, password: String, nonce: Option[String]): Authenticating = CrAuthenticating(db, user, password, nonce)

  def unapply(auth: Authenticating): Option[(String, String, String)] =
    auth match {
      case CrAuthenticating(db, user, pass, _) =>
        Some((db, user, pass))

      case ScramSha1Authenticating(db, user, pass, _, _, _, _, _) =>
        Some((db, user, pass))

      case _ =>
        None
    }
}

case class CrAuthenticating(db: String, user: String, password: String, nonce: Option[String]) extends Authenticating {
  override def toString: String =
    s"Authenticating($db, $user, ${nonce.map(_ => "<nonce>").getOrElse("<>")})"
}

case class ScramSha1Authenticating(
  db: String, user: String, password: String,
  randomPrefix: String, saslStart: String,
  conversationId: Option[Int] = None,
  serverSignature: Option[Array[Byte]] = None,
  step: Int = 0) extends Authenticating {

  override def toString: String =
    s"Authenticating($db, $user})"
}

case class Authenticated(db: String, user: String) extends Authentication
