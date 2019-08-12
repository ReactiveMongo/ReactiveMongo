package reactivemongo.core.commands

import reactivemongo.bson.{
  BSONDocument,
  BSONInteger,
  BSONNumberLike,
  BSONString
}
import reactivemongo.core.protocol.Response

// --- MongoDB CR authentication ---

/**
 * Getnonce Command for Mongo CR authentication.
 *
 * Gets a nonce for authentication token.
 */
object GetCrNonce extends Command[String] {
  override def makeDocuments = BSONDocument("getnonce" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[String] {
    def apply(document: BSONDocument) =
      CommandError.checkOk(document, Some("getnonce")).
        toLeft(document.getAs[BSONString]("nonce").get.value)
  }
}

/**
 * Mongo CR authenticate Command.
 *
 * @param user username
 * @param password user's password
 * @param nonce the previous nonce given by the server
 */
private[core] case class CrAuthenticate(
  user: String, password: String, nonce: String) extends Command[SuccessfulAuthentication] {
  import reactivemongo.bson.utils.Converters._

  /** the computed digest of the password */
  lazy val pwdDigest = md5Hex(s"$user:mongo:$password", "UTF-8")

  /** the digest of the tuple (''nonce'', ''user'', ''pwdDigest'') */
  lazy val key = md5Hex(nonce + user + pwdDigest, "UTF-8")

  override def makeDocuments = BSONDocument(
    "authenticate" -> BSONInteger(1),
    "user" -> BSONString(user),
    "nonce" -> BSONString(nonce),
    "key" -> BSONString(key))

  val ResultMaker = CrAuthenticate
}

/** Authentication command's response deserializer. */
object CrAuthenticate extends BSONCommandResultMaker[SuccessfulAuthentication] {
  def parseResponse(response: Response): Either[CommandError, SuccessfulAuthentication] = apply(response)

  def apply(document: BSONDocument) = {
    CommandError.checkOk(document, Some("authenticate"), (doc, _) => {
      FailedAuthentication(
        doc.getAs[BSONString]("errmsg").fold("")(_.value), Some(doc))

    }).toLeft(document.get("dbname") match {
      case Some(BSONString(dbname)) => VerboseSuccessfulAuthentication(
        dbname,
        document.getAs[String]("user").get,
        document.getAs[Boolean]("readOnly").getOrElse(false))

      case _ => SilentSuccessfulAuthentication
    })
  }
}

private[core] case class X509Authenticate(user: Option[String])
  extends Command[SuccessfulAuthentication] {

  private val userNameDocument = user.fold(BSONDocument.empty) { name =>
    BSONDocument("user" -> BSONString(name))
  }

  override def makeDocuments = BSONDocument(
    "authenticate" -> BSONInteger(1),
    "mechanism" -> BSONString("MONGODB-X509")) ++ userNameDocument

  override val ResultMaker = X509Authenticate
}

object X509Authenticate extends BSONCommandResultMaker[SuccessfulAuthentication] {
  def parseResponse(response: Response): Either[CommandError, SuccessfulAuthentication] = apply(response)

  def apply(document: BSONDocument) = {
    CommandError.checkOk(document, Some("authenticate"), (doc, _) => {
      FailedAuthentication(doc.getAs[BSONString]("errmsg").map(_.value).getOrElse(""), Some(doc))
    }).toLeft(SilentSuccessfulAuthentication)
  }
}

/** an authentication result */
sealed trait AuthenticationResult

/** A successful authentication result. */
sealed trait SuccessfulAuthentication extends AuthenticationResult

/** A silent successful authentication result (MongoDB <= 2.0).*/
object SilentSuccessfulAuthentication extends SuccessfulAuthentication

/**
 * A verbose successful authentication result (MongoDB >= 2.2).
 *
 * Previous versions of MongoDB only return ok = BSONDouble(1.0).
 *
 * @param db the database name
 * @param user the user name
 * @param readOnly states if the authentication gives us only the right to read from the database.
 */
case class VerboseSuccessfulAuthentication(
  db: String,
  user: String,
  readOnly: Boolean) extends SuccessfulAuthentication

/**
 * A failed authentication result
 *
 * @param message the explanation of the error
 */
case class FailedAuthentication(
  message: String,
  originalDocument: Option[BSONDocument] = None) extends BSONCommandError with AuthenticationResult {
  val code = None
}
