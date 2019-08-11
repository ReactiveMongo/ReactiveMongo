package reactivemongo.core.commands

import javax.crypto.SecretKeyFactory

import reactivemongo.api.{
  AuthenticationMode,
  ScramSha1Authentication
}

import reactivemongo.bson.{
  BSONBinary,
  BSONDocument,
  BSONInteger,
  BSONNumberLike,
  BSONString,
  Subtype
}
import reactivemongo.util
import reactivemongo.core.protocol.Response

// --- MongoDB SCRAM authentication ---

private[core] sealed trait ScramChallenge[M <: AuthenticationMode.Scram] {
  protected def mechanism: M

  def conversationId: Int
  def payload: Array[Byte]

  override def toString = s"ScramChallenge(${mechanism}, $conversationId)"
}

private[core] case class ScramSha1Challenge(
  conversationId: Int,
  payload: Array[Byte]) extends ScramChallenge[ScramSha1Authentication.type] {

  val mechanism = ScramSha1Authentication
}

/**
 * Command to start with Mongo SCRAM authentication.
 */
private[core] sealed trait ScramInitiate[M <: AuthenticationMode.Scram] extends Command[ScramChallenge[M]] {

  /** The user name */
  def user: String

  protected def mechanism: M

  val randomPrefix = ScramInitiate.randomPrefix(this.hashCode)

  import akka.util.ByteString
  import reactivemongo.bson.buffer.ArrayReadableBuffer

  /** Initial SCRAM message */
  val message: String = {
    val preparedUsername =
      s"""n=${user.replace("=", "=3D").replace(",", "=2D")}"""

    s"n,,$preparedUsername,r=$randomPrefix"
  }

  override def makeDocuments = BSONDocument(
    "saslStart" -> 1,
    "mechanism" -> mechanism.name,
    "payload" -> BSONBinary(
      ArrayReadableBuffer(ByteString(message).toArray[Byte]),
      Subtype.GenericBinarySubtype))

  val ResultMaker: BSONCommandResultMaker[ScramChallenge[M]]
}

private[core] case class ScramSha1Initiate(
  user: String) extends ScramInitiate[ScramSha1Authentication.type] {
  val mechanism = ScramSha1Authentication
  val ResultMaker = ScramSha1Initiate
}

@deprecated("Will be private", "0.18.4")
object ScramSha1Initiate extends BSONCommandResultMaker[ScramChallenge[ScramSha1Authentication.type]] {
  def parseResponse(response: Response): Either[CommandError, ScramChallenge[ScramSha1Authentication.type]] = apply(response)

  def apply(bson: BSONDocument) = ScramInitiate.parseResponse(ScramSha1Authentication, bson)(ScramSha1Challenge.apply)

  @inline def authChars: Stream[Char] = ScramInitiate.authChars

  @inline def randomPrefix(seed: Int): String = ScramInitiate.randomPrefix(seed)
}

private[core] object ScramInitiate {
  def parseResponse[M <: AuthenticationMode.Scram](mechanism: M, bson: BSONDocument)(f: (Int, Array[Byte]) => ScramChallenge[M]): Either[CommandError, ScramChallenge[M]] = {
    CommandError.checkOk(bson, Some("authenticate"), (doc, _) => {
      FailedAuthentication(
        doc.getAs[BSONString]("errmsg").fold("")(_.value), Some(doc))
    }).map[Either[CommandError, ScramChallenge[M]]](Left(_)).getOrElse {
      (for {
        cid <- bson.getAs[BSONNumberLike]("conversationId").map(_.toInt)
        pay <- bson.getAs[Array[Byte]]("payload")
      } yield (cid, pay)).fold[Either[CommandError, ScramChallenge[M]]](Left(FailedAuthentication(s"invalid $mechanism challenge response: ${BSONDocument pretty bson}", Some(bson)))) {
        case (conversationId, payload) =>
          Right(f(conversationId, payload))
      }
    }
  }

  // Request utility
  val authChars: Stream[Char] = new Iterator[Char] {
    val rand = new scala.util.Random(this.hashCode)
    val chars = rand.shuffle("""!"#$%&'()*+-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~""".toList)

    def hasNext = true

    var i = 0
    def next(): Char = {
      if (i == chars.length) {
        i = 1
        chars(0)
      } else {
        i += 1
        chars(i - 1)
      }
    }
  }.toStream

  def randomPrefix(seed: Int): String = {
    val pos = ((System.nanoTime() / 1000000L) % 100).toInt // temporal position
    val slice = authChars.slice(pos, pos + 24 /* random str length */ )
    val rand = new scala.util.Random(seed)

    rand.shuffle(slice.toList).mkString
  }
}

/**
 * @param serverSignature the SCRAM-SHA1 signature for the MongoDB server
 * @param request the next client request for the SCRAM-SHA1 authentication
 */
private[core] case class ScramNegociation(
  serverSignature: Array[Byte],
  request: BSONDocument)

@deprecated("Use `ScramNegociation`", "0.18.4")
private[core] case class ScramSha1Negociation(
  serverSignature: Array[Byte],
  request: BSONDocument)

@deprecated("Will be internal", "0.18.4")
object ScramSha1Negociation {
  @inline def parsePayload(payload: String): Map[String, String] = ScramNegociation.parsePayload(payload)

  def parsePayload(payload: Array[Byte]): Map[String, String] =
    parsePayload(new String(payload, "UTF-8"))

}

/**
 * Command to continue with Mongo SCRAM authentication.
 */
private[core] sealed trait ScramStartNegociation[M <: AuthenticationMode.Scram]
  extends Command[Either[SuccessfulAuthentication, Array[Byte]]] {

  protected def mechanism: M

  /** The name of the user */
  def user: String

  /** The user password */
  def password: String

  def conversationId: Int

  /** The initial payload */
  def payload: Array[Byte]

  protected val randomPrefix: String

  def startMessage: String

  protected def digest(data: Array[Byte]): Array[Byte]

  protected def hmac(key: Array[Byte], input: Array[Byte]): Array[Byte]

  protected def keyFactory: SecretKeyFactory

  // ---

  import javax.crypto.spec.PBEKeySpec
  import org.apache.commons.codec.digest.DigestUtils
  import org.apache.commons.codec.binary.Base64
  import akka.util.ByteString
  import reactivemongo.bson.buffer.ArrayReadableBuffer

  val data: Either[CommandError, ScramNegociation] = {
    val challenge = new String(payload, "UTF-8")
    val response = ScramNegociation.parsePayload(challenge)

    for {
      rand <- response.get("r").filter(_ startsWith randomPrefix).
        toRight(CommandError(s"invalid $mechanism random prefix")).right

      salt <- response.get("s").flatMap[Array[Byte]] { s =>
        try { Some(Base64 decodeBase64 s) } catch { case _: Throwable => None }
      }.toRight(CommandError(s"invalid $mechanism password salt")).right

      iter <- response.get("i").flatMap[Int] { i =>
        try { Some(i.toInt) } catch { case _: Throwable => None }
      }.toRight(CommandError(s"invalid $mechanism iteration count")).right

      nego <- try {
        val nonce = s"c=biws,r=$rand" // biws = base64("n,,")
        val saltedPassword: Array[Byte] = {
          val digest = DigestUtils.md5Hex(s"$user:mongo:$password")
          val spec = new PBEKeySpec(digest.toCharArray, salt, iter, 160 /* 20 * 8 = 20 bytes */ )
          keyFactory.generateSecret(spec).getEncoded
        }
        val authMsg =
          s"${startMessage.drop(3)},$challenge,$nonce".getBytes("UTF-8")

        val clientKey = hmac(saltedPassword, ScramNegociation.ClientKeySeed)

        val clientSig = hmac(digest(clientKey), authMsg)
        val clientProof = (clientKey, clientSig).
          zipped.map((a, b) => (a ^ b).toByte).toArray

        val message = s"$nonce,p=${Base64 encodeBase64String clientProof}"
        val serverKey = hmac(saltedPassword, ScramNegociation.ServerKeySeed)

        Right(ScramNegociation(
          serverSignature = hmac(serverKey, authMsg),
          request = BSONDocument(
            "saslContinue" -> 1,
            "conversationId" -> conversationId,
            "payload" -> BSONBinary(
              ArrayReadableBuffer(ByteString(message).toArray[Byte]),
              Subtype.GenericBinarySubtype)))).right

      } catch {
        case err: Throwable => Left(CommandError(
          s"fails to negociate $mechanism: ${err.getMessage}")).right
      }
    } yield nego
  }

  def serverSignature: Either[CommandError, Array[Byte]] =
    data.right.map(_.serverSignature)

  override def makeDocuments = data.right.map(_.request).right.get

}

private[core] case class ScramSha1StartNegociation(
  user: String,
  password: String,
  conversationId: Int,
  payload: Array[Byte],
  randomPrefix: String,
  startMessage: String) extends ScramStartNegociation[ScramSha1Authentication.type] {
  val mechanism = ScramSha1Authentication

  import org.apache.commons.codec.digest.{
    DigestUtils,
    HmacAlgorithms,
    HmacUtils
  }

  @inline def hmac(key: Array[Byte], input: Array[Byte]): Array[Byte] =
    new HmacUtils(HmacAlgorithms.HMAC_SHA_1, key).hmac(input)

  @inline def digest(data: Array[Byte]): Array[Byte] = DigestUtils.sha1(data)

  @inline def keyFactory: SecretKeyFactory =
    ScramSha1StartNegociation.keyFactory

  val ResultMaker = ScramSha1StartNegociation
}

private[core] object ScramNegociation {
  /**
   * Parses the UTF-8 `payload` as a map of properties exchanged
   * during the SCRAM-SHA1 authentication.
   */
  def parsePayload(payload: String): Map[String, String] =
    util.toMap(payload.split(",").map(_.split("=", 2)))(
      array => array(0) -> array(1))

  /**
   * Parses the binary `payload` as a map of properties exchanged
   * during the SCRAM-SHA1 authentication.
   */
  def parsePayload(payload: Array[Byte]): Map[String, String] =
    parsePayload(new String(payload, "UTF-8"))

  private[commands] val ClientKeySeed = // "Client Key" bytes
    Array[Byte](67, 108, 105, 101, 110, 116, 32, 75, 101, 121)

  private[commands] val ServerKeySeed = // "Server Key" bytes
    Array[Byte](83, 101, 114, 118, 101, 114, 32, 75, 101, 121)

}

private[core] object ScramStartNegociation {
  import reactivemongo.bson.BSONBooleanLike

  type ResType = Either[CommandError, Either[SuccessfulAuthentication, Array[Byte]]]

  def parseResponse[M <: AuthenticationMode.Scram](
    mechanism: M, bson: BSONDocument): ResType = {

    if (!bson.getAs[BSONBooleanLike]("ok").fold(false)(_.toBoolean)) {
      Left(CommandError(bson.getAs[String]("errmsg").
        getOrElse(s"${mechanism} authentication failure")))

    } else if (bson.getAs[BSONBooleanLike]("done").fold(false)(_.toBoolean)) {
      Right(Left(SilentSuccessfulAuthentication))
    } else bson.getAs[Array[Byte]]("payload").fold[ResType](
      Left(CommandError(s"missing ${mechanism} payload")))(
        bytes => Right(Right(bytes)))
  }
}

@deprecated("Will be internal", "0.18.4")
@SerialVersionUID(113814637L)
object ScramSha1StartNegociation extends BSONCommandResultMaker[Either[SuccessfulAuthentication, Array[Byte]]] {
  type ResType = ScramStartNegociation.ResType

  def parseResponse(response: Response): ResType = apply(response)

  @inline def apply(bson: BSONDocument) =
    ScramStartNegociation.parseResponse(ScramSha1Authentication, bson)

  @inline private[commands] def ClientKeySeed = ScramNegociation.ClientKeySeed

  @inline private[commands] def ServerKeySeed = ScramNegociation.ServerKeySeed

  @transient lazy val keyFactory =
    SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")

}

sealed trait ScramFinalNegociation extends Command[SuccessfulAuthentication] {
  /** The ID of the SCRAM conversation */
  def conversationId: Int

  def payload: Array[Byte]

  import reactivemongo.bson.buffer.ArrayReadableBuffer

  override def makeDocuments = BSONDocument(
    "saslContinue" -> 1,
    "conversationId" -> conversationId,
    "payload" -> BSONBinary(
      ArrayReadableBuffer(payload), Subtype.GenericBinarySubtype))

  val ResultMaker = ScramFinalNegociation

  private[core] lazy val tupled = conversationId -> payload
}

@deprecated("Use ScramFinalNegociation", "0.18.4")
@SerialVersionUID(304027329L)
private[core] case class ScramSha1FinalNegociation(
  conversationId: Int,
  payload: Array[Byte]) extends ScramFinalNegociation {

}

@deprecated("Unused", "0.18.4")
object ScramSha1FinalNegociation
  extends BSONCommandResultMaker[SuccessfulAuthentication] {

  def apply(bson: BSONDocument) = ???
}

private[core] object ScramFinalNegociation
  extends BSONCommandResultMaker[SuccessfulAuthentication] {

  def apply(bson: BSONDocument) = ???

  def apply(
    conversationId: Int,
    payload: Array[Byte]): ScramFinalNegociation =
    new Default(conversationId, payload)

  private final class Default(
    val conversationId: Int,
    val payload: Array[Byte]) extends ScramFinalNegociation {

    override def equals(that: Any): Boolean = that match {
      case other: ScramFinalNegociation => tupled == other.tupled
      case _                            => false
    }

    override def hashCode: Int = tupled.hashCode
  }
}

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
