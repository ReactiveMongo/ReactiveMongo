package reactivemongo.core.commands

import reactivemongo.bson.{
  BSONBinary,
  BSONDocument,
  BSONInteger,
  BSONNumberLike,
  BSONString,
  Subtype
}
import reactivemongo.core.protocol.Response

// --- MongoDB SCRAM-SHA1 authentication ---

private[core] case class ScramSha1Challenge(
  conversationId: Int, payload: Array[Byte]) {
  override def toString = s"ScramSha1Challenge($conversationId)"
}

/**
 * Command to start with Mongo SCRAM-SHA1 authentication.
 *
 * @param user username
 */
private[core] case class ScramSha1Initiate(
  user: String) extends Command[ScramSha1Challenge] {

  import akka.util.ByteString
  import reactivemongo.bson.buffer.ArrayReadableBuffer

  val randomPrefix = ScramSha1Initiate.randomPrefix(this.hashCode)

  /** Initial SCRAM-SHA1 message */
  val message = {
    val preparedUsername =
      s"""n=${user.replace("=", "=3D").replace(",", "=2D")}"""

    s"n,,$preparedUsername,r=$randomPrefix"
  }

  override def makeDocuments = BSONDocument(
    "saslStart" -> 1,
    "mechanism" -> "SCRAM-SHA-1",
    "payload" -> BSONBinary(
      ArrayReadableBuffer(ByteString(message).toArray[Byte]),
      Subtype.GenericBinarySubtype))

  val ResultMaker = ScramSha1Initiate
}

object ScramSha1Initiate extends BSONCommandResultMaker[ScramSha1Challenge] {
  def parseResponse(response: Response): Either[CommandError, ScramSha1Challenge] = apply(response)

  def apply(bson: BSONDocument) =
    CommandError.checkOk(bson, Some("authenticate"), (doc, name) => {
      FailedAuthentication(doc.getAs[BSONString]("errmsg").
        map(_.value).getOrElse(""), Some(doc))
    }).map[Either[CommandError, ScramSha1Challenge]](Left(_)).getOrElse {
      (for {
        cid <- bson.getAs[BSONNumberLike]("conversationId").map(_.toInt)
        pay <- bson.getAs[Array[Byte]]("payload")
      } yield (cid, pay)).fold[Either[CommandError, ScramSha1Challenge]](Left(FailedAuthentication(s"invalid SCRAM-SHA1 challenge response: ${BSONDocument pretty bson}", Some(bson)))) {
        case (conversationId, payload) =>
          Right(ScramSha1Challenge(conversationId, payload))
      }
    }

  // Request utility
  private val authChars: Stream[Char] = new Iterator[Char] {
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
    val pos = (System.currentTimeMillis % 100).toInt // temporal position
    val slice = authChars.slice(pos, pos + 24 /* random str length */ )
    val rand = new scala.util.Random(seed)

    rand.shuffle(slice.toList).mkString
  }
}

/**
 * @param serverSignature the SCRAM-SHA1 signature for the MongoDB server
 * @param request the next client request for the SCRAM-SHA1 authentication
 */
private[core] case class ScramSha1Negociation(
  serverSignature: Array[Byte],
  request: BSONDocument)

object ScramSha1Negociation {
  /**
   * Parses the UTF-8 `payload` as a map of properties exchanged
   * during the SCRAM-SHA1 authentication.
   */
  def parsePayload(payload: String): Map[String, String] =
    payload.split(",").map(_.split("=", 2)).
      map(array => array(0) -> array(1)).toMap

  /**
   * Parses the binary `payload` as a map of properties exchanged
   * during the SCRAM-SHA1 authentication.
   */
  def parsePayload(payload: Array[Byte]): Map[String, String] =
    parsePayload(new String(payload, "UTF-8"))

}

/**
 * Command to continue with Mongo SCRAM-SHA1 authentication.
 *
 * @param user the name of the user
 * @param password the user password
 * @param conversationId
 * @param payload the initial payload
 * @param randomPrefix
 * @param startMessage
 */
private[core] case class ScramSha1StartNegociation(
  user: String,
  password: String,
  conversationId: Int,
  payload: Array[Byte],
  randomPrefix: String,
  startMessage: String) extends Command[Either[SuccessfulAuthentication, Array[Byte]]] {

  import javax.crypto.spec.PBEKeySpec
  import org.apache.commons.codec.binary.Base64
  import org.apache.commons.codec.digest.{ DigestUtils, HmacUtils }
  import akka.util.ByteString
  import reactivemongo.bson.buffer.ArrayReadableBuffer

  val data: Either[CommandError, ScramSha1Negociation] = {
    val challenge = new String(payload, "UTF-8")
    val response = ScramSha1Negociation.parsePayload(challenge)

    for {
      rand <- response.get("r").filter(_ startsWith randomPrefix).
        toRight(CommandError("invalid SCRAM-SHA1 random prefix")).right

      salt <- response.get("s").flatMap[Array[Byte]] { s =>
        try { Some(Base64 decodeBase64 s) } catch { case _: Throwable => None }
      }.toRight(CommandError("invalid SCRAM-SHA1 password salt")).right

      iter <- response.get("i").flatMap[Int] { i =>
        try { Some(i.toInt) } catch { case _: Throwable => None }
      }.toRight(CommandError("invalid SCRAM-SHA1 iteration count")).right

      nego <- try {
        import HmacUtils.hmacSha1

        val nonce = s"c=biws,r=$rand" // biws = base64("n,,")
        val saltedPassword: Array[Byte] = {
          val digest = DigestUtils.md5Hex(s"$user:mongo:$password")
          val spec = new PBEKeySpec(digest.toCharArray, salt, iter, 160 /* 20 * 8 = 20 bytes */ )
          ScramSha1StartNegociation.keyFactory.generateSecret(spec).getEncoded
        }
        val authMsg =
          s"${startMessage.drop(3)},$challenge,$nonce".getBytes("UTF-8")

        val clientKey =
          hmacSha1(saltedPassword, ScramSha1StartNegociation.ClientKeySeed)
        val clientSig = hmacSha1(DigestUtils.sha1(clientKey), authMsg)
        val clientProof: Array[Byte] = (clientKey, clientSig).
          zipped.map((a, b) => (a ^ b).toByte)

        val message = s"$nonce,p=${Base64.encodeBase64String(clientProof)}"
        val serverKey =
          hmacSha1(saltedPassword, ScramSha1StartNegociation.ServerKeySeed)

        Right(ScramSha1Negociation(
          serverSignature = hmacSha1(serverKey, authMsg),
          request = BSONDocument(
            "saslContinue" -> 1, "conversationId" -> conversationId,
            "payload" -> BSONBinary(
              ArrayReadableBuffer(ByteString(message).toArray[Byte]),
              Subtype.GenericBinarySubtype)))).right

      } catch {
        case err: Throwable => Left(CommandError(
          s"fails to negociate SCRAM-SHA1: ${err.getMessage}")).right
      }
    } yield nego
  }

  def serverSignature: Either[CommandError, Array[Byte]] =
    data.right.map(_.serverSignature)

  override def makeDocuments = data.right.map(_.request).right.get

  val ResultMaker = ScramSha1StartNegociation
}

@SerialVersionUID(113814637L)
object ScramSha1StartNegociation extends BSONCommandResultMaker[Either[SuccessfulAuthentication, Array[Byte]]] {
  import reactivemongo.bson.BSONBooleanLike

  type ResType = Either[CommandError, Either[SuccessfulAuthentication, Array[Byte]]]

  def parseResponse(response: Response): ResType = apply(response)

  def apply(bson: BSONDocument) = {
    if (!bson.getAs[BSONBooleanLike]("ok").fold(false)(_.toBoolean)) {
      Left(CommandError(bson.getAs[String]("errmsg").
        getOrElse("SCRAM-SHA1 authentication failure")))

    } else if (bson.getAs[BSONBooleanLike]("done").fold(false)(_.toBoolean)) {
      Right(Left(SilentSuccessfulAuthentication))
    } else bson.getAs[Array[Byte]]("payload").fold[ResType](
      Left(CommandError("missing SCRAM-SHA1 payload")))(
        bytes => Right(Right(bytes)))
  }

  private[commands] val ClientKeySeed = // "Client Key" bytes
    Array[Byte](67, 108, 105, 101, 110, 116, 32, 75, 101, 121)

  private[commands] val ServerKeySeed = // "Server Key" bytes
    Array[Byte](83, 101, 114, 118, 101, 114, 32, 75, 101, 121)

  @transient lazy val keyFactory =
    javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
}

/**
 * @param conversationId the ID of the SCRAM-SHA1 conversation
 */
@SerialVersionUID(304027329L)
private[core] case class ScramSha1FinalNegociation(
  conversationId: Int,
  payload: Array[Byte]) extends Command[SuccessfulAuthentication] {

  import reactivemongo.bson.buffer.ArrayReadableBuffer

  override def makeDocuments = BSONDocument(
    "saslContinue" -> 1, "conversationId" -> conversationId,
    "payload" -> BSONBinary(
      ArrayReadableBuffer(payload), Subtype.GenericBinarySubtype))

  val ResultMaker = ScramSha1FinalNegociation
}

object ScramSha1FinalNegociation
  extends BSONCommandResultMaker[SuccessfulAuthentication] {

  def apply(bson: BSONDocument) = ???
}

// --- MongoDB CR authentication ---

@deprecated(message = "See [[GetCrNonce]]", since = "0.11.10")
object Getnonce extends Command[String] {
  override def makeDocuments = GetCrNonce.makeDocuments
  val ResultMaker = GetCrNonce.ResultMaker
}

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

@deprecated("See `CrAuthenticate`", "0.11.10")
case class Authenticate(user: String, password: String, nonce: String)
  extends Command[SuccessfulAuthentication] {

  private val underlying = CrAuthenticate(user, password, nonce)

  @deprecated("See `CrAuthenticate.makeDocuments`", "0.11.10")
  override def makeDocuments = underlying.makeDocuments

  @deprecated("See `CrAuthenticate.ResultMaker`", "0.11.10")
  val ResultMaker = underlying.ResultMaker

  @deprecated("See `CrAuthenticate.pwdDigest`", "0.11.10")
  def pwdDigest = underlying.pwdDigest

  @deprecated("See `CrAuthenticate.key`", "0.11.10")
  def key = underlying.key
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
  lazy val pwdDigest = md5Hex(s"$user:mongo:$password")

  /** the digest of the tuple (''nonce'', ''user'', ''pwdDigest'') */
  lazy val key = md5Hex(nonce + user + pwdDigest)

  override def makeDocuments = BSONDocument("authenticate" -> BSONInteger(1), "user" -> BSONString(user), "nonce" -> BSONString(nonce), "key" -> BSONString(key))

  val ResultMaker = CrAuthenticate
}

/** Authentication command's response deserializer. */
object CrAuthenticate extends BSONCommandResultMaker[SuccessfulAuthentication] {
  def parseResponse(response: Response): Either[CommandError, SuccessfulAuthentication] = apply(response)

  def apply(document: BSONDocument) = {
    CommandError.checkOk(document, Some("authenticate"), (doc, name) => {
      FailedAuthentication(doc.getAs[BSONString]("errmsg").map(_.value).getOrElse(""), Some(doc))
    }).toLeft(document.get("dbname") match {
      case Some(BSONString(dbname)) => VerboseSuccessfulAuthentication(
        dbname,
        document.getAs[String]("user").get,
        document.getAs[Boolean]("readOnly").getOrElse(false))
      case _ => SilentSuccessfulAuthentication
    })
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
 * @param db database name
 * @param user username
 * @param readOnly states if the authentication gives us only the right to read from the database.
 */
case class VerboseSuccessfulAuthentication(
  db: String,
  user: String,
  readOnly: Boolean) extends SuccessfulAuthentication

/**
 * A failed authentication result
 *
 * @param message the explanation of the error.
 */
case class FailedAuthentication(
  message: String,
  originalDocument: Option[BSONDocument] = None) extends BSONCommandError with AuthenticationResult {
  val code = None
}
