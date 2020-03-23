package reactivemongo.core.commands

import javax.crypto.SecretKeyFactory

import reactivemongo.api.{
  AuthenticationMode,
  ScramSha1Authentication,
  ScramSha256Authentication
}
import reactivemongo.api.commands.SaslPrep

import reactivemongo.api.bson.{ BSONBinary, BSONDocument, Subtype }
import reactivemongo.api.bson.collection.{ BSONSerializationPack => pack }

import reactivemongo.util

import reactivemongo.core.protocol.Response
import reactivemongo.core.errors.CommandError

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

private[core] case class ScramSha256Challenge(
  conversationId: Int,
  payload: Array[Byte]) extends ScramChallenge[ScramSha256Authentication.type] {

  val mechanism = ScramSha256Authentication
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
      ByteString(message).toArray[Byte],
      Subtype.GenericBinarySubtype))

  val ResultMaker: BSONCommandResultMaker[ScramChallenge[M]]
}

private[core] case class ScramSha1Initiate(
  user: String) extends ScramInitiate[ScramSha1Authentication.type] {
  val mechanism = ScramSha1Authentication
  val ResultMaker = ScramSha1Initiate
}

private[core] object ScramSha1Initiate extends BSONCommandResultMaker[ScramChallenge[ScramSha1Authentication.type]] {
  def parseResponse(response: Response): Either[CommandError, ScramChallenge[ScramSha1Authentication.type]] = apply(response)

  def apply(bson: BSONDocument) = ScramInitiate.parseResponse(ScramSha1Authentication, bson)(ScramSha1Challenge.apply)

  @inline protected def authChars: Stream[Char] = ScramInitiate.authChars

  @inline def randomPrefix(seed: Int): String = ScramInitiate.randomPrefix(seed)
}

private[core] case class ScramSha256Initiate(
  user: String) extends ScramInitiate[ScramSha256Authentication.type] {
  val mechanism = ScramSha256Authentication
  val ResultMaker = ScramSha256Initiate
}

private[core] object ScramSha256Initiate extends BSONCommandResultMaker[ScramChallenge[ScramSha256Authentication.type]] {
  def parseResponse(response: Response): Either[CommandError, ScramChallenge[ScramSha256Authentication.type]] = apply(response)

  def apply(bson: BSONDocument) = ScramInitiate.parseResponse(ScramSha256Authentication, bson)(ScramSha256Challenge.apply)
}

private[core] object ScramInitiate {
  def parseResponse[M <: AuthenticationMode.Scram](mechanism: M, bson: BSONDocument)(f: (Int, Array[Byte]) => ScramChallenge[M]): Either[CommandError, ScramChallenge[M]] = {
    CommandError.checkOk(bson, Some("authenticate"), (doc, _) => {
      FailedAuthentication(pack)(
        doc.string("errmsg").getOrElse(""),
        doc.int("code"),
        Some(doc))
    }).fold({
      (for {
        cid <- bson.int("conversationId")
        pay <- bson.getAsOpt[Array[Byte]]("payload")
      } yield (cid, pay)).fold[Either[CommandError, ScramChallenge[M]]](
        Left(FailedAuthentication(pack)(
          s"invalid $mechanism challenge response: ${BSONDocument pretty bson}",
          None,
          Some(bson)))) {
          case (conversationId, payload) =>
            Right(f(conversationId, payload))
        }
    })(Left(_))
  }

  // Request utility
  val authChars: Stream[Char] = new Iterator[Char] {
    val rand = new scala.util.Random(this.hashCode)
    val chars = rand.shuffle("""!"#$%&'()*+-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~""".toList)

    val hasNext = true

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
 * @param serverSignature the SCRAM signature for the MongoDB server
 * @param request the next client request for the SCRAM authentication
 */
private[core] case class ScramNegociation(
  serverSignature: Array[Byte],
  request: BSONDocument)

private[core] case class ScramSha1Negociation(
  serverSignature: Array[Byte],
  request: BSONDocument)

private[core] case class ScramSha256Negociation(
  serverSignature: Array[Byte],
  request: BSONDocument)

private[core] object ScramSha1Negociation { // TODO: private
  @inline def parsePayload(payload: String): Map[String, String] =
    ScramNegociation.parsePayload(payload)

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

  protected def startMessage: String

  protected def digest(data: Array[Byte]): Array[Byte]

  protected def hmac(key: Array[Byte], input: Array[Byte]): Array[Byte]

  protected def keyFactory: SecretKeyFactory

  protected def credential: Either[CommandError, String]

  protected def storedKeySize: Int

  // ---

  import javax.crypto.spec.PBEKeySpec
  import org.apache.commons.codec.binary.Base64
  import akka.util.ByteString

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

      hashCredential <- credential.right
      nego <- try {
        val nonce = s"c=biws,r=$rand" // biws = base64("n,,")
        val saltedPassword: Array[Byte] = {
          val digest = hashCredential

          val spec = new PBEKeySpec(
            digest.toCharArray, salt, iter, storedKeySize)

          keyFactory.generateSecret(spec).getEncoded
        }
        val authMsg =
          s"${startMessage drop 3},$challenge,$nonce".getBytes("UTF-8")

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
              ByteString(message).toArray[Byte],
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

  @inline def credential: Either[CommandError, String] =
    Right(DigestUtils.md5Hex(s"$user:mongo:$password"))

  @inline def storedKeySize = 160 /* 20 x 8 = 20 bytes */

  @inline def keyFactory: SecretKeyFactory =
    ScramSha1StartNegociation.keyFactory

  val ResultMaker = ScramSha1StartNegociation
}

private[core] object ScramSha1StartNegociation extends BSONCommandResultMaker[Either[SuccessfulAuthentication, Array[Byte]]] {
  type ResType = ScramStartNegociation.ResType

  def parseResponse(response: Response): ResType = apply(response)

  @inline def apply(bson: BSONDocument) =
    ScramStartNegociation.parseResponse(ScramSha1Authentication, bson)

  @inline private[commands] def ClientKeySeed = ScramNegociation.ClientKeySeed

  @inline private[commands] def ServerKeySeed = ScramNegociation.ServerKeySeed

  @transient lazy val keyFactory =
    SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")

}

private[core] case class ScramSha256StartNegociation(
  user: String,
  password: String,
  conversationId: Int,
  payload: Array[Byte],
  randomPrefix: String,
  startMessage: String) extends ScramStartNegociation[ScramSha256Authentication.type] {
  val mechanism = ScramSha256Authentication

  import org.apache.commons.codec.digest.{
    DigestUtils,
    HmacAlgorithms,
    HmacUtils
  }

  @inline def hmac(key: Array[Byte], input: Array[Byte]): Array[Byte] =
    new HmacUtils(HmacAlgorithms.HMAC_SHA_256, key).hmac(input)

  @inline def digest(data: Array[Byte]): Array[Byte] = DigestUtils.sha256(data)

  @inline def storedKeySize = 256 /* 32 x 8 = 256 bytes */

  @inline def credential = SaslPrep(password, false).left.map(CommandError(_))

  @inline def keyFactory: SecretKeyFactory =
    ScramSha256StartNegociation.keyFactory

  val ResultMaker = ScramSha256StartNegociation
}

private[core] object ScramSha256StartNegociation extends BSONCommandResultMaker[Either[SuccessfulAuthentication, Array[Byte]]] {
  def parseResponse(response: Response): ScramStartNegociation.ResType = apply(response)

  @inline def apply(bson: BSONDocument) =
    ScramStartNegociation.parseResponse(ScramSha256Authentication, bson)

  @transient lazy val keyFactory =
    SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")

}

private[core] object ScramNegociation {
  /**
   * Parses the UTF-8 `payload` as a map of properties exchanged
   * during the SCRAM authentication.
   */
  def parsePayload(payload: String): Map[String, String] =
    util.toMap(payload.split(",").map(_.span(_ != '=')).filterNot(_._2 == "")) {
      case (k, v) => k -> v.drop(1)
    }

  /**
   * Parses the binary `payload` as a map of properties exchanged
   * during the SCRAM authentication.
   */
  def parsePayload(payload: Array[Byte]): Map[String, String] =
    parsePayload(new String(payload, "UTF-8"))

  private[commands] val ClientKeySeed = // "Client Key" bytes
    Array[Byte](67, 108, 105, 101, 110, 116, 32, 75, 101, 121)

  private[commands] val ServerKeySeed = // "Server Key" bytes
    Array[Byte](83, 101, 114, 118, 101, 114, 32, 75, 101, 121)

}

private[core] object ScramStartNegociation {
  type ResType = Either[CommandError, Either[SuccessfulAuthentication, Array[Byte]]]

  def parseResponse[M <: AuthenticationMode.Scram](
    mechanism: M, bson: BSONDocument): ResType = {

    if (!bson.booleanLike("ok").getOrElse(false)) {
      Left(CommandError(bson.getAsOpt[String]("errmsg").
        getOrElse(s"${mechanism} authentication failure")))

    } else if (bson.booleanLike("done").getOrElse(false)) {
      Right(Left(SilentSuccessfulAuthentication))
    } else bson.getAsOpt[Array[Byte]]("payload").fold[ResType](
      Left(CommandError(s"missing ${mechanism} payload")))(
        bytes => Right(Right(bytes)))
  }
}

private[core] sealed trait ScramFinalNegociation
  extends Command[SuccessfulAuthentication] {

  /** The ID of the SCRAM conversation */
  def conversationId: Int

  def payload: Array[Byte]

  import reactivemongo.api.bson.buffer.ReadableBuffer

  override def makeDocuments = BSONDocument(
    "saslContinue" -> 1,
    "conversationId" -> conversationId,
    "payload" -> BSONBinary(payload, Subtype.GenericBinarySubtype))

  val ResultMaker = ScramFinalNegociation

  private[core] lazy val tupled = conversationId -> payload
}

@deprecated("Use ScramFinalNegociation", "0.18.4")
@SerialVersionUID(304027329L)
private[core] case class ScramSha1FinalNegociation(
  conversationId: Int,
  payload: Array[Byte]) extends ScramFinalNegociation {

}

private[core] object ScramFinalNegociation
  extends BSONCommandResultMaker[SuccessfulAuthentication] {

  def apply(bson: BSONDocument) = ??? // TODO: Remove

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
