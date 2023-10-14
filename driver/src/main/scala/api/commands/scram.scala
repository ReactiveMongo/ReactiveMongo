package reactivemongo.api.commands

import javax.crypto.SecretKeyFactory

import scala.util.control.NonFatal

import reactivemongo.core.errors.{ CommandException => CmdErr }

import reactivemongo.api.{
  AuthenticationMode,
  ScramSha1Authentication,
  ScramSha256Authentication,
  SerializationPack
}

import reactivemongo.actors.util.ByteString
import reactivemongo.util

// --- MongoDB SCRAM authentication ---

private[reactivemongo] sealed trait ScramChallenge[
    M <: AuthenticationMode.Scram] {
  protected def mechanism: M

  def conversationId: Int
  def payload: Array[Byte]

  override def toString = s"ScramChallenge(${mechanism}, $conversationId)"
}

private[reactivemongo] case class ScramSha1Challenge(
    conversationId: Int,
    payload: Array[Byte])
    extends ScramChallenge[ScramSha1Authentication.type] {

  val mechanism = ScramSha1Authentication
}

private[reactivemongo] case class ScramSha256Challenge(
    conversationId: Int,
    payload: Array[Byte])
    extends ScramChallenge[ScramSha256Authentication.type] {

  val mechanism = ScramSha256Authentication
}

/**
 * Command to start with Mongo SCRAM authentication.
 */
private[reactivemongo] sealed trait ScramInitiate[M <: AuthenticationMode.Scram]
    extends Command
    with CommandWithResult[ScramChallenge[M]] {
  val commandKind = CommandKind.SaslStart

  /** The user name */
  def user: String

  protected def mechanism: M

  val randomPrefix = ScramInitiate.randomPrefix(this.hashCode)

  /** Initial SCRAM message */
  val message: String = {
    val preparedUsername =
      s"""n=${user.replace("=", "=3D").replace(",", "=2D")}"""

    s"n,,$preparedUsername,r=$randomPrefix"
  }
}

private[reactivemongo] case class ScramSha1Initiate(
    user: String)
    extends ScramInitiate[ScramSha1Authentication.type] {
  val mechanism = ScramSha1Authentication
}

private[reactivemongo] object ScramSha1Initiate {
  import ScramInitiate.Result

  @inline def reader[P <: SerializationPack](
      pack: P
    ): pack.Reader[Result[ScramSha1Authentication.type]] = ScramInitiate
    .reader(pack, ScramSha1Authentication)(ScramSha1Challenge.apply)
}

private[reactivemongo] case class ScramSha256Initiate(
    user: String)
    extends ScramInitiate[ScramSha256Authentication.type] {
  val mechanism = ScramSha256Authentication
}

private[reactivemongo] object ScramSha256Initiate {
  import ScramInitiate.Result

  @inline def reader[P <: SerializationPack](
      pack: P
    ): pack.Reader[Result[ScramSha256Authentication.type]] = ScramInitiate
    .reader(pack, ScramSha256Authentication)(ScramSha256Challenge.apply)

}

private[reactivemongo] object ScramInitiate {
  type Result[M <: AuthenticationMode.Scram] = Either[CmdErr, ScramChallenge[M]]

  def reader[P <: SerializationPack, M <: AuthenticationMode.Scram](
      pack: P,
      mechanism: M
    )(f: (Int, Array[Byte]) => ScramChallenge[M]
    ): pack.Reader[Result[M]] = {
    val decoder = pack.newDecoder

    import decoder.{ int, string }

    pack.reader[Result[M]] { doc =>
      string(doc, "errmsg") match {
        case Some(error) =>
          Left(FailedAuthentication(pack)(error, int(doc, "code"), Some(doc)))

        case _ =>
          (for {
            cid <- int(doc, "conversationId")
            pay <- decoder.binary(doc, "payload")
          } yield f(cid, pay)).fold[Result[M]](
            Left(
              FailedAuthentication(pack)(
                s"invalid $mechanism challenge response: ${pack pretty doc}",
                None,
                Some(doc)
              )
            )
          )(Right(_))
      }
    }
  }

  def writer[P <: SerializationPack, M <: AuthenticationMode.Scram](
      pack: P,
      mechanism: M
    ): pack.Writer[ScramInitiate[M]] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => elem }

    pack.writer[ScramInitiate[M]] { cmd =>
      builder.document(
        Seq(
          elem("saslStart", builder.int(1)),
          elem("mechanism", builder.string(mechanism.name)),
          elem("payload", builder.binary(ByteString(cmd.message).toArray[Byte]))
        )
      )
    }
  }

  // Request utility
  private val rand = new scala.util.Random(this.hashCode)

  private val authChars = util.toStream(new Iterator[Char] {

    val chars = rand.shuffle(
      """!"#$%&'()*+-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~""".toIndexedSeq
    )

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
  })

  def randomPrefix(seed: Int): String = {
    val pos = ((System.nanoTime() / 1000000L) % 100).toInt // temporal position
    val slice = authChars.slice(pos, pos + 24 /* random str length */ )
    val pr = new scala.util.Random(seed)

    pr.shuffle(slice.toList).mkString
  }
}

/**
 * @param serverSignature the SCRAM signature for the MongoDB server
 * @param request the next client request for the SCRAM authentication
 */
private[reactivemongo] case class ScramNegociation(
    serverSignature: Array[Byte],
    requestConversationId: Int,
    requestPayload: String)

/**
 * Command to continue with Mongo SCRAM authentication.
 */
private[reactivemongo] sealed trait ScramStartNegociation[
    M <: AuthenticationMode.Scram]
    extends Command
    with CommandWithResult[Either[SuccessfulAuthentication, Array[Byte]]] {
  val commandKind = CommandKind.SaslContinue

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

  protected def credential: Either[CmdErr, String]

  protected def storedKeySize: Int

  // ---

  import javax.crypto.spec.PBEKeySpec
  import org.apache.commons.codec.binary.Base64

  lazy val data: Either[CmdErr, ScramNegociation] = {
    val challenge = new String(payload, "UTF-8")
    val response = ScramNegociation.parsePayload(challenge)

    for {
      rand <- response
        .get("r")
        .filter(_ startsWith randomPrefix)
        .toRight(CmdErr(s"invalid $mechanism random prefix"))
        .right

      salt <- response
        .get("s")
        .flatMap[Array[Byte]] { s =>
          try {
            Some(Base64 decodeBase64 s)
          } catch {
            case NonFatal(_) => None
          }
        }
        .toRight(CmdErr(s"invalid $mechanism password salt"))
        .right

      iter <- response
        .get("i")
        .flatMap[Int] { i =>
          try {
            Some(i.toInt)
          } catch {
            case NonFatal(_) => None
          }
        }
        .toRight(CmdErr(s"invalid $mechanism iteration count"))
        .right

      hashCredential <- credential.right
      nego <-
        try {
          val nonce = s"c=biws,r=$rand" // biws = base64("n,,")
          val saltedPassword: Array[Byte] = {
            val digest = hashCredential

            val spec =
              new PBEKeySpec(digest.toCharArray, salt, iter, storedKeySize)

            keyFactory.generateSecret(spec).getEncoded
          }
          val authMsg =
            s"${startMessage drop 3},$challenge,$nonce".getBytes("UTF-8")

          val clientKey = hmac(saltedPassword, ScramNegociation.ClientKeySeed)

          val clientSig = hmac(digest(clientKey), authMsg)
          val clientProof = util
            .lazyZip(clientKey, clientSig)
            .map((a, b) => (a ^ b).toByte)
            .toArray

          val message = s"$nonce,p=${Base64 encodeBase64String clientProof}"
          val serverKey = hmac(saltedPassword, ScramNegociation.ServerKeySeed)

          Right(
            ScramNegociation(
              serverSignature = hmac(serverKey, authMsg),
              requestConversationId = conversationId,
              requestPayload = message
            )
          ).right

        } catch {
          case NonFatal(err) =>
            Left(
              CmdErr(s"fails to negociate $mechanism: ${err.getMessage}")
            ).right
        }
    } yield nego
  }

  def serverSignature: Either[CmdErr, Array[Byte]] =
    data.right.map(_.serverSignature)
}

private[reactivemongo] case class ScramSha1StartNegociation(
    user: String,
    password: String,
    conversationId: Int,
    payload: Array[Byte],
    randomPrefix: String,
    startMessage: String)
    extends ScramStartNegociation[ScramSha1Authentication.type] {
  val mechanism = ScramSha1Authentication

  import org.apache.commons.codec.digest.{
    DigestUtils,
    HmacAlgorithms,
    HmacUtils
  }

  @inline def hmac(key: Array[Byte], input: Array[Byte]): Array[Byte] =
    new HmacUtils(HmacAlgorithms.HMAC_SHA_1, key).hmac(input)

  @inline def digest(data: Array[Byte]): Array[Byte] = DigestUtils.sha1(data)

  @inline def credential: Either[CmdErr, String] =
    Right(DigestUtils.md5Hex(s"$user:mongo:$password"))

  @inline def storedKeySize = 160 /* 20 x 8 = 20 bytes */

  @inline def keyFactory: SecretKeyFactory =
    ScramSha1StartNegociation.keyFactory
}

private[reactivemongo] object ScramSha1StartNegociation {

  @transient lazy val keyFactory =
    SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")

}

private[reactivemongo] case class ScramSha256StartNegociation(
    user: String,
    password: String,
    conversationId: Int,
    payload: Array[Byte],
    randomPrefix: String,
    startMessage: String)
    extends ScramStartNegociation[ScramSha256Authentication.type] {
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

  @inline def credential = SaslPrep(password, false).left.map(CmdErr(_))

  @inline def keyFactory: SecretKeyFactory =
    ScramSha256StartNegociation.keyFactory
}

private[reactivemongo] object ScramSha256StartNegociation {

  @transient lazy val keyFactory =
    SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")

}

private[reactivemongo] object ScramNegociation {

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

  val ClientKeySeed = // "Client Key" bytes
    Array[Byte](67, 108, 105, 101, 110, 116, 32, 75, 101, 121)

  val ServerKeySeed = // "Server Key" bytes
    Array[Byte](83, 101, 114, 118, 101, 114, 32, 75, 101, 121)

}

private[reactivemongo] object ScramStartNegociation {

  def writer[P <: SerializationPack, M <: AuthenticationMode.Scram](
      pack: P
    ): pack.Writer[ScramStartNegociation[M]] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => elem, int }

    val saslContinue = elem("saslContinue", int(1))

    pack.writer[ScramStartNegociation[M]] { cmd =>
      cmd.data match {
        case Left(error) =>
          throw error

        case Right(start) =>
          builder.document(
            Seq(
              saslContinue,
              elem("conversationId", int(start.requestConversationId)),
              elem(
                "payload",
                builder.binary(ByteString(start.requestPayload).toArray[Byte])
              )
            )
          )
      }
    }
  }

  type Result = Either[CmdErr, Either[SuccessfulAuthentication, Array[Byte]]]

  def reader[P <: SerializationPack, M <: AuthenticationMode.Scram](
      pack: P,
      mechanism: M
    ): pack.Reader[Result] = {

    val decoder = pack.newDecoder

    import decoder.string

    pack.reader[Result] { doc =>
      string(doc, "errmsg") match {
        case Some(error) =>
          Left(CmdErr(error))

        case _ =>
          if (decoder.booleanLike(doc, "done") contains true) {
            Right(Left(SilentSuccessfulAuthentication))
          } else {
            decoder
              .binary(doc, "payload")
              .fold[Result](Left(CmdErr(s"missing ${mechanism} payload")))(
                bytes => Right(Right(bytes))
              )
          }
      }
    }
  }
}

/**
 * @param conversationId the ID of the SCRAM conversation
 * @param payload the request payload
 */
private[reactivemongo] case class ScramFinalNegociation(
    conversationId: Int,
    payload: Array[Byte])
    extends Command
    with CommandWithResult[SuccessfulAuthentication] {
  val commandKind = CommandKind.Authenticate
}

private[reactivemongo] object ScramFinalNegociation {

  def writer[P <: SerializationPack](
      pack: P
    ): pack.Writer[ScramFinalNegociation] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => elem, int }

    val saslContinue = elem("saslContinue", int(1))

    pack.writer[ScramFinalNegociation] { nego =>
      builder.document(
        Seq(
          saslContinue,
          elem("conversationId", int(nego.conversationId)),
          elem("payload", builder.binary(nego.payload))
        )
      )
    }
  }
}
