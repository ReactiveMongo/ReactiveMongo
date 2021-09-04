package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Getnonce Command for Mongo CR authentication.
 *
 * Gets a nonce for authentication token.
 */
private[reactivemongo] object GetCrNonce
  extends Command with CommandWithResult[CrNonce] {

  val commandKind = CommandKind.GetNonce

  def writer[P <: SerializationPack](pack: P): pack.Writer[GetCrNonce.type] = {
    val builder = pack.newBuilder
    val cmd = builder.document(Seq(
      builder.elementProducer("getnonce", builder.int(1))))

    pack.writer[GetCrNonce.type](_ => cmd)
  }

  def reader[P <: SerializationPack](pack: P): pack.Reader[CrNonce] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandExceptionsReaderOpt[pack.type, CrNonce](pack) {
      decoder.string(_, "nonce").map { new CrNonce(_) }
    }
  }
}

private[reactivemongo] final class CrNonce(val value: String) extends AnyVal

/**
 * Mongo CR authenticate Command.
 *
 * @param user the username
 * @param password the user password
 * @param nonce the previous nonce given by the server
 */
private[reactivemongo] case class CrAuthenticate(
  user: String,
  password: String,
  nonce: String) extends Command with CommandWithResult[AuthenticationResult] {
  import reactivemongo.util._

  val commandKind = CommandKind.Authenticate

  /** the computed digest of the password */
  lazy val pwdDigest = md5Hex(s"$user:mongo:$password", "UTF-8")

  /** the digest of the tuple (''nonce'', ''user'', ''pwdDigest'') */
  lazy val key = md5Hex(nonce + user + pwdDigest, "UTF-8")
}

/** Authentication command's response deserializer. */
private[reactivemongo] object CrAuthenticate {
  def writer[P <: SerializationPack](pack: P): pack.Writer[CrAuthenticate] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => elem, string }

    pack.writer[CrAuthenticate] { auth =>
      builder.document(Seq(
        elem("authenticate", builder.int(1)),
        elem("user", string(auth.user)),
        elem("nonce", string(auth.nonce)),
        elem("key", string(auth.key))))
    }
  }

  def reader[P <: SerializationPack](pack: P): pack.Reader[AuthenticationResult] = {
    val decoder = pack.newDecoder

    import decoder.string

    pack.readerOpt[AuthenticationResult] { doc =>
      string(doc, "errmsg") match {
        case Some(error) =>
          Some(
            FailedAuthentication(pack)(error, decoder.int(doc, "code"),
              Some(doc)))

        case _ => (for {
          dbn <- string(doc, "dbname")
          usr <- string(doc, "user")
          ro = decoder.booleanLike(doc, "readOnly").getOrElse(false)
        } yield VerboseSuccessfulAuthentication(dbn, usr, ro))
      }
    }
  }
}
