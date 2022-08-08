package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

private[reactivemongo] case class X509Authenticate(user: Option[String])
    extends Command
    with CommandWithResult[AuthenticationResult] {
  val commandKind = CommandKind.Authenticate
}

private[reactivemongo] object X509Authenticate {

  def writer[P <: SerializationPack](pack: P): pack.Writer[X509Authenticate] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => elem, string }

    val baseCmd = Seq(
      elem("authenticate", builder.int(1)),
      elem("mechanism", string("MONGODB-X509"))
    )

    pack.writer[X509Authenticate] { auth =>
      val cmd = Seq.newBuilder[pack.ElementProducer] ++= baseCmd

      auth.user.foreach { name => cmd += elem("user", string(name)) }

      builder.document(cmd.result())
    }
  }

  def reader[P <: SerializationPack](
      pack: P
    ): pack.Reader[AuthenticationResult] = {
    val decoder = pack.newDecoder

    pack.reader[AuthenticationResult] { doc =>
      decoder.string(doc, "errmsg") match {
        case Some(error) =>
          FailedAuthentication(pack)(error, decoder.int(doc, "code"), Some(doc))

        case _ =>
          SilentSuccessfulAuthentication
      }
    }
  }
}
