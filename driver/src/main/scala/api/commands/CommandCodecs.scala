package reactivemongo.api.commands

import reactivemongo.api.{ ReadConcern, SerializationPack }

private[reactivemongo] trait CommandCodecs[P <: SerializationPack] {
  protected val pack: P

  /**
   * Helper to read a command result, with error handling.
   */
  private[reactivemongo] def dealingWithGenericCommandErrorsReader[A](readResult: pack.Document => A): pack.Reader[A] = {
    val decoder = pack.newDecoder

    pack.reader[A] { doc: pack.Document =>
      decoder.booleanLike(doc, "ok") match {
        case Some(true) => readResult(doc)

        case _ => throw CommandError(pack)(
          code = decoder.int(doc, "code"),
          errmsg = decoder.string(doc, "errmsg"),
          originalDocument = doc)
      }
    }
  }

  implicit private[reactivemongo] def unitBoxReader: pack.Reader[UnitBox.type] =
    dealingWithGenericCommandErrorsReader[UnitBox.type] { _ => UnitBox }

  private[reactivemongo] lazy val writeReadConcern: ReadConcern => pack.Document = {
    val builder = pack.newBuilder

    { concern: ReadConcern =>
      builder.document(Seq(builder.elementProducer(
        "level", builder.string(concern.level))))
    }
  }
}
