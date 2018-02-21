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
        case Some(true) => {
          decoder.string(doc, "note").foreach { note =>
            Command.logger.info(s"${note}: ${pack pretty doc}")
          }

          readResult(doc)
        }

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

  implicit private[reactivemongo] def defaultWriteResultReader: pack.Reader[DefaultWriteResult] = {
    val decoder = pack.newDecoder

    def readWriteError(doc: pack.Document): WriteError = (for {
      index <- decoder.int(doc, "index")
      code <- decoder.int(doc, "code")
      err <- decoder.string(doc, "errmsg")
    } yield WriteError(index, code, err)).get

    def readWriteConcernError(doc: pack.Document): WriteConcernError = (for {
      code <- decoder.int(doc, "code")
      err <- decoder.string(doc, "errmsg")
    } yield WriteConcernError(code, err)).get

    dealingWithGenericCommandErrorsReader[DefaultWriteResult] { doc =>
      val werrors = decoder.children(doc, "writeErrors").map(readWriteError(_))

      val wcError = decoder.child(doc, "writeConcernError").
        map(readWriteConcernError(_))

      DefaultWriteResult(
        ok = decoder.booleanLike(doc, "ok").getOrElse(true),
        n = decoder.int(doc, "n").getOrElse(0),
        writeErrors = werrors,
        writeConcernError = wcError,
        code = decoder.int(doc, "code"),
        errmsg = decoder.string(doc, "errmsg"))

    }
  }
}
