package reactivemongo.api.commands

import reactivemongo.api.{ ReadConcern, Session, SerializationPack }

private[reactivemongo] trait CommandCodecs[P <: SerializationPack with Singleton] {
  protected val pack: P

  /**
   * Helper to read a command result, with error handling.
   */
  private[reactivemongo] def dealingWithGenericCommandErrorsReader[A](readResult: pack.Document => A): pack.Reader[A] = CommandCodecs.dealingWithGenericCommandErrorsReader[pack.type, A](pack)(readResult)

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

private[reactivemongo] object CommandCodecs {
  /**
   * Helper to read a command result, with error handling.
   */
  def dealingWithGenericCommandErrorsReader[P <: SerializationPack, A](pack: P)(readResult: pack.Document => A): pack.Reader[A] = {
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

  def unitBoxReader[P <: SerializationPack](pack: P): pack.Reader[UnitBox.type] = dealingWithGenericCommandErrorsReader[pack.type, UnitBox.type](pack) { _ => UnitBox }

  def writeReadConcern[P <: SerializationPack with Singleton](pack: P): ReadConcern => pack.Document = writeReadConcern[pack.type](pack.newBuilder)

  def writeReadConcern[P <: SerializationPack with Singleton](builder: SerializationPack.Builder[P]): ReadConcern => builder.pack.Document = { c: ReadConcern =>
    builder.document(Seq(builder.elementProducer(
      "level", builder.string(c.level))))
  }

  @inline def writeWriteConcern[P <: SerializationPack with Singleton](pack: P): WriteConcern => pack.Document = writeWriteConcern[pack.type](pack.newBuilder)

  def writeWriteConcern[P <: SerializationPack with Singleton](
    builder: SerializationPack.Builder[P]): WriteConcern => builder.pack.Document = { writeConcern: WriteConcern =>
    import builder.{ elementProducer => element, int, string }

    val elements = Seq.newBuilder[builder.pack.ElementProducer]

    writeConcern.w match {
      case GetLastError.Majority =>
        elements += element("w", string("majority"))

      case GetLastError.TagSet(tagSet) =>
        elements += element("w", string(tagSet))

      case GetLastError.WaitForAcknowledgments(n) =>
        elements += element("w", int(n))

      case GetLastError.WaitForAknowledgments(n) =>
        elements += element("w", int(n))

    }

    element("j", builder.boolean(true))

    writeConcern.wtimeout.foreach { t =>
      elements += element("wtimeout", int(t))
    }

    builder.document(elements.result())
  }

  type SeqBuilder[T] = scala.collection.mutable.Builder[T, Seq[T]]

  def writeSession[P <: SerializationPack with Singleton](builder: SerializationPack.Builder[P]): SeqBuilder[builder.pack.ElementProducer] => Session => Unit = { elements =>
    import builder.{ document, elementProducer => element }

    { session: Session =>
      elements += element("lsid", document(
        Seq(element("id", builder.uuid(session.lsid)))))

      elements += element("txnNumber", builder.long(session.nextTxnNumber()))

      ()
    }
  }
}
