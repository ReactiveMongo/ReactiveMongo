package reactivemongo.api.commands

import reactivemongo.api.{ ReadConcern, Session, SerializationPack }

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

  def defaultWriteResultReader[P <: SerializationPack with Singleton](pack: P): pack.Reader[DefaultWriteResult] = {
    val decoder = pack.newDecoder
    val readWriteError = CommandCodecs.readWriteError(decoder)
    val readWriteConcernError = CommandCodecs.readWriteConcernError(decoder)

    dealingWithGenericCommandErrorsReader[pack.type, DefaultWriteResult](pack) { doc =>
      val werrors = decoder.children(doc, "writeErrors").map(readWriteError)

      val wcError = decoder.child(doc, "writeConcernError").
        map(readWriteConcernError)

      DefaultWriteResult(
        ok = decoder.booleanLike(doc, "ok").getOrElse(true),
        n = decoder.int(doc, "n").getOrElse(0),
        writeErrors = werrors,
        writeConcernError = wcError,
        code = decoder.int(doc, "code"),
        errmsg = decoder.string(doc, "errmsg"))

    }
  }

  def unitBoxReader[P <: SerializationPack](pack: P): pack.Reader[UnitBox.type] = dealingWithGenericCommandErrorsReader[pack.type, UnitBox.type](pack) { _ => UnitBox }

  def writeReadConcern[P <: SerializationPack with Singleton](pack: P): ReadConcern => pack.Document = writeReadConcern[pack.type](pack.newBuilder)

  def writeReadConcern[P <: SerializationPack with Singleton](builder: SerializationPack.Builder[P]): ReadConcern => builder.pack.Document = { c: ReadConcern =>
    builder.document(Seq(
      builder.elementProducer("level", builder.string(c.level))))
  }

  def writeSessionReadConcern[P <: SerializationPack with Singleton](builder: SerializationPack.Builder[P]): (ReadConcern, Long) => builder.pack.Document = {
    (c: ReadConcern, time: Long) =>
      import builder.{ elementProducer => element }

      builder.document(Seq(
        element("level", builder.string(c.level)),
        element("afterClusterTime", builder.timestamp(time))))
  }

  @inline def writeWriteConcern[P <: SerializationPack with Singleton](pack: P): WriteConcern => pack.Document = writeWriteConcern[pack.type](pack.newBuilder)

  def writeGetLastErrorWWriter[P <: SerializationPack with Singleton](
    builder: SerializationPack.Builder[P]): GetLastError.W => builder.pack.Value = {
    case GetLastError.TagSet(tagSet)            => builder.string(tagSet)
    case GetLastError.WaitForAcknowledgments(n) => builder.int(n)
    case GetLastError.WaitForAknowledgments(n)  => builder.int(n)
    case _                                      => builder.string("majority")
  }

  def writeWriteConcern[P <: SerializationPack with Singleton](
    builder: SerializationPack.Builder[P]): WriteConcern => builder.pack.Document = {
    val writeGLEW = writeGetLastErrorWWriter(builder)

    { writeConcern: WriteConcern =>
      import builder.{ elementProducer => element, int }

      val elements = Seq.newBuilder[builder.pack.ElementProducer]

      elements += element("w", writeGLEW(writeConcern.w))
      elements += element("j", builder.boolean(true))

      writeConcern.wtimeout.foreach { t =>
        elements += element("wtimeout", int(t))
      }

      builder.document(elements.result())
    }
  }

  type SeqBuilder[T] = scala.collection.mutable.Builder[T, Seq[T]]

  def writeSession[P <: SerializationPack with Singleton](builder: SerializationPack.Builder[P]): SeqBuilder[builder.pack.ElementProducer] => Session => Unit = { elements =>
    import builder.{ document, elementProducer => element }

    { session: Session =>
      elements += element("lsid", document(
        Seq(element("id", builder.uuid(session.lsid)))))

      session.nextTxnNumber.foreach { txnNumber =>
        elements += element("txnNumber", builder.long(txnNumber))
      }

      ()
    }
  }

  def readWriteError[P <: SerializationPack with Singleton](decoder: SerializationPack.Decoder[P]): decoder.pack.Document => WriteError = { doc =>
    (for {
      index <- decoder.int(doc, "index")
      code <- decoder.int(doc, "code")
      err <- decoder.string(doc, "errmsg")
    } yield WriteError(index, code, err)).get
  }

  def readWriteConcernError[P <: SerializationPack with Singleton](decoder: SerializationPack.Decoder[P]): decoder.pack.Document => WriteConcernError = { doc =>
    (for {
      code <- decoder.int(doc, "code")
      err <- decoder.string(doc, "errmsg")
    } yield WriteConcernError(code, err)).get
  }

  def readUpserted[P <: SerializationPack with Singleton](decoder: SerializationPack.Decoder[P]): decoder.pack.Document => Upserted = { document =>
    (for {
      index <- decoder.int(document, "index")
      id <- decoder.get(document, "_id").map(decoder.pack.bsonValue)
    } yield Upserted(index, id)).get
  }
}
