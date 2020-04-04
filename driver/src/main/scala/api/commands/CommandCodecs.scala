package reactivemongo.api.commands

import scala.util.Success

import reactivemongo.api.{
  ReadConcern,
  Session,
  SerializationPack,
  WriteConcern
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

        case _ => throw reactivemongo.core.errors.CommandError(pack)(
          _message = decoder.string(doc, "errmsg").getOrElse(""),
          _originalDocument = Some(doc),
          _code = decoder.int(doc, "code"))
      }
    }
  }

  @inline def defaultWriteResultReader[P <: SerializationPack](pack: P): pack.Reader[DefaultWriteResult] = writeResultReader[DefaultWriteResult, pack.type](pack)

  def writeResultReader[WR >: DefaultWriteResult, P <: SerializationPack](pack: P): pack.Reader[WR] = {
    val decoder = pack.newDecoder
    val readWriteError = CommandCodecs.readWriteError(decoder)
    val readWriteConcernError = CommandCodecs.readWriteConcernError(decoder)

    dealingWithGenericCommandErrorsReader[pack.type, WR](pack) { doc =>
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

  //def writeReadConcern[P <: SerializationPack](pack: P): ReadConcern => pack.Document = writeReadConcern[pack.type](pack.newBuilder)

  def writeReadConcern[P <: SerializationPack](builder: SerializationPack.Builder[P]): ReadConcern => Seq[builder.pack.ElementProducer] = { c: ReadConcern => Seq(builder.elementProducer("level", builder.string(c.level))) }

  def writeSessionReadConcern[P <: SerializationPack](builder: SerializationPack.Builder[P]): Option[Session] => ReadConcern => Seq[builder.pack.ElementProducer] = {
    import builder.{ document, elementProducer => element, pack }

    val simpleReadConcern = writeReadConcern(builder)
    def simpleRead(c: ReadConcern): pack.ElementProducer =
      element("readConcern", document(simpleReadConcern(c)))

    val writeSession = CommandCodecs.writeSession(builder)

    { session =>
      session match {
        case Some(s) => {
          val elements = Seq.newBuilder[pack.ElementProducer]

          elements ++= writeSession(s)

          if (!s.transaction.filter(_.flagSent).isSuccess) {
            // No transaction, or flag not yet send (first tx command)
            s.operationTime match {
              case Some(opTime) => { c: ReadConcern =>
                elements += element("readConcern", document(
                  simpleReadConcern(c) :+ element(
                    "afterClusterTime", builder.timestamp(opTime))))

                elements.result()
              }

              case _ => { c: ReadConcern =>
                elements += simpleRead(c)

                elements.result()
              }
            }
          } else { _: ReadConcern =>
            // Ignore: Only the first command in a transaction
            // may specify a readConcern (code=72)

            elements.result()
          }
        }

        case _ => { c: ReadConcern => Seq(simpleRead(c)) }
      }
    }
  }

  @inline def writeWriteConcern[P <: SerializationPack](pack: P): WriteConcern => pack.Document = writeWriteConcern[pack.type](pack.newBuilder)

  def writeWriteConcernWWriter[P <: SerializationPack](
    builder: SerializationPack.Builder[P]): WriteConcern.W => builder.pack.Value = {
    case WriteConcern.TagSet(tagSet)            => builder.string(tagSet)
    case WriteConcern.WaitForAcknowledgments(n) => builder.int(n)
    case _                                      => builder.string("majority")
  }

  def writeWriteConcern[P <: SerializationPack](
    builder: SerializationPack.Builder[P]): WriteConcern => builder.pack.Document = {
    val writeGLEW = writeWriteConcernWWriter(builder)

    { writeConcern: WriteConcern =>
      import builder.{ elementProducer => element, int }

      val elements = Seq.newBuilder[builder.pack.ElementProducer]

      elements += element("w", writeGLEW(writeConcern.w))
      elements += element("j", builder.boolean(writeConcern.j))

      writeConcern.wtimeout.foreach { t =>
        elements += element("wtimeout", int(t))
      }

      builder.document(elements.result())
    }
  }

  def writeSession[P <: SerializationPack](builder: SerializationPack.Builder[P]): Session => Seq[builder.pack.ElementProducer] = {
    import builder.{ elementProducer => element }

    { session: Session =>
      val idElmt = builder.document(Seq(
        element("id", builder.uuid(session.lsid))))

      session.transaction match {
        case Success(transaction) => {
          val elms = Seq.newBuilder[builder.pack.ElementProducer]

          elms ++= Seq(
            element("lsid", idElmt),
            element("txnNumber", builder.long(transaction.txnNumber)))

          if (!session.transactionToFlag()) {
            elms += element("startTransaction", builder.boolean(true))
          }

          elms += element("autocommit", builder.boolean(false))

          elms.result()
        }

        case _ => Seq(element("lsid", idElmt))
      }
    }
  }

  def readWriteError[P <: SerializationPack](decoder: SerializationPack.Decoder[P]): decoder.pack.Document => WriteError = { doc =>
    (for {
      index <- decoder.int(doc, "index")
      code <- decoder.int(doc, "code")
      err <- decoder.string(doc, "errmsg")
    } yield WriteError(index, code, err)).get
  }

  def readWriteConcernError[P <: SerializationPack](decoder: SerializationPack.Decoder[P]): decoder.pack.Document => WriteConcernError = { doc =>
    (for {
      code <- decoder.int(doc, "code")
      err <- decoder.string(doc, "errmsg")
    } yield new WriteConcernError(code, err)).get
  }

  def readUpserted[P <: SerializationPack](decoder: SerializationPack.Decoder[P]): decoder.pack.Document => Upserted.Aux[P] = { document =>
    (for {
      index <- decoder.int(document, "index")
      id <- decoder.get(document, "_id")
    } yield Upserted.init[P](index, id)).get
  }
}
