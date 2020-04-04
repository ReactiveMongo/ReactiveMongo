package reactivemongo.api.commands

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{
  Collation,
  PackSupport,
  Session,
  SerializationPack,
  WriteConcern
}

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/update/ update]] command.
 */
private[reactivemongo] trait UpdateCommand[P <: SerializationPack] { _: PackSupport[P] =>

  final class Update(
    val firstUpdate: UpdateElement,
    val updates: Seq[UpdateElement],
    val ordered: Boolean,
    val writeConcern: WriteConcern,
    val bypassDocumentValidation: Boolean) extends CollectionCommand with CommandWithResult[UpdateWriteResult] {

    private[commands] lazy val tupled = Tuple5(
      firstUpdate, updates, ordered, writeConcern, bypassDocumentValidation)

    override def equals(that: Any): Boolean = that match {
      case other: this.type => other.tupled == this.tupled
      case _                => false
    }

    @inline override def hashCode: Int = tupled.hashCode
  }

  final protected[reactivemongo] type UpdateCmd = ResolvedCollectionCommand[Update]

  /**
   * @param q the query that matches the documents to update
   * @param u the modifications to apply
   * @param upsert if true perform an insert if no documents match the query
   * @param multi if true updates all the matching documents
   * @param collation the collation to use for the operation
   * @param arrayFilters an array of filter documents that determines which array elements to modify for an update operation on an array field
   */
  final class UpdateElement(
    val q: pack.Document,
    val u: pack.Document,
    val upsert: Boolean,
    val multi: Boolean,
    val collation: Option[Collation],
    val arrayFilters: Seq[pack.Document]) {

    private val data = (q, u, upsert, multi, collation, arrayFilters)

    override def hashCode: Int = data.hashCode

    override def equals(that: Any): Boolean = that match {
      case other: this.type => data == other.data
      case _                => false
    }

    override def toString: String = s"UpdateElement${data.toString}"
  }

  final protected[reactivemongo] def writeElement(
    builder: SerializationPack.Builder[pack.type]): UpdateElement => pack.Document = {
    import builder.{ boolean, document, elementProducer }

    def base(element: UpdateElement) =
      Seq.newBuilder[pack.ElementProducer] ++= Seq(
        elementProducer("q", element.q),
        elementProducer("u", element.u),
        elementProducer("upsert", boolean(element.upsert)),
        elementProducer("multi", boolean(element.multi)))

    if (maxWireVersion < MongoWireVersion.V34) { element =>
      document(base(element).result())
    } else if (maxWireVersion < MongoWireVersion.V36) { element =>
      val elements = base(element)

      element.collation.foreach { c =>
        elements += elementProducer(
          "collation",
          Collation.serializeWith(pack, c)(builder))
      }

      document(elements.result())
    } else { element => // > 3.4
      val elements = base(element)

      element.collation.foreach { c =>
        elements += elementProducer(
          "collation",
          Collation.serializeWith(pack, c)(builder))
      }

      element.arrayFilters.headOption.foreach { f =>
        elements += elementProducer(
          "arrayFilters", builder.array(f, element.arrayFilters.tail))
      }

      document(elements.result())
    }
  }

  private[reactivemongo] def session(): Option[Session]

  protected def maxWireVersion: MongoWireVersion

  implicit final private[reactivemongo] lazy val updateWriter: pack.Writer[UpdateCmd] = pack.writer[UpdateCmd](writer)

  final private def writer: ResolvedCollectionCommand[Update] => pack.Document = {
    val builder = pack.newBuilder
    val writeSession = CommandCodecs.writeSession(builder)
    val writeElement = this.writeElement(builder)
    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)

    import builder.{ elementProducer => element }

    { update =>
      import update.command

      val ordered = builder.boolean(command.ordered)
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq[pack.ElementProducer](
        element("update", builder.string(update.collection)),
        element("ordered", ordered),
        element("updates", builder.array(
          writeElement(command.firstUpdate),
          command.updates.map(writeElement))))

      session.foreach { s =>
        elements ++= writeSession(s)
      }

      if (!session.exists(_.transaction.isSuccess)) {
        // writeConcern is not allowed within a multi-statement transaction
        // code=72

        elements += element(
          "writeConcern", writeWriteConcern(command.writeConcern))
      }

      builder.document(elements.result())
    }
  }

  final protected implicit def updateReader: pack.Reader[UpdateWriteResult] = {
    val decoder = pack.newDecoder
    val readWriteError = CommandCodecs.readWriteError(decoder)
    val readWriteConcernError = CommandCodecs.readWriteConcernError(decoder)
    val readUpserted = CommandCodecs.readUpserted(decoder)

    CommandCodecs.dealingWithGenericCommandErrorsReader[pack.type, UpdateWriteResult](pack) { doc =>
      val werrors = decoder.children(doc, "writeErrors").map(readWriteError)

      val wcError = decoder.child(doc, "writeConcernError").
        map(readWriteConcernError)

      val upserted = decoder.array(doc, "upserted").map(_.flatMap { v =>
        decoder.asDocument(v).map(readUpserted)
      })

      new UpdateWriteResult(
        ok = decoder.booleanLike(doc, "ok").getOrElse(true),
        n = decoder.int(doc, "n").getOrElse(0),
        nModified = decoder.int(doc, "nModified").getOrElse(0),
        upserted = upserted.getOrElse(Seq.empty[Upserted]),
        writeErrors = werrors,
        writeConcernError = wcError,
        code = decoder.int(doc, "code"),
        errmsg = decoder.string(doc, "errmsg"))
    }
  }
}
