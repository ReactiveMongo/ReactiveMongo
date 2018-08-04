package reactivemongo.api.commands

import reactivemongo.api.{ Session, SerializationPack }

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/update/ update]] command.
 */
@deprecated("Use the new update operation", "0.16.0")
trait UpdateCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  val pack: P

  case class Update( // TODO: bypassDocumentValidation: bool
    @deprecatedName('documents) updates: Seq[UpdateElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[UpdateResult] with Mongo26WriteCommand {

    @deprecated(message = "Use [[updates]]", since = "0.12.7")
    def documents = updates
  }

  type UpdateResult = UpdateWriteResult

  /**
   * @param q the query that matches the documents to update
   * @param u the modifications to apply
   * @param upsert if true perform an insert if no documents match the query
   * @param multi if true updates all the matching documents
   */
  case class UpdateElement(
    q: pack.Document,
    u: pack.Document,
    upsert: Boolean,
    multi: Boolean)

  object UpdateElement {
    def apply(q: ImplicitlyDocumentProducer, u: ImplicitlyDocumentProducer, upsert: Boolean = false, multi: Boolean = false): UpdateElement =
      UpdateElement(
        q.produce,
        u.produce,
        upsert,
        multi)
  }

  object Update {
    def apply(firstUpdate: UpdateElement, updates: UpdateElement*): Update =
      apply()(firstUpdate, updates: _*)
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstUpdate: UpdateElement, updates: UpdateElement*): Update =
      Update(
        firstUpdate +: updates,
        ordered,
        writeConcern)
  }
}

private[reactivemongo] object UpdateCommand {
  def writeElement[P <: SerializationPack with Singleton](
    context: UpdateCommand[P])(
    builder: SerializationPack.Builder[context.pack.type]): context.UpdateElement => context.pack.Document = { element =>
    import builder.{ boolean, document, elementProducer, pack }

    document(Seq[pack.ElementProducer](
      elementProducer("q", element.q),
      elementProducer("u", element.u),
      elementProducer("upsert", boolean(element.upsert)),
      elementProducer("multi", boolean(element.multi))))

  }

  // TODO: Unit test
  def writer[P <: SerializationPack with Singleton](pack: P)(
    context: UpdateCommand[pack.type]): Option[Session] => ResolvedCollectionCommand[context.Update] => pack.Document = {
    val builder = pack.newBuilder
    val writeWriteConcern = CommandCodecs.writeWriteConcern(pack)
    val writeSession = CommandCodecs.writeSession(builder)

    { session: Option[Session] =>
      import builder.{ elementProducer => element }

      { update =>
        import update.command

        val writeElement = UpdateCommand.writeElement(context)(builder)
        val ordered = builder.boolean(command.ordered)
        val elements = Seq.newBuilder[pack.ElementProducer]

        elements ++= Seq[pack.ElementProducer](
          element("update", builder.string(update.collection)),
          element("ordered", ordered),
          element("writeConcern", writeWriteConcern(command.writeConcern)))

        command.updates match {
          case first +: updates =>
            elements += element("updates", builder.array(
              writeElement(first), updates.map(writeElement)))

          case _ =>
            elements += element(
              "updates",
              builder.string("_used_by_UpdateOps_to_compute_bson_size"))
        }

        session.foreach { s =>
          elements ++= writeSession(s)
        }

        builder.document(elements.result())
      }
    }
  }

  def reader[P <: SerializationPack with Singleton](pack: P)(
    context: UpdateCommand[pack.type]): pack.Reader[context.UpdateResult] = {
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

      UpdateWriteResult(
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
