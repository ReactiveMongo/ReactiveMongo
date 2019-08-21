package reactivemongo.api.commands

import reactivemongo.core.protocol.MongoWireVersion

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
   * @param collation the collation to use for the operation
   * @param arrayFilters an array of filter documents that determines which array elements to modify for an update operation on an array field
   */
  class UpdateElement @deprecated("Will be private/internal", "0.17.0") (
    val q: pack.Document,
    val u: pack.Document,
    val upsert: Boolean,
    val multi: Boolean,
    val collation: Option[Collation],
    val arrayFilters: Seq[pack.Document]) extends Serializable with Product4[pack.Document, pack.Document, Boolean, Boolean] {
    @deprecated("Use `q`", "0.17.0") val _1 = q
    @deprecated("Use `u`", "0.17.0") val _2 = u
    @deprecated("Use `upsert`", "0.17.0") val _3 = upsert
    @deprecated("Use `multi`", "0.17.0") val _4 = multi

    private val data = (q, u, upsert, multi, collation, arrayFilters)

    override def hashCode: Int = data.hashCode

    override def equals(that: Any): Boolean = that match {
      case other: UpdateElement => data == other.data
      case _                    => false
    }

    def canEqual(that: Any): Boolean = that match {
      case _: UpdateElement => true
      case _                => false
    }

    override def toString: String = s"UpdateElement${data.toString}"
  }

  @deprecated("Will be private/internal", "0.17.0")
  object UpdateElement {
    def apply(q: ImplicitlyDocumentProducer, u: ImplicitlyDocumentProducer, upsert: Boolean = false, multi: Boolean = false): UpdateElement =
      new UpdateElement(
        q = q.produce,
        u = u.produce,
        upsert = upsert,
        multi = multi,
        collation = None,
        arrayFilters = Seq.empty)

    def unapply(that: Any): Option[(pack.Document, pack.Document, Boolean, Boolean)] = that match {
      case other: UpdateElement =>
        Some(Tuple4(other.q, other.u, other.upsert, other.multi))

      case _ =>
        None
    }
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
    context: UpdateCommand[P], ver: MongoWireVersion)(
    builder: SerializationPack.Builder[context.pack.type]): context.UpdateElement => context.pack.Document = {
    import builder.{ boolean, document, elementProducer, pack }

    def base(element: context.UpdateElement) =
      Seq.newBuilder[pack.ElementProducer] += (
        elementProducer("q", element.q),
        elementProducer("u", element.u),
        elementProducer("upsert", boolean(element.upsert)),
        elementProducer("multi", boolean(element.multi)))

    if (ver < MongoWireVersion.V34) { element =>
      document(base(element).result())
    } else if (ver < MongoWireVersion.V36) { element =>
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

  // TODO: Unit test
  def writer[P <: SerializationPack with Singleton](pack: P)(
    context: UpdateCommand[pack.type]): (Option[Session], MongoWireVersion) => ResolvedCollectionCommand[context.Update] => pack.Document = {
    val builder = pack.newBuilder
    val writeWriteConcern = CommandCodecs.writeWriteConcern(pack)
    val writeSession = CommandCodecs.writeSession(builder)

    { (session: Option[Session], ver: MongoWireVersion) =>
      import builder.{ elementProducer => element }

      val writeElement = UpdateCommand.writeElement(context, ver)(builder)

      { update =>
        import update.command

        val ordered = builder.boolean(command.ordered)
        val elements = Seq.newBuilder[pack.ElementProducer]

        elements ++= Seq[pack.ElementProducer](
          element("update", builder.string(update.collection)),
          element("ordered", ordered))

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

        if (!session.exists(_.transaction.isSuccess)) {
          // writeConcern is not allowed within a multi-statement transaction
          // code=72

          elements += element(
            "writeConcern", writeWriteConcern(command.writeConcern))
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
