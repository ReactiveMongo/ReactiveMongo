package reactivemongo.api.commands

import reactivemongo.api.{
  Collation,
  PackSupport,
  SerializationPack,
  Session,
  WriteConcern
}

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] command.
 */
private[reactivemongo] trait DeleteCommand[P <: SerializationPack] { self: PackSupport[P] =>

  private[api] final class Delete(
    val deletes: Seq[DeleteElement],
    val ordered: Boolean,
    val writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[DeleteResult]

  /**
   * @param q the query that matches documents to delete
   * @param limit the number of matching documents to delete
   * @param collation the collation to use for the operation
   */
  final class DeleteElement private[api] (
    val q: pack.Document,
    val limit: Int,
    val collation: Option[Collation]) {
  }

  final type DeleteResult = DefaultWriteResult

  protected final type DeleteCmd = ResolvedCollectionCommand[Delete]

  private[reactivemongo] def session(): Option[Session]

  protected final implicit lazy val deleteWriter: pack.Writer[DeleteCmd] =
    deleteWriter(self.session)

  protected final def deleteWriter(
    session: Option[Session]): pack.Writer[DeleteCmd] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => element }

    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)
    val writeSession = CommandCodecs.writeSession(builder)

    pack.writer[DeleteCmd] { delete =>
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("delete", builder.string(delete.collection)),
        element("ordered", builder.boolean(delete.command.ordered)),
        element("writeConcern", writeWriteConcern(delete.command.writeConcern)))

      session.foreach { s =>
        elements ++= writeSession(s)
      }

      delete.command.deletes.headOption.foreach { first =>
        elements += element("deletes", builder.array(
          writeElement(builder, first) +:
            delete.command.deletes.map(writeElement(builder, _))))
      }

      builder.document(elements.result())
    }
  }

  private[api] def writeElement(
    builder: SerializationPack.Builder[pack.type],
    e: DeleteElement): pack.Document = {

    import builder.{ elementProducer => element }

    val elements = Seq.newBuilder[pack.ElementProducer]

    elements ++= Seq(
      element("q", e.q),
      element("limit", builder.int(e.limit)))

    e.collation.foreach { c =>
      elements += element(
        "collation",
        Collation.serializeWith(pack, c)(builder))
    }

    builder.document(elements.result())
  }
}
