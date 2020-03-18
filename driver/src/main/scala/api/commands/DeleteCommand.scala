package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] command.
 */
private[reactivemongo] trait DeleteCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Delete(
    deletes: Seq[DeleteElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[DeleteResult]

  /**
   * @param q the query that matches documents to delete
   * @param limit the number of matching documents to delete
   * @param collation the collation to use for the operation
   */
  final class DeleteElement(
    val q: pack.Document,
    val limit: Int,
    val collation: Option[Collation]) {
  }

  type DeleteResult = DefaultWriteResult

  // TODO: Remove
  def serialize(delete: ResolvedCollectionCommand[Delete]): pack.Document = {
    val builder = pack.newBuilder
    import builder.{ elementProducer => element }

    val elements = Seq.newBuilder[pack.ElementProducer]

    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)

    elements ++= Seq(
      element("delete", builder.string(delete.collection)),
      element("ordered", builder.boolean(delete.command.ordered)),
      element("writeConcern", writeWriteConcern(delete.command.writeConcern)))

    delete.command.deletes.headOption.foreach { first =>
      elements += element("deletes", builder.array(
        writeElement(builder, first),
        delete.command.deletes.map(writeElement(builder, _))))
    }

    builder.document(elements.result())
  }

  // TODO: Remove
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
