package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/insert/ insert]] command.
 */
trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  /**
   * @param head the first mandatory document
   * @param tail maybe other documents
   */
  case class Insert( // TODO: bypassDocumentValidation: bool
    head: pack.Document,
    tail: Seq[pack.Document],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[InsertResult] with Mongo26WriteCommand {

    @deprecated("Use head+tail", "0.12.7")
    def this(
      documents: Seq[pack.Document],
      ordered: Boolean,
      writeConcern: WriteConcern) = this(
      documents.head, documents.tail, ordered, writeConcern)

    @deprecated("Use head+tail", "0.12.7")
    def documents: Seq[pack.Document] = head +: tail
  }

  type InsertResult = DefaultWriteResult // for simplified imports

  private[reactivemongo] def serialize(
    insert: ResolvedCollectionCommand[Insert])(
    implicit
    wcw: pack.Writer[WriteConcern]): pack.Document = {

    val builder = pack.newBuilder
    val documents = builder.array(insert.command.head, insert.command.tail)
    val ordered = builder.boolean(insert.command.ordered)
    val wc = pack.serialize(insert.command.writeConcern, wcw)

    val elements = Seq[pack.ElementProducer](
      builder.elementProducer("insert", builder.string(insert.collection)),
      builder.elementProducer("documents", documents),
      builder.elementProducer("ordered", ordered),
      builder.elementProducer("writeConcern", wc))

    // TODO: 3.4; bypassDocumentValidation: boolean

    builder.document(elements)
  }

  object Insert {
    @deprecated("Use the default [[Insert]] constructor", "0.12.7")
    def apply(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert = apply()(firstDoc, otherDocs: _*)

    @deprecated("Use the default [[Insert]] constructor", "0.12.7")
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert = new Insert(firstDoc.produce #:: otherDocs.toStream.map(_.produce), ordered, writeConcern)
  }
}
