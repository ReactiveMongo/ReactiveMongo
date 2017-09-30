package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] command.
 */
trait DeleteCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Delete(
    deletes: Seq[DeleteElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[DeleteResult] with Mongo26WriteCommand

  object Delete {
    def apply(firstDelete: DeleteElement, deletes: DeleteElement*): Delete =
      apply()(firstDelete, deletes: _*)
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstDelete: DeleteElement, deletes: DeleteElement*): Delete =
      Delete(firstDelete +: deletes, ordered, writeConcern)
  }

  case class DeleteElement( // TODO: collation
    q: P#Document,
    limit: Int)

  object DeleteElement {
    def apply(doc: ImplicitlyDocumentProducer, limit: Int = 0): DeleteElement =
      DeleteElement(doc.produce, limit)
  }

  type DeleteResult = DefaultWriteResult
}
