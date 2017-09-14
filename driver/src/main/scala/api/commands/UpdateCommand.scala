package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/update/ update]] command.
 */
trait UpdateCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
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
    q: P#Document,
    u: P#Document,
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
