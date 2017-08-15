package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

trait FindAndModifyCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  import pack._

  case class FindAndModify(
    query: Document,
    modify: Modify,
    sort: Option[Document],
    fields: Option[Document]) extends CollectionCommand with CommandWithPack[P] with CommandWithResult[FindAndModifyResult] {
    def upsert = modify.upsert
  }

  object FindAndModify {
    def apply(query: ImplicitlyDocumentProducer, modify: Modify, sort: Option[ImplicitlyDocumentProducer] = None, fields: Option[ImplicitlyDocumentProducer] = None): FindAndModify =
      FindAndModify(
        query.produce,
        modify,
        sort.map(_.produce),
        fields.map(_.produce))
  }

  /** A modify operation, part of a FindAndModify command */
  sealed trait Modify {
    /** Only for the [[Update]] modifier */
    def upsert = false
  }

  /**
   * Update (part of a FindAndModify command).
   *
   * @param update the modifier document.
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert if true, creates a new document if no document matches the query, or if documents match the query, findAndModify performs an update
   */
  case class Update(
    update: Document,
    fetchNewObject: Boolean,
    override val upsert: Boolean) extends Modify

  object Update {
    def apply(update: ImplicitlyDocumentProducer, fetchNewObject: Boolean = false, upsert: Boolean = false): Update = Update(update.produce, fetchNewObject, upsert)
  }

  /** Remove (part of a FindAndModify command). */
  object Remove extends Modify

  case class UpdateLastError(
    updatedExisting: Boolean,
    upsertedId: Option[Any], // TODO. It is the id of the upserted value
    n: Int,
    err: Option[String])

  case class FindAndModifyResult(
    lastError: Option[UpdateLastError],
    value: Option[Document]) {
    def result[R](implicit reader: Reader[R]): Option[R] =
      value.map(pack.deserialize(_, reader))
  }
}
