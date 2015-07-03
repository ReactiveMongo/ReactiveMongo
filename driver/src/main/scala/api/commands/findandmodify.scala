package reactivemongo.api.commands

import scala.concurrent.Future
import reactivemongo.api.{ BSONSerializationPack, Cursor, SerializationPack }

trait FindAndModifyCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  import pack._

  case class FindAndModify(
    query: Document,
    modify: Modify,
    upsert: Boolean,
    sort: Option[Document],
    fields: Option[Document]) extends CollectionCommand with CommandWithPack[P] with CommandWithResult[FindAndModifyResult]

  object FindAndModify {
    def apply(query: ImplicitlyDocumentProducer, modify: Modify, upsert: Boolean = false, sort: Option[ImplicitlyDocumentProducer] = None, fields: Option[ImplicitlyDocumentProducer] = None): FindAndModify =
      FindAndModify(
        query.produce,
        modify,
        upsert,
        sort.map(_.produce),
        fields.map(_.produce))
  }

  /** A modify operation, part of a FindAndModify command */
  sealed trait Modify

  /**
   * Update (part of a FindAndModify command).
   *
   * @param update the modifier document.
   * @param fetchNewObject the command result must be the new object instead of the old one.
   */
  case class Update(
    update: Document,
    fetchNewObject: Boolean) extends Modify

  object Update {
    def apply(update: ImplicitlyDocumentProducer, fetchNewObject: Boolean = false): Update = Update(update.produce, fetchNewObject)
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
