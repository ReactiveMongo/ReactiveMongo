package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] command.
 */
@deprecated("Use the new `delete` operation", "0.16.0")
trait DeleteCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Delete(
    deletes: Seq[DeleteElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[DeleteResult]
    with Mongo26WriteCommand

  object Delete {
    def apply(firstDelete: DeleteElement, deletes: DeleteElement*): Delete =
      apply()(firstDelete, deletes: _*)

    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstDelete: DeleteElement, deletes: DeleteElement*): Delete =
      Delete(firstDelete +: deletes, ordered, writeConcern)
  }

  sealed trait DeleteElement
    extends Product2[pack.Document, Int] with Serializable {

    /** The query that matches documents to delete. */
    def q: pack.Document

    /** The number of matching documents to delete. */
    def limit: Int

    /** The collation to use for the operation. */
    def collation: Option[Collation]

    final def _1 = q
    final def _2 = limit

    def canEqual(that: Any): Boolean = that match {
      case _: DeleteElement => true
      case _                => false
    }
  }

  object DeleteElement {
    private final class Impl(
      val q: pack.Document,
      val limit: Int,
      val collation: Option[Collation]) extends DeleteElement

    def apply(
      q: pack.Document,
      limit: Int,
      collation: Option[Collation]): DeleteElement =
      new Impl(q, limit, collation)
  }

  type DeleteResult = DefaultWriteResult
}
