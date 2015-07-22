package reactivemongo.api

import reactivemongo.bson.{ BSONArray, BSONDocument, BSONDocumentWriter }

/**
 * MongoDB Read Preferences enable to read from primary or secondaries
 * with a predefined strategy.
 */
sealed trait ReadPreference {
  def slaveOk: Boolean = true
  def filterTag: Option[BSONDocument => Boolean]
}

object ReadPreference {
  /** Read only from the primary. This is the default choice. */
  object Primary extends ReadPreference {
    override def slaveOk = false
    override def filterTag = None
  }

  /** Read from the primary if it is available, or secondaries if it is not. */
  case class PrimaryPreferred(filterTag: Option[BSONDocument => Boolean]) extends ReadPreference

  /** Read only from any secondary. */
  case class Secondary(filterTag: Option[BSONDocument => Boolean]) extends ReadPreference

  /** Read from any secondary, or from the primary if they are not available. */
  case class SecondaryPreferred(filterTag: Option[BSONDocument => Boolean]) extends ReadPreference

  /**
   * Read from the faster node (ie the node which replies faster than all others), regardless its status
   * (primary or secondary).
   */
  case class Nearest(filterTag: Option[BSONDocument => Boolean]) extends ReadPreference

  private implicit class BSONDocumentWrapper(val underlying: BSONDocument) extends AnyVal {
    def contains(doc: BSONDocument): Boolean = {

      val els = underlying.elements
      doc.elements.forall { element =>
        els.find {
          case (name, value) => element._1 == name && ((element._2, value) match {
            case (d1: BSONDocument, d2: BSONDocument) => d1.elements == d2.elements
            case (a1: BSONArray, a2: BSONArray)       => a1.values == a2.values
            case (v1, v2)                             => v1 == v2
          })
        }.isDefined
      }
    }
  }

  private val defaultFilterTag = (doc: BSONDocument) => true

  /** Read only from the primary. This is the default choice. */
  def primary: Primary.type = Primary

  /** Read from the primary if it is available, or secondaries if it is not. */
  def primaryPreferred: PrimaryPreferred = new PrimaryPreferred(None)

  /**  Read from any node that has the given `tag` in the replica set (preferably the primary). */
  def primaryPreferred[T](tag: T)(implicit writer: BSONDocumentWriter[T]): PrimaryPreferred =
    new PrimaryPreferred(Some(doc => doc.contains(writer.write(tag))))

  /** Read only from any secondary. */
  def secondary: Secondary = new Secondary(None)

  /**  Read from a secondary that has the given `tag` in the replica set. */
  def secondary[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Secondary =
    new Secondary(Some(doc => doc.contains(writer.write(tag))))

  /** Read from any secondary, or from the primary if they are not available. */
  def secondaryPreferred: SecondaryPreferred = new SecondaryPreferred(None)

  /**  Read from any node that has the given `tag` in the replica set (preferably a secondary). */
  def secondaryPreferred[T](tag: T)(implicit writer: BSONDocumentWriter[T]): SecondaryPreferred =
    new SecondaryPreferred(Some(doc => doc.contains(writer.write(tag))))

  /**
   * Read from the fastest node (ie the node which replies faster than all others), regardless its status
   * (primary or secondary).
   */
  def nearest: Nearest = new Nearest(None)

  /**
   * Read from the fastest node (ie the node which replies faster than all others) that has the given `tag`,
   * regardless its status (primary or secondary).
   */
  def nearest[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Nearest =
    new Nearest(Some(doc => doc.contains(writer.write(tag))))
}
