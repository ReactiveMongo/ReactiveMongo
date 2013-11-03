package reactivemongo.api

import reactivemongo.bson._

sealed trait ReadPreference {
  def slaveOk: Boolean = true
  def filterTag: Option[BSONDocument => Boolean]
}

object ReadPreference {
  object Primary extends ReadPreference {
    override def slaveOk = false
    override def filterTag = None
  }
  case class PrimaryPrefered(filterTag: Option[BSONDocument => Boolean]) extends ReadPreference
  case class Secondary(filterTag: Option[BSONDocument => Boolean]) extends ReadPreference
  case class SecondaryPrefered(filterTag: Option[BSONDocument => Boolean]) extends ReadPreference
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

  def primary: Primary.type = Primary

  def primaryPrefered: PrimaryPrefered = new PrimaryPrefered(None)

  def primaryPrefered[T](tag: T)(implicit writer: BSONDocumentWriter[T]): PrimaryPrefered =
    new PrimaryPrefered(Some(doc => doc.contains(writer.write(tag))))

  def secondary: Secondary = new Secondary(None)

  def secondary[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Secondary =
    new Secondary(Some(doc => doc.contains(writer.write(tag))))

  def secondaryPrefered: SecondaryPrefered = new SecondaryPrefered(None)

  def secondaryPrefered[T](tag: T)(implicit writer: BSONDocumentWriter[T]): SecondaryPrefered =
    new SecondaryPrefered(Some(doc => doc.contains(writer.write(tag))))

  def nearest: Nearest = new Nearest(None)

  def nearest[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Nearest =
    new Nearest(Some(doc => doc.contains(writer.write(tag))))
}