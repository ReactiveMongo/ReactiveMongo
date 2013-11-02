package reactivemongo.api

import reactivemongo.bson._

sealed trait ReadPreference {
  def slaveOk: Boolean = true
  def filterTag: BSONDocument => Boolean
}

object ReadPreference {
  object Primary extends ReadPreference {
    override def slaveOk = false
    override def filterTag = _ => true
  }
  case class PrimaryPrefered(filterTag: BSONDocument => Boolean) extends ReadPreference
  case class Secondary(filterTag: BSONDocument => Boolean) extends ReadPreference
  case class SecondaryPrefered(filterTag: BSONDocument => Boolean) extends ReadPreference
  case class Nearest(filterTag: BSONDocument => Boolean) extends ReadPreference

  private implicit class BSONDocumentWrapper(val underlying: BSONDocument) extends AnyVal {
    def contains(doc: BSONDocument): Boolean = {
      val els = underlying.elements
      doc.elements.find { element =>
        els.find {
          case (name, value) => element._1 == name && ((element._2, value) match {
            case (d1: BSONDocument, d2: BSONDocument) => d1.elements == d2.elements
            case (a1: BSONArray, a2: BSONArray)       => a1.values == a2.values
            case (v1, v2)                             => v1 == v2
          })
        }.isDefined
      }.isDefined
    }
  }

  private val defaultFilterTag = (doc: BSONDocument) => true

  def primary: Primary.type = Primary

  def primaryPrefered: PrimaryPrefered = new PrimaryPrefered(defaultFilterTag)

  def primaryPrefered[T](tag: T)(implicit writer: BSONDocumentWriter[T]): PrimaryPrefered =
    new PrimaryPrefered(doc => writer.write(tag).contains(doc))

  def secondary: Secondary = new Secondary(defaultFilterTag)

  def secondary[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Secondary =
    new Secondary(doc => writer.write(tag).contains(doc))

  def secondaryPrefered: SecondaryPrefered = new SecondaryPrefered(defaultFilterTag)

  def secondaryPrefered[T](tag: T)(implicit writer: BSONDocumentWriter[T]): SecondaryPrefered =
    new SecondaryPrefered(doc => writer.write(tag).contains(doc))

  def nearest: Nearest = new Nearest(defaultFilterTag)

  def nearest[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Nearest =
    new Nearest(doc => writer.write(tag).contains(doc))
}