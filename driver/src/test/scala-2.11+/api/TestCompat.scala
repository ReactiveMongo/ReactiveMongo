package reactivemongo.api

import java.util.Date

import reactivemongo.api.bson.{ BSONReader, Producer }

// Use to migration test between legacy BSON API to new one
object TestCompat {
  type BSONCollection = reactivemongo.api.collections.GenericCollection[Serialization.Pack]

  type BSONValue = reactivemongo.api.bson.BSONValue

  type BSONDocument = reactivemongo.api.bson.BSONDocument

  @inline def BSONDocument(elements: reactivemongo.api.bson.ElementProducer*) =
    reactivemongo.api.bson.BSONDocument(elements: _*)

  object BSONDocument {
    @inline def empty = reactivemongo.api.bson.BSONDocument.empty
  }

  @inline def BSONArray(values: Producer[BSONValue]*) =
    reactivemongo.api.bson.BSONArray(values: _*)

  @inline def BSONBoolean(b: Boolean) = reactivemongo.api.bson.BSONBoolean(b)

  type BSONString = reactivemongo.api.bson.BSONString

  @inline def BSONString(s: String) = reactivemongo.api.bson.BSONString(s)

  object BSONString {
    def unapply(v: reactivemongo.api.bson.BSONValue): Option[String] =
      v match {
        case reactivemongo.api.bson.BSONString(str) => Option(str)
        case _                                      => None
      }
  }

  @inline def BSONInteger(i: Int) = reactivemongo.api.bson.BSONInteger(i)

  type BSONObjectID = reactivemongo.api.bson.BSONObjectID

  @inline def BSONJavaScript(code: String) =
    reactivemongo.api.bson.BSONJavaScript(code)

  type BSONJavaScript = reactivemongo.api.bson.BSONJavaScript

  def dateReader: BSONReader[Date] = implicitly[BSONReader[java.time.Instant]].afterRead { i => new Date(i.toEpochMilli) }

  @inline def BSONObjectID(data: Array[Byte]) =
    reactivemongo.api.bson.BSONObjectID.parse(data).get
}
