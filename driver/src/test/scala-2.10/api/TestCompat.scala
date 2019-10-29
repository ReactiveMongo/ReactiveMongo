package reactivemongo.api

import reactivemongo.bson.{ BSONElement, BSONValue, Producer }

object TestCompat {
  type BSONCollection = reactivemongo.api.collections.GenericCollection[Compat.SerializationPack]

  type BSONDocument = reactivemongo.bson.BSONDocument

  @inline def BSONDocument(elements: Producer[BSONElement]*) =
    reactivemongo.bson.BSONDocument(elements: _*)

  object BSONDocument {
    @inline def empty = reactivemongo.bson.BSONDocument.empty
  }

  @inline def BSONArray(values: Producer[BSONValue]*) =
    reactivemongo.bson.BSONArray(values: _*)

  @inline def BSONBoolean(b: Boolean) = reactivemongo.bson.BSONBoolean(b)

  type BSONString = reactivemongo.bson.BSONString

  @inline def BSONString(s: String) = reactivemongo.bson.BSONString(s)

  object BSONString {
    def unapply(v: reactivemongo.bson.BSONValue): Option[String] =
      v match {
        case reactivemongo.bson.BSONString(str) => Option(str)
        case _                                  => None
      }
  }

  @inline def BSONInteger(i: Int) = reactivemongo.bson.BSONInteger(i)

  type BSONObjectID = reactivemongo.bson.BSONObjectID

  @inline def BSONJavaScript(code: String) =
    reactivemongo.bson.BSONJavaScript(code)

  type BSONJavaScript = reactivemongo.bson.BSONJavaScript

  @inline def dateReader = implicitly[reactivemongo.bson.BSONReader[_ <: BSONValue, java.util.Date]]

  @inline def BSONObjectID(data: Array[Byte]) =
    reactivemongo.bson.BSONObjectID(data)
}
