package reactivemongo.api.bson

import reactivemongo.bson.{ BSONElement, Producer }

@deprecated("Will be replace by actual new BSON API", "0.19.3")
object `package` {
  type BSONDocument = reactivemongo.bson.BSONDocument

  @inline def BSONDocument(elements: Producer[BSONElement]*) =
    reactivemongo.bson.BSONDocument(elements: _*)

  object BSONDocument {
    @inline def empty = reactivemongo.bson.BSONDocument.empty
  }

  // ---

  type BSONDocumentReader[T] = reactivemongo.bson.BSONDocumentReader[T]

  // ---

  type BSONString = reactivemongo.bson.BSONString

  @inline def BSONString(s: String) = reactivemongo.bson.BSONString(s)

  object BSONString {
    def unapply(v: reactivemongo.bson.BSONValue): Option[String] =
      v match {
        case reactivemongo.bson.BSONString(str) => Option(str)
        case _                                  => None
      }
  }

  // ---

  type BSONInteger = reactivemongo.bson.BSONInteger

  @inline def BSONInteger(i: Int) = reactivemongo.bson.BSONInteger(i)
}
