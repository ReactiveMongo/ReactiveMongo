package reactivemongo.api

import reactivemongo.bson.{ BSONElement, BSONValue, Producer }

object TestCompat {
  type DefaultCollection = Serialization.DefaultCollection

  @inline def BSONArray(values: Producer[BSONValue]*) =
    reactivemongo.bson.BSONArray(values: _*)

  @inline def BSONBoolean(b: Boolean) = reactivemongo.bson.BSONBoolean(b)

  type BSONObjectID = reactivemongo.bson.BSONObjectID

  @inline def BSONJavaScript(code: String) =
    reactivemongo.bson.BSONJavaScript(code)

  type BSONJavaScript = reactivemongo.bson.BSONJavaScript

  @inline def dateReader = implicitly[reactivemongo.bson.BSONReader[_ <: BSONValue, java.util.Date]]

  @inline def BSONObjectID(data: Array[Byte]) =
    reactivemongo.bson.BSONObjectID(data)
}
