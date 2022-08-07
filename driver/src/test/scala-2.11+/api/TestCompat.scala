package reactivemongo.api

import java.util.Date

import reactivemongo.api.bson.{ BSONReader, Producer }

// Use to migration test between legacy BSON API to new one
object TestCompat {
  type DefaultCollection = Serialization.DefaultCollection

  type BSONValue = reactivemongo.api.bson.BSONValue

  @inline def BSONArray(values: Producer[BSONValue]*) =
    reactivemongo.api.bson.BSONArray(values: _*)

  @inline def BSONBoolean(b: Boolean) = reactivemongo.api.bson.BSONBoolean(b)

  type BSONObjectID = reactivemongo.api.bson.BSONObjectID

  @inline def BSONJavaScript(code: String) =
    reactivemongo.api.bson.BSONJavaScript(code)

  type BSONJavaScript = reactivemongo.api.bson.BSONJavaScript

  def dateReader: BSONReader[Date] =
    implicitly[BSONReader[java.time.Instant]].afterRead { i =>
      new Date(i.toEpochMilli)
    }

  @inline def BSONObjectID(data: Array[Byte]) =
    reactivemongo.api.bson.BSONObjectID.parse(data).get
}
