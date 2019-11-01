package reactivemongo.api.gridfs

import scala.reflect.ClassTag

import reactivemongo.api.SerializationPack

import reactivemongo.api.gridfs.{ ReadFile => RF }

private[gridfs] trait GridFSSerialization[P <: SerializationPack] {
  self: GridFS[P] =>

  @annotation.implicitNotFound("Cannot resolve a file reader: make sure Id type ${Id} is a serialized value (e.g. kind of BSON value) and that a ClassTag instance is implicitly available for")
  private[api] sealed trait FileReader[Id <: P#Value] {
    def read(doc: pack.Document): ReadFile[Id]

    implicit lazy val reader = pack.reader[ReadFile[Id]](read(_))
  }

  private[api] object FileReader {
    implicit def default[Id <: pack.Value](
      implicit
      idTag: ClassTag[Id]): FileReader[Id] = {
      val underlying = RF.reader[P, Id](pack)

      new FileReader[Id] {
        def read(doc: pack.Document) = pack.deserialize(doc, underlying)
      }
    }
  }
}
