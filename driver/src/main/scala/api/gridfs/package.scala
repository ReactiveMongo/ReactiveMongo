package reactivemongo.api.gridfs

import reactivemongo.util.LazyLogger

import reactivemongo.api.BSONSerializationPack

import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONElement,
  BSONValue,
  Producer
}

object `package` {
  private[gridfs] val logger = LazyLogger("reactivemongo.api.gridfs")

  @deprecated("Unused", "0.19.0")
  type IdProducer[Id] = Tuple2[String, Id] => Producer[BSONElement]
}

object Implicits {
  @deprecated("Use reactivemongo.api.bson types", "0.19.0")
  implicit object DefaultReadFileReader extends BSONDocumentReader[ReadFile[BSONSerializationPack.type, BSONValue]] {
    val underlying = ReadFile.reader[BSONSerializationPack.type, BSONValue](BSONSerializationPack)

    def read(doc: BSONDocument) = underlying.read(doc)
  }
}
