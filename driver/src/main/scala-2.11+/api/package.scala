package reactivemongo.api

import scala.language.implicitConversions

import reactivemongo.api.bson.{
  compat,
  BSONDocument,
  BSONDocumentHandler,
  BSONDocumentReader,
  BSONDocumentWriter
}

import reactivemongo.bson.{
  BSONDocument => LegacyDocument,
  BSONDocumentHandler => LegacyDocHandler,
  BSONDocumentReader => LegacyDocReader,
  BSONDocumentWriter => LegacyDocWriter
}

object `package` extends LowPriorityPackage with PackageCompat {
  @deprecated("Unused, will be removed", "0.17.0")
  type SerializationPackObject = SerializationPack with Singleton

  @deprecated("Use reactivemongo-bson-api or use reactivemongo-bson-compat: import reactivemongo.api.bson.compat._", "0.19.0")
  implicit final def toDocumentHandler[T](implicit h: LegacyDocHandler[T]): BSONDocumentHandler[T] = compat.toDocumentHandler[T](h)

  @deprecated("Use reactivemongo-bson-api", "0.19.0")
  implicit final def toDocument(doc: LegacyDocument): BSONDocument =
    compat.toDocument(doc)
}

private[api] sealed trait LowPriorityPackage {
  @deprecated("Use reactivemongo-bson-api or use reactivemongo-bson-compat: import reactivemongo.api.bson.compat._", "0.19.0")
  implicit final def toDocumentReader[T](implicit h: LegacyDocReader[T]): BSONDocumentReader[T] = compat.toDocumentReader[T](h)

  @deprecated("Use reactivemongo-bson-api or use reactivemongo-bson-compat: import reactivemongo.api.bson.compat._", "0.19.0")
  implicit final def toDocumentWriter[T](implicit h: LegacyDocWriter[T]): BSONDocumentWriter[T] = compat.toDocumentWriter[T](h)
}
