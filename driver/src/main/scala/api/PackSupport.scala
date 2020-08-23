package reactivemongo.api

private[reactivemongo] trait PackSupport[P <: SerializationPack] {
  /**
   * The serialization pack ([[https://javadoc.io/doc/org.reactivemongo/reactivemongo-bson-api_2.12/latest/index.html BSON]] by default).
   *
   * Used to resolve the types of values
   * (by default `BSONValue`, `BSONDocument`, ...), and the related typeclasses
   * (e.g. `BSONDocumentReader` ...).
   */
  val pack: P
}
