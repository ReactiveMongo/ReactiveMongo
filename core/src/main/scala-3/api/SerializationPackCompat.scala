package reactivemongo.api

private[api] trait SerializationPackCompat {
  _self: Singleton & SerializationPack =>

  lazy val IdentityWriter: Writer[Document]
  lazy val IdentityReader: Reader[Document]
}

private[api] object BSONCompat {
  import reactivemongo.api.bson.BSONDocument

  @inline def document(
      elements: Seq[bson.collection.BSONSerializationPack.ElementProducer]
    ) = BSONDocument(elements*)
}
