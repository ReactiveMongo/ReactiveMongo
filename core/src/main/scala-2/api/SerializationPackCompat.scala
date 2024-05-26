package reactivemongo.api

private[api] trait SerializationPackCompat {
  _: Singleton with SerializationPack =>

  val IdentityWriter: Writer[Document]
  val IdentityReader: Reader[Document]
}

private[api] object BSONCompat {
  import reactivemongo.api.bson.BSONDocument

  @inline def document(
      elements: Seq[bson.collection.BSONSerializationPack.ElementProducer]
    ) = BSONDocument(elements: _*)
}
