package reactivemongo.api

private[api] trait SerializationPackCompat {
  _: Singleton with SerializationPack =>

  val IdentityWriter: Writer[Document]
  val IdentityReader: Reader[Document]
}
