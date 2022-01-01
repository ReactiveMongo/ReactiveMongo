package reactivemongo.api

private[api] trait SerializationPackCompat {
  _self: Singleton with SerializationPack =>

  lazy val IdentityWriter: Writer[Document]
  lazy val IdentityReader: Reader[Document]
}
