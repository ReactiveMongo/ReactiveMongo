package reactivemongo.api

private[reactivemongo] trait PackSupport[P <: SerializationPack with Singleton] {
  val pack: P
}
