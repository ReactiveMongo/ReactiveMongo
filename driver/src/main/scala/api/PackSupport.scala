package reactivemongo.api

private[reactivemongo] trait PackSupport[P <: SerializationPack] {
  val pack: P
}
