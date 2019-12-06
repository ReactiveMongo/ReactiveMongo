package reactivemongo.api.commands

@deprecated("Will be removed", "0.19.4")
final case class DBHashResult( // TODO: Move to `api` package
  host: String,
  collectionHashes: Map[String, String],
  md5: String,
  timeMillis: Long)
