package reactivemongo.api

package object commands {
  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  type WriteConcern = GetLastError

  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  val WriteConcern = GetLastError

  @deprecated("Will be replaced by `reactivemongo.api.Collation`", "0.19.8")
  type Collation = reactivemongo.api.Collation

  @deprecated("Will be replaced by `reactivemongo.api.Collation`", "0.19.8")
  val Collation = reactivemongo.api.Collation
}
