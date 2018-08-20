package reactivemongo.api

package object commands {
  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  type WriteConcern = GetLastError

  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  val WriteConcern = GetLastError
}
