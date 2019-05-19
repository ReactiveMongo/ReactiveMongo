package reactivemongo.api

import reactivemongo.core.protocol.QueryFlags

/**
 * @param flags the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#flags flags]] representing the current options
 *
 * {{{
 * import reactivemongo.api.CursorOptions
 *
 * // Create a options to specify a cursor is tailable
 * val opts = CursorOptions.empty.tailable
 * }}}
 */
final class CursorOptions(val flags: Int) extends AnyVal {
  def tailable = copy(flags | QueryFlags.TailableCursor)

  def slaveOk = copy(flags | QueryFlags.SlaveOk)

  def oplogReplay = copy(flags | QueryFlags.OplogReplay)

  def noCursorTimeout = copy(flags | QueryFlags.NoCursorTimeout)

  def awaitData = copy(flags | QueryFlags.AwaitData)

  def exhaust = copy(flags | QueryFlags.Exhaust)

  def partial = copy(flags | QueryFlags.Partial)

  @inline private def copy(newFlags: Int) = new CursorOptions(newFlags)
}

object CursorOptions {
  val empty = new CursorOptions(0)
}
