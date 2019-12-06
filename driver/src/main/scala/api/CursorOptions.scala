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
 *
 * @define enableFlag Enable the flag
 */
final class CursorOptions(val flags: Int) extends AnyVal {
  /** $enableFlag [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/index.html#DBQuery.Option.tailable `tailable`]] flag */
  def tailable = copy(flags | QueryFlags.TailableCursor)

  /** $enableFlag [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/index.html#DBQuery.Option.slaveOk `slaveOk`]] flag */
  def slaveOk = copy(flags | QueryFlags.SlaveOk)

  def oplogReplay = copy(flags | QueryFlags.OplogReplay)

  /** $enableFlag [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/index.html#DBQuery.Option.noTimeout `noTimeout`]] flag */
  def noCursorTimeout = copy(flags | QueryFlags.NoCursorTimeout)

  /** $enableFlag [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/index.html#DBQuery.Option.awaitData `awaitData`]] flag */
  def awaitData = copy(flags | QueryFlags.AwaitData)

  /** $enableFlag [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/index.html#DBQuery.Option.exhaust `exhaust`]] flag */
  def exhaust = copy(flags | QueryFlags.Exhaust)

  /** $enableFlag [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/index.html#DBQuery.Option.partial `partial`]] flag */
  def partial = copy(flags | QueryFlags.Partial)

  @inline private def copy(newFlags: Int) = new CursorOptions(newFlags)
}

object CursorOptions {
  /** Creates empty cursor options */
  val empty = new CursorOptions(0)
}
