package reactivemongo.api

/** Operations about query. */
trait QueryOps {
  type Self

  /** Sets the number of documents to skip. */
  def skip(n: Int): Self

  /** Sets an upper limit on the number of documents to retrieve per batch. Defaults to 0 (meaning no upper limit - MongoDB decides). */
  def batchSize(n: Int): Self

  /** Toggles TailableCursor: Makes the cursor not to close after all the data is consumed. */
  def tailable: Self

  /** Toggles SlaveOk: The query is might be run on a secondary. */
  def slaveOk: Self

  /** Toggles OplogReplay */
  def oplogReplay: Self

  /** Toggles NoCursorTimeout: The cursor will not expire automatically */
  def noCursorTimeout: Self

  /**
   * Toggles AwaitData: Block a little while waiting for more data instead of returning immediately if no data.
   * Use along with TailableCursor.
   */
  def awaitData: Self

  /** Toggles Exhaust */
  def exhaust: Self

  /**
   * Sets the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.partial flag]] to return partial data from a query against a sharded cluster in which some shards do not respond rather than throwing an error.
   *
   * {{{
   * import reactivemongo.api.QueryOps
   *
   * def partial[T <: QueryOps](ops: QueryOps): ops.Self =
   *   ops.allowPartialResults
   * }}}
   */
  def allowPartialResults: Self
}
