/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.api

import reactivemongo.core.protocol.QueryFlags

/**
 * A helper to make the query options.
 * You may use the methods to set the fields of this class,
 * or set them directly.
 *
 * @param skipN the number of documents to skip.
 * @param batchSizeN the upper limit on the number of documents to retrieve per batch (0 for unspecified)
 * @param flagsN the query flags
 */
case class QueryOpts(
  skipN: Int = 0,
  batchSizeN: Int = 0,
  flagsN: Int = 0) extends QueryOps {

  // TODO: Merge skipN and batchSizeN with QueryBuilder
  // keep flags preparation as internal for compat < 3.2
  type Self = QueryOpts

  /** Sets the query (raw) [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#flags flags]]. */
  def flags(n: Int) = copy(flagsN = n)

  /**
   * Sets how many documents must be skipped at the beginning of the results.
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().skip(10)
   * }}}
   */
  def skip(n: Int) = copy(skipN = n)

  /**
   * Sets the size of result batches.
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().batchSize(10)
   * }}}
   */
  def batchSize(n: Int) = copy(batchSizeN = n)

  /**
   * Makes the result [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.tailable cursor tailable]].
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().tailable
   * }}}
   */
  def tailable = copy(flagsN = flagsN | QueryFlags.TailableCursor)

  /**
   * Allows querying of a replica slave ([[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.slaveOk `slaveOk`]]).
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().slaveOk
   * }}}
   */
  def slaveOk = copy(flagsN = flagsN | QueryFlags.SlaveOk)

  def oplogReplay = copy(flagsN = flagsN | QueryFlags.OplogReplay)

  /**
   * Sets the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.noTimeout `noTimeout`]] flag.
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().noCursorTimeout
   * }}}
   */
  def noCursorTimeout = copy(flagsN = flagsN | QueryFlags.NoCursorTimeout)

  /**
   * Makes the result cursor [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.awaitData await data]].
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().awaitData
   * }}}
   */
  def awaitData = copy(flagsN = flagsN | QueryFlags.AwaitData)

  /**
   * Sets the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.exhaust flag]] to return all data returned by the query at once rather than splitting the results into batches.
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().exhaust
   * }}}
   */
  def exhaust = copy(flagsN = flagsN | QueryFlags.Exhaust)

  /**
   * Sets the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.partial flag]] to return partial data from a query against a sharded cluster in which some shards do not respond rather than throwing an error.
   *
   * {{{
   * import reactivemongo.api.QueryOpts
   *
   * val opts: QueryOpts = QueryOpts().partial
   * }}}
   */
  def partial = copy(flagsN = flagsN | QueryFlags.Partial)
}
