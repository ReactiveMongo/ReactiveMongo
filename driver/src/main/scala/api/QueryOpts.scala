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

  /** Sets the query flags. */
  def flags(n: Int) = copy(flagsN = n)

  def skip(n: Int) = copy(skipN = n)

  def batchSize(n: Int) = copy(batchSizeN = n)

  def tailable = copy(flagsN = flagsN ^ QueryFlags.TailableCursor)

  def slaveOk = copy(flagsN = flagsN ^ QueryFlags.SlaveOk)

  def oplogReplay = copy(flagsN = flagsN ^ QueryFlags.OplogReplay)

  def noCursorTimeout = copy(flagsN = flagsN ^ QueryFlags.NoCursorTimeout)

  def awaitData = copy(flagsN = flagsN ^ QueryFlags.AwaitData)

  def exhaust = copy(flagsN = flagsN ^ QueryFlags.Exhaust)

  def partial = copy(flagsN = flagsN ^ QueryFlags.Partial)
}
