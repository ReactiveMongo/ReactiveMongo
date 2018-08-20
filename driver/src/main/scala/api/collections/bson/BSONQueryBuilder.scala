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
package reactivemongo.api.collections.bson

import reactivemongo.bson.BSONDocument

import reactivemongo.api.{
  Collection,
  BSONSerializationPack,
  FailoverStrategy,
  QueryOpts
}
import reactivemongo.api.collections.GenericQueryBuilder

@SerialVersionUID(1634796413L)
@deprecated("Useless, will be remove", "0.16.0")
case class BSONQueryBuilder(
  collection: Collection,
  @deprecatedName('failover) failoverStrategy: FailoverStrategy,
  queryOption: Option[BSONDocument] = None,
  sortOption: Option[BSONDocument] = None,
  projectionOption: Option[BSONDocument] = None,
  hintOption: Option[BSONDocument] = None,
  explainFlag: Boolean = false,
  snapshotFlag: Boolean = false,
  commentString: Option[String] = None,
  options: QueryOpts = QueryOpts(),
  maxTimeMsOption: Option[Long] = None) extends GenericQueryBuilder[BSONSerializationPack.type] {
  type Self = BSONQueryBuilder
  @transient val pack = BSONSerializationPack

  def copy(
    queryOption: Option[BSONDocument] = queryOption,
    sortOption: Option[BSONDocument] = sortOption,
    projectionOption: Option[BSONDocument] = projectionOption,
    hintOption: Option[BSONDocument] = hintOption,
    explainFlag: Boolean = explainFlag,
    snapshotFlag: Boolean = snapshotFlag,
    commentString: Option[String] = commentString,
    options: QueryOpts = options,
    @deprecatedName('failover) failoverStrategy: FailoverStrategy = failoverStrategy,
    maxTimeMsOption: Option[Long] = maxTimeMsOption): BSONQueryBuilder =
    BSONQueryBuilder(collection, failoverStrategy, queryOption, sortOption, projectionOption, hintOption, explainFlag, snapshotFlag, commentString, options, maxTimeMsOption)
}
