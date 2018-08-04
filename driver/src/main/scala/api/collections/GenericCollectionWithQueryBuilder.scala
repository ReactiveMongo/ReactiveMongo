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
package reactivemongo.api.collections

import reactivemongo.api.{
  FailoverStrategy,
  QueryOpts,
  ReadConcern,
  SerializationPack
}

private[api] trait GenericCollectionWithQueryBuilder[P <: SerializationPack with Singleton] { parent: GenericCollection[P] =>

  protected final class CollectionQueryBuilder(
    val failover: FailoverStrategy,
    val queryOption: Option[pack.Document] = None,
    val sortOption: Option[pack.Document] = None,
    val projectionOption: Option[pack.Document] = None,
    val hintOption: Option[pack.Document] = None,
    val explainFlag: Boolean = false,
    val snapshotFlag: Boolean = false,
    val commentString: Option[String] = None,
    val options: QueryOpts = QueryOpts(),
    val maxTimeMsOption: Option[Long] = None,
    override val readConcern: ReadConcern = parent.readConcern) extends GenericQueryBuilder[pack.type] {
    type Self = CollectionQueryBuilder
    val pack: parent.pack.type = parent.pack
    override val collection = parent
    @inline override def readPreference = parent.readPreference

    def copy(
      queryOption: Option[pack.Document] = queryOption,
      sortOption: Option[pack.Document] = sortOption,
      projectionOption: Option[pack.Document] = projectionOption,
      hintOption: Option[pack.Document] = hintOption,
      explainFlag: Boolean = explainFlag,
      snapshotFlag: Boolean = snapshotFlag,
      commentString: Option[String] = commentString,
      options: QueryOpts = options,
      failover: FailoverStrategy = failover,
      maxTimeMsOption: Option[Long] = maxTimeMsOption): CollectionQueryBuilder =
      new CollectionQueryBuilder(failover, queryOption, sortOption,
        projectionOption, hintOption, explainFlag, snapshotFlag, commentString,
        options, maxTimeMsOption, readConcern)

  }
}
