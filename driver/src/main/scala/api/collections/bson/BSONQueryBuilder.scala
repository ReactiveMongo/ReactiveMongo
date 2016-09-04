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

import reactivemongo.bson.{
  BSONArray,
  BSONBoolean,
  BSONDocument,
  BSONElement,
  BSONLong,
  BSONString
}

import reactivemongo.api.{
  Collection,
  BSONSerializationPack,
  FailoverStrategy,
  QueryOpts,
  ReadPreference
}
import reactivemongo.api.collections.GenericQueryBuilder

case class BSONQueryBuilder(
  collection: Collection,
  failover: FailoverStrategy,
  queryOption: Option[BSONDocument] = None,
  sortOption: Option[BSONDocument] = None,
  projectionOption: Option[BSONDocument] = None,
  hintOption: Option[BSONDocument] = None,
  explainFlag: Boolean = false,
  snapshotFlag: Boolean = false,
  commentString: Option[String] = None,
  options: QueryOpts = QueryOpts(),
  maxTimeMsOption: Option[Long] = None
)
    extends GenericQueryBuilder[BSONSerializationPack.type] {
  import reactivemongo.util.option

  type Self = BSONQueryBuilder
  val pack = BSONSerializationPack

  def copy(
    queryOption: Option[BSONDocument] = queryOption,
    sortOption: Option[BSONDocument] = sortOption,
    projectionOption: Option[BSONDocument] = projectionOption,
    hintOption: Option[BSONDocument] = hintOption,
    explainFlag: Boolean = explainFlag,
    snapshotFlag: Boolean = snapshotFlag,
    commentString: Option[String] = commentString,
    options: QueryOpts = options,
    failover: FailoverStrategy = failover,
    maxTimeMsOption: Option[Long] = maxTimeMsOption
  ): BSONQueryBuilder =
    BSONQueryBuilder(collection, failover, queryOption, sortOption, projectionOption, hintOption, explainFlag, snapshotFlag, commentString, options, maxTimeMsOption)

  def merge(readPreference: ReadPreference): BSONDocument = {
    // Primary and SecondaryPreferred are encoded as the slaveOk flag;
    // the others are encoded as $readPreference field.

    def pref = {
      val mode = readPreference match {
        case ReadPreference.Primary                    => "primary"
        case ReadPreference.PrimaryPreferred(filter)   => "primaryPreferred"
        case ReadPreference.Secondary(filter)          => "secondary"
        case ReadPreference.SecondaryPreferred(filter) => "secondaryPreferred"
        case ReadPreference.Nearest(filter)            => "nearest"
      }
      val base = Seq[BSONElement]("mode" -> BSONString(mode))

      BSONDocument(readPreference match {
        case ReadPreference.Taggable(tagSet) =>
          base :+ BSONElement("tags", BSONArray(
            tagSet.map(tags => BSONDocument(tags.toList.map {
              case (k, v) => k -> BSONString(v)
            }))
          ))

        case _ => base
      })
    }

    val optional = List(
      queryOption.map { "$query" -> _ },
      sortOption.map { "$orderby" -> _ },
      hintOption.map { "$hint" -> _ },
      maxTimeMsOption.map { "$maxTimeMS" -> BSONLong(_) },
      commentString.map { "$comment" -> BSONString(_) },
      option(explainFlag, "$explain" -> BSONBoolean(true)),
      option(snapshotFlag, "$snapshot" -> BSONBoolean(true))
    ).flatten

    BSONDocument(optional :+ ("$readPreference" -> pref))
  }
}
