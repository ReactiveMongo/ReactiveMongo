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

import reactivemongo.api.{ Collection, DB, FailoverStrategy, QueryOpts, ReadPreference }
import reactivemongo.api.commands.bson._
import reactivemongo.api.collections.{ BatchCommands, GenericCollection, GenericCollectionProducer, GenericQueryBuilder }
import reactivemongo.api.BSONSerializationPack
import reactivemongo.bson._

object `package` {
  implicit object BSONCollectionProducer extends GenericCollectionProducer[BSONSerializationPack.type, BSONCollection] {
    def apply(db: DB, name: String, failoverStrategy: FailoverStrategy = FailoverStrategy()): BSONCollection =
      BSONCollection(db, name, failoverStrategy)
  }
}

object BSONBatchCommands extends BatchCommands[BSONSerializationPack.type] {
  val pack = BSONSerializationPack

  val CountCommand = BSONCountCommand
  implicit def CountWriter = BSONCountCommandImplicits.CountWriter
  implicit def CountResultReader = BSONCountCommandImplicits.CountResultReader

  val InsertCommand = BSONInsertCommand
  implicit def InsertWriter = BSONInsertCommandImplicits.InsertWriter

  val UpdateCommand = BSONUpdateCommand
  implicit def UpdateWriter = BSONUpdateCommandImplicits.UpdateWriter
  implicit def UpdateReader = BSONUpdateCommandImplicits.UpdateResultReader

  val DeleteCommand = BSONDeleteCommand
  implicit def DeleteWriter = BSONDeleteCommandImplicits.DeleteWriter
  implicit def DefaultWriteResultReader = BSONCommonWriteCommandsImplicits.DefaultWriteResultReader

  val FindAndModifyCommand = BSONFindAndModifyCommand
  implicit def FindAndModifyWriter = BSONFindAndModifyImplicits.FindAndModifyWriter
  implicit def FindAndModifyReader = BSONFindAndModifyImplicits.FindAndModifyResultReader

  implicit def LastErrorReader = BSONGetLastErrorImplicits.LastErrorReader
}

case class BSONCollection(val db: DB, val name: String, val failoverStrategy: FailoverStrategy) extends GenericCollection[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
  val BatchCommands = BSONBatchCommands
  def genericQueryBuilder = BSONQueryBuilder(this, failoverStrategy)
}

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
    options: QueryOpts = QueryOpts()) extends GenericQueryBuilder[BSONSerializationPack.type] {
  import reactivemongo.utils.option

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
    failover: FailoverStrategy = failover): BSONQueryBuilder =
    BSONQueryBuilder(collection, failover, queryOption, sortOption, projectionOption, hintOption, explainFlag, snapshotFlag, commentString, options)

  def merge(readPreference: ReadPreference): BSONDocument = {
    // Primary and SecondaryPreferred are encoded as the slaveOk flag;
    // the others are encoded as $readPreference field.
    val readPreferenceDocument = readPreference match {
      case ReadPreference.Primary                    => None
      case ReadPreference.PrimaryPreferred(filter)   => Some(BSONDocument("mode" -> "primaryPreferred"))
      case ReadPreference.Secondary(filter)          => Some(BSONDocument("mode" -> "secondary"))
      case ReadPreference.SecondaryPreferred(filter) => None
      case ReadPreference.Nearest(filter)            => Some(BSONDocument("mode" -> "nearest"))
    }
    val optionalFields = List(
      sortOption.map { "$orderby" -> _ },
      hintOption.map { "$hint" -> _ },
      commentString.map { "$comment" -> BSONString(_) },
      option(explainFlag, "$explain" -> BSONBoolean(true)),
      option(snapshotFlag, "$snapshot" -> BSONBoolean(true)),
      readPreferenceDocument.map { "$readPreference" -> _ }).flatten
    val query = queryOption.getOrElse(BSONDocument())
    if (optionalFields.isEmpty)
      query
    else
      BSONDocument(("$query" -> query) :: optionalFields)
  }
}
