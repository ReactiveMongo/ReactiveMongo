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

import reactivemongo.api.{ DB, FailoverStrategy }
import reactivemongo.api.commands.bson._
import reactivemongo.api.collections.{
  BatchCommands,
  GenericCollection,
  GenericCollectionProducer
}
import reactivemongo.api.BSONSerializationPack

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

  val DistinctCommand = BSONDistinctCommand
  implicit def DistinctWriter = BSONDistinctCommandImplicits.DistinctWriter
  implicit def DistinctResultReader = BSONDistinctCommandImplicits.DistinctResultReader

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

  val AggregationFramework = BSONAggregationFramework
  implicit def AggregateWriter = BSONAggregationImplicits.AggregateWriter
  implicit def AggregateReader =
    BSONAggregationImplicits.AggregationResultReader

  implicit def LastErrorReader = BSONGetLastErrorImplicits.LastErrorReader
}

case class BSONCollection(val db: DB, val name: String, val failoverStrategy: FailoverStrategy) extends GenericCollection[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
  val BatchCommands = BSONBatchCommands
  def genericQueryBuilder = BSONQueryBuilder(this, failoverStrategy)
}
