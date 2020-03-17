package reactivemongo.api.collections.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.collections.BatchCommands

@deprecated("Unused", "0.18.0")
object BSONBatchCommands extends BatchCommands[BSONSerializationPack.type] {
  import reactivemongo.api.commands.bson._

  val pack = BSONSerializationPack

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
  implicit def FindAndModifyWriter = ???
  implicit def FindAndModifyReader = ???

  val AggregationFramework = BSONAggregationFramework
  implicit def AggregateWriter = BSONAggregationImplicits.AggregateWriter
  implicit def AggregateReader =
    BSONAggregationImplicits.AggregationResultReader

  implicit def LastErrorReader = BSONGetLastErrorImplicits.LastErrorReader
}
