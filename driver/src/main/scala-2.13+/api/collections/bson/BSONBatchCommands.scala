package reactivemongo.api.collections.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.collections.BatchCommands

@deprecated("Unused", "0.18.0")
object BSONBatchCommands extends BatchCommands[BSONSerializationPack.type] {
  import reactivemongo.api.commands.bson._

  val pack = BSONSerializationPack

  val AggregationFramework = BSONAggregationFramework

  val CountCommand = BSONCountCommand

  val FindAndModifyCommand = BSONFindAndModifyCommand
}
