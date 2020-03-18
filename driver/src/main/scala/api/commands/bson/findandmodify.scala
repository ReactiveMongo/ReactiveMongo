package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.FindAndModifyCommand

@deprecated("Internal: will be made private", "0.16.0")
object BSONFindAndModifyCommand
  extends FindAndModifyCommand[BSONSerializationPack.type] {
  val pack: BSONSerializationPack.type = BSONSerializationPack

  type Result = FindAndModifyCommand.Result[BSONSerializationPack.type]
}

