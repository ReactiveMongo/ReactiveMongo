package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.{
  FindAndModifyCommand,
  ResolvedCollectionCommand
}
import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentWriter,
  BSONValue
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONFindAndModifyCommand
  extends FindAndModifyCommand[BSONSerializationPack.type] {
  val pack: BSONSerializationPack.type = BSONSerializationPack

  type Result = FindAndModifyCommand.Result[BSONSerializationPack.type]
}

