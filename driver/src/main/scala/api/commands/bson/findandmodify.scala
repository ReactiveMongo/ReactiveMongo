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

@deprecated("Internal: will be made private", "0.16.0")
object BSONFindAndModifyImplicits {
  import BSONFindAndModifyCommand._
  implicit object FindAndModifyResultReader
    extends DealingWithGenericCommandErrorsReader[BSONFindAndModifyCommand.Result] {

    import BSONFindAndModifyCommand.Result

    def readResult(result: BSONDocument): Result =
      FindAndModifyCommand.Result[BSONSerializationPack.type](
        BSONSerializationPack)(
        result.getAs[BSONDocument]("lastErrorObject").map { doc =>
          FindAndModifyCommand.UpdateLastError(
            updatedExisting = doc.getAs[Boolean]("updatedExisting").getOrElse(false),
            n = doc.getAs[Int]("n").getOrElse(0),
            err = doc.getAs[String]("err"),
            upsertedId = doc.getAs[BSONValue]("upserted"))
        },
        result.getAs[BSONDocument]("value"))
  }

  @deprecated("Do not use", "0.14.0")
  object FindAndModifyWriter
    extends BSONDocumentWriter[ResolvedCollectionCommand[FindAndModify]] {

    def write(cmd: ResolvedCollectionCommand[FindAndModify]): BSONDocument = ???
  }
}
