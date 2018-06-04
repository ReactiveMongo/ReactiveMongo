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

object BSONFindAndModifyCommand
  extends FindAndModifyCommand[BSONSerializationPack.type] {
  val pack: BSONSerializationPack.type = BSONSerializationPack
}

object BSONFindAndModifyImplicits {
  import BSONFindAndModifyCommand._
  implicit object FindAndModifyResultReader
    extends DealingWithGenericCommandErrorsReader[FindAndModifyResult] {

    def readResult(result: BSONDocument): FindAndModifyResult =
      FindAndModifyResult(
        result.getAs[BSONDocument]("lastErrorObject").map { doc =>
          UpdateLastError(
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
