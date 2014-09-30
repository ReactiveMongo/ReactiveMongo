package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._
import reactivemongo.bson._

object BSONFindAndModifyCommand extends FindAndModifyCommand[BSONSerializationPack.type] {
  val pack: BSONSerializationPack.type = BSONSerializationPack
}

object BSONFindAndModifyImplicits {
  import BSONFindAndModifyCommand._
  implicit object FindAndModifyResultReader extends BSONDocumentReader[FindAndModifyResult] {
    def read(result: BSONDocument): FindAndModifyResult = {
      FindAndModifyResult(
        result.getAs[BSONDocument]("lastErrorObject").map { doc =>
          UpdateLastError(
            updatedExisting = doc.getAs[Boolean]("updatedExisting").getOrElse(false),
            n = doc.getAs[Int]("n").getOrElse(0),
            err = doc.getAs[String]("err"),
            upsertedId = doc.getAs[BSONValue]("upserted")
          )
        },
        result.getAs[BSONDocument]("value")
      )
    }
  }
  implicit object FindAndModifyWriter extends BSONDocumentWriter[ResolvedCollectionCommand[FindAndModify]] {
    def write(command: ResolvedCollectionCommand[FindAndModify]): BSONDocument =
      BSONDocument(
        "findAndModify" -> command.collection,
        "query" -> command.command.query,
        "sort" -> command.command.sort,
        "fields" -> command.command.fields,
        "upsert" -> (if(command.command.upsert) Some(true) else None)
      ) ++ (command.command.modify match {
        case Update(document, fetchNewObject) =>
          BSONDocument(
            "update" -> document,
            "new" -> fetchNewObject
          )
        case Remove =>
          BSONDocument("remove" -> true)
      })
  }
}