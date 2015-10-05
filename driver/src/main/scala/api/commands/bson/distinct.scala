package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._
import reactivemongo.bson._

object BSONDistinctCommand extends DistinctCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONDistinctCommandImplicits {
  import BSONDistinctCommand._

  implicit object DistinctWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Distinct]] {
    def write(distinct: ResolvedCollectionCommand[Distinct]): BSONDocument =
      BSONDocument(
        "distinct" -> distinct.collection,
        "key" -> distinct.command.keyString,
        "query" -> distinct.command.query)
  }

  implicit object DistinctResultReader extends DealingWithGenericCommandErrorsReader[DistinctResult] {
    def readResult(doc: BSONDocument): DistinctResult =
      DistinctResult(doc.getAs[BSONArray]("values").fold[List[BSONValue]](List())(_.values.toList))

  }
}
