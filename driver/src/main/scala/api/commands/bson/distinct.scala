package reactivemongo.api.commands.bson

import reactivemongo.bson.{
  BSONArray,
  BSONDocument,
  BSONDocumentWriter,
  BSONValue
}
import reactivemongo.api.{ BSONSerializationPack, ReadConcern }
import reactivemongo.api.commands.{ DistinctCommand, ResolvedCollectionCommand }

object BSONDistinctCommand extends DistinctCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONDistinctCommandImplicits {
  import BSONDistinctCommand._

  implicit object DistinctWriter
      extends BSONDocumentWriter[ResolvedCollectionCommand[Distinct]] {

    def write(distinct: ResolvedCollectionCommand[Distinct]): BSONDocument =
      BSONDocument(
        "distinct" -> distinct.collection,
        "key" -> distinct.command.keyString,
        "query" -> distinct.command.query)
  }

  implicit object DistinctResultReader
      extends DealingWithGenericCommandErrorsReader[DistinctResult] {

    def readResult(doc: BSONDocument): DistinctResult =
      DistinctResult(doc.getAs[BSONArray]("values").
        fold(List.empty[BSONValue])(_.values.toList))

  }
}
