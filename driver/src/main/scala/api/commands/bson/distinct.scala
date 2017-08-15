package reactivemongo.api.commands.bson

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.bson._
import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.{ DistinctCommand, ResolvedCollectionCommand }

object BSONDistinctCommand extends DistinctCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONDistinctCommandImplicits {
  import BSONDistinctCommand._

  implicit object DistinctWriter
    extends BSONDocumentWriter[ResolvedCollectionCommand[Distinct]] {

    import CommonImplicits.ReadConcernWriter

    def write(distinct: ResolvedCollectionCommand[Distinct]): BSONDocument = {
      val cmd = BSONDocument(
        "distinct" -> distinct.collection,
        "key" -> distinct.command.keyString,
        "query" -> distinct.command.query)

      if (distinct.command.version >= MongoWireVersion.V32) {
        cmd ++ ("readConcern" -> distinct.command.readConcern)
      } else cmd
    }
  }

  implicit object DistinctResultReader
    extends DealingWithGenericCommandErrorsReader[DistinctResult] {

    def readResult(doc: BSONDocument): DistinctResult =
      DistinctResult(doc.getAs[BSONArray]("values").
        fold(Stream.empty[BSONValue])(_.values))

  }
}
