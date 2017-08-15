package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._

object BSONCountCommand extends CountCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONCountCommandImplicits {
  import reactivemongo.bson.{
    BSONDocument,
    BSONDocumentWriter,
    BSONNumberLike,
    BSONString,
    BSONValue,
    BSONWriter
  }
  import BSONCountCommand._

  implicit object HintWriter extends BSONWriter[Hint, BSONValue] {
    def write(hint: Hint): BSONValue = hint match {
      case HintString(s)     => BSONString(s)
      case HintDocument(doc) => doc
    }
  }

  implicit object CountWriter
    extends BSONDocumentWriter[ResolvedCollectionCommand[Count]] {

    def write(count: ResolvedCollectionCommand[Count]): BSONDocument =
      BSONDocument(
        "count" -> count.collection,
        "query" -> count.command.query,
        "limit" -> count.command.limit,
        "skip" -> count.command.skip,
        "hint" -> count.command.hint)
  }

  implicit object CountResultReader
    extends DealingWithGenericCommandErrorsReader[CountResult] {

    def readResult(doc: BSONDocument): CountResult =
      CountResult(doc.getAs[BSONNumberLike]("n").map(_.toInt).getOrElse(0))

  }
}
