package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._
import reactivemongo.bson._
import reactivemongo.utils.option

object BSONCountCommand extends CountCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONCountCommandImplicits {
  import BSONCountCommand._
  implicit object HintWriter extends BSONWriter[Hint, BSONValue] {
    def write(hint: Hint): BSONValue =
      hint match {
        case HintString(s) => BSONString(s)
        case HintDocument(doc) => doc
      }
  }
  implicit object CountWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Count]] {
    def write(count: ResolvedCollectionCommand[Count]): BSONDocument =
      BSONDocument(
        "count" -> count.collection,
        "query" ->  count.command.query,
        "limit" -> option(count.command.limit != 0, count.command.limit),
        "skip" -> option(count.command.skip != 0, count.command.skip),
        "hint" -> count.command.hint
      )
  }

  implicit object CountResultReader extends BSONDocumentReader[CountResult] {
    def read(doc: BSONDocument): CountResult = {
      println(BSONDocument.pretty(doc))
      CountResult(doc.getAs[BSONNumberLike]("n").map(_.toInt).getOrElse(0))
    }
  }
}