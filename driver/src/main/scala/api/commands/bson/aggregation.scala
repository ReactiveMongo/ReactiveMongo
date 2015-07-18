package reactivemongo.api.commands.bson

import reactivemongo.bson.{ BSONDocument, BSONElement, BSONValue, Producer }

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.AggregationFramework

object BSONAggregationFramework
    extends AggregationFramework[BSONSerializationPack.type] {

  import reactivemongo.bson.{ BSONBoolean, BSONDouble, BSONInteger, BSONString }

  val pack: BSONSerializationPack.type = BSONSerializationPack

  protected def makeDocument(elements: Seq[Producer[BSONElement]]) =
    BSONDocument(elements: _*)

  protected def elementProducer(name: String, value: BSONValue) =
    name -> value

  protected def booleanValue(b: Boolean): BSONValue = BSONBoolean(b)
  protected def intValue(i: Int): BSONValue = BSONInteger(i)
  protected def longValue(l: Long): BSONValue = BSONDouble(l)
  protected def doubleValue(d: Double): BSONValue = BSONDouble(d)
  protected def stringValue(s: String): BSONValue = BSONString(s)
}

object BSONAggregationImplicits {
  import reactivemongo.api.commands.ResolvedCollectionCommand
  import BSONAggregationFramework.{ Aggregate, AggregationResult }
  import reactivemongo.bson.{
    BSONArray,
    BSONBoolean,
    BSONDocumentReader,
    BSONDocumentWriter,
    BSONInteger,
    BSONString
  }

  implicit object AggregateWriter
      extends BSONDocumentWriter[ResolvedCollectionCommand[Aggregate]] {
    def write(agg: ResolvedCollectionCommand[Aggregate]) = BSONDocument(
      "aggregate" -> BSONString(agg.collection),
      "pipeline" -> BSONArray(
        { for (pipe <- agg.command.pipeline) yield pipe.makePipe }.toStream),
      "explain" -> BSONBoolean(agg.command.explain),
      "allowDiskUse" -> BSONBoolean(agg.command.allowDiskUse),
      "cursor" -> agg.command.cursor.map(c => BSONInteger(c.batchSize)))
  }

  implicit object AggregationResultReader
      extends DealingWithGenericCommandErrorsReader[AggregationResult] {
    def readResult(doc: BSONDocument): AggregationResult =
      doc.getAs[List[BSONDocument]]("result").map(AggregationResult.apply).get

  }
}
