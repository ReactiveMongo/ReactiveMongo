package reactivemongo.api.commands.bson

import reactivemongo.bson.{
  BSONDocument,
  BSONElement,
  BSONNumberLike,
  BSONValue,
  Producer
}

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.api.{ BSONSerializationPack, ReadConcern }
import reactivemongo.api.commands.{ AggregationFramework, ResultCursor }
import reactivemongo.api.commands.bson.CommonImplicits.ReadConcernWriter

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
  import BSONAggregationFramework.{ Aggregate, AggregationResult, Cursor }
  import reactivemongo.bson.{
    BSONArray,
    BSONBoolean,
    BSONDocumentReader,
    BSONDocumentWriter,
    BSONInteger,
    BSONString
  }

  implicit object CursorWriter extends BSONDocumentWriter[Cursor] {
    def write(cursor: Cursor) = BSONDocument("batchSize" -> cursor.batchSize)
  }

  implicit object AggregateWriter
      extends BSONDocumentWriter[ResolvedCollectionCommand[Aggregate]] {
    def write(agg: ResolvedCollectionCommand[Aggregate]) = {
      val cmd = BSONDocument(
        "aggregate" -> BSONString(agg.collection),
        "pipeline" -> BSONArray(
          { for (pipe <- agg.command.pipeline) yield pipe.makePipe }.toStream),
        "explain" -> BSONBoolean(agg.command.explain),
        "allowDiskUse" -> BSONBoolean(agg.command.allowDiskUse),
        "cursor" -> agg.command.cursor.map(CursorWriter.write(_)))

      if (agg.command.wireVersion < MongoWireVersion.V32) cmd else {
        cmd ++ ("bypassDocumentValidation" -> BSONBoolean(
          agg.command.bypassDocumentValidation),
          "readConcern" -> agg.command.readConcern)
      }
    }
  }

  implicit object AggregationResultReader
      extends DealingWithGenericCommandErrorsReader[AggregationResult] {
    def readResult(doc: BSONDocument): AggregationResult =
      if (doc contains "stages") AggregationResult(List(doc), None) else {
        doc.getAs[List[BSONDocument]]("result") match {
          case Some(docs) => AggregationResult(docs, None)
          case _ => (for {
            cursor <- doc.getAsTry[BSONDocument]("cursor")
            id <- cursor.getAsTry[BSONNumberLike]("id").map(_.toLong)
            ns <- cursor.getAsTry[String]("ns")
            firstBatch <- cursor.getAsTry[List[BSONDocument]]("firstBatch")
          } yield AggregationResult(firstBatch, Some(ResultCursor(id, ns)))).get
        }
      }
  }
}
