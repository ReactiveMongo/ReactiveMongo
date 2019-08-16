package reactivemongo.api.commands.bson

import reactivemongo.bson.{ BSONDocument, BSONDocumentReader, BSONNumberLike }

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.{ AggregationFramework, ResultCursor }
import reactivemongo.api.commands.bson.CommonImplicits.ReadConcernWriter

@deprecated("Internal: will be made private", "0.16.0")
object BSONAggregationFramework
  extends AggregationFramework[BSONSerializationPack.type] {

  val pack: BSONSerializationPack.type = BSONSerializationPack
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONAggregationImplicits {
  import reactivemongo.api.commands.ResolvedCollectionCommand
  import BSONAggregationFramework.{ Aggregate, AggregationResult, Cursor }
  import reactivemongo.bson.{
    BSONBoolean,
    BSONDocumentWriter,
    BSONString
  }

  @deprecated("", "0.12.7")
  implicit object CursorWriter extends BSONDocumentWriter[Cursor] {
    def write(cursor: Cursor) = BSONDocument("batchSize" -> cursor.batchSize)
  }

  @deprecated("", "0.12.7")
  implicit object AggregateWriter
    extends BSONDocumentWriter[ResolvedCollectionCommand[Aggregate]] {
    def write(agg: ResolvedCollectionCommand[Aggregate]) = {
      val cmd = BSONDocument(
        "aggregate" -> BSONString(agg.collection),
        "pipeline" -> agg.command.pipeline.map(_.makePipe),
        "explain" -> BSONBoolean(agg.command.explain),
        "allowDiskUse" -> BSONBoolean(agg.command.allowDiskUse),
        "cursor" -> agg.command.cursor.map(CursorWriter.write(_)))

      println(s"pipeline = ${agg.command.pipeline.map(_.makePipe.asInstanceOf[BSONDocument]).map(BSONDocument.pretty)}")

      if (agg.command.wireVersion < MongoWireVersion.V32) cmd else {
        cmd ++ (
          "bypassDocumentValidation" -> BSONBoolean(
            agg.command.bypassDocumentValidation),
            "readConcern" -> agg.command.readConcern)
      }
    }
  }

  implicit object AggregationResultReader
    extends DealingWithGenericCommandErrorsReader[AggregationResult] {
    def readResult(doc: BSONDocument): AggregationResult = {
      if (doc contains "stages") {
        AggregationResult(List(doc), None)
      } else {
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
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONAggregationResultImplicits {
  import BSONAggregationFramework.{ IndexStatsResult, IndexStatAccesses }

  implicit object BSONIndexStatAccessesReader
    extends BSONDocumentReader[IndexStatAccesses] {

    def read(doc: BSONDocument): IndexStatAccesses = (for {
      ops <- doc.getAsTry[BSONNumberLike]("ops").map(_.toLong)
      since <- doc.getAsTry[BSONNumberLike]("since").map(_.toLong)
    } yield IndexStatAccesses(ops, since)).get
  }

  implicit object BSONIndexStatsReader
    extends BSONDocumentReader[IndexStatsResult] {

    def read(doc: BSONDocument): IndexStatsResult = (for {
      name <- doc.getAsTry[String]("name")
      key <- doc.getAsTry[BSONDocument]("key")
      host <- doc.getAsTry[String]("host")
      acc <- doc.getAsTry[IndexStatAccesses]("accesses")
    } yield IndexStatsResult(name, key, host, acc)).get
  }
}
