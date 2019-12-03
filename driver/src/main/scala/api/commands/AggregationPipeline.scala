package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

private[commands] trait AggregationPipeline[P <: SerializationPack] {
  val pack: P

  /**
   * One of MongoDBs pipeline operators for aggregation.
   * Sealed as these are defined in the MongoDB specifications,
   * and clients should not have custom operators.
   */
  trait PipelineOperator {
    def makePipe: pack.Value
  }

  /**
   * Only for advanced user: Factory for stage not already provided in the API.
   *
   * For example for `{ \$sample: { size: 2 } }`
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.{ BSONDocument, BSONInteger, BSONString }
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def foo(coll: BSONCollection)(implicit ec: ExecutionContext) =
   *   coll.aggregateWith[BSONDocument]() { agg =>
   *     import agg.PipelineOperator
   *
   *     val stage = PipelineOperator(BSONDocument(
   *       f"$$sample" -> BSONDocument("size" -> 2)))
   *
   *     stage -> List.empty
   *   }
   * }}}
   */
  object PipelineOperator {
    def apply(pipe: => pack.Value): PipelineOperator = new PipelineOperator {
      val makePipe = pipe
    }
  }

  /**
   * Aggregation pipeline (with at least one stage operator)
   */
  type Pipeline = (PipelineOperator, List[PipelineOperator])
}
