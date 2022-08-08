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
    protected[reactivemongo] def makePipe: pack.Document
  }

  /**
   * Only for advanced user: Factory for stage not already provided in the API.
   *
   * For example for `{ \$sample: { size: 2 } }`
   *
   * {{{
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def foo(coll: BSONCollection) =
   *   coll.aggregateWith[BSONDocument]() { agg =>
   *     import agg.PipelineOperator
   *
   *     List(PipelineOperator(BSONDocument(
   *       f"$$sample" -> BSONDocument("size" -> 2))))
   *   }
   * }}}
   */
  object PipelineOperator {

    def apply(pipe: => pack.Document): PipelineOperator = new PipelineOperator {
      val makePipe = pipe
    }

    implicit def writer[Op <: PipelineOperator]: pack.Writer[Op] =
      pack.writer[Op](_.makePipe)
  }

  /**
   * Aggregation pipeline (with at least one stage operator)
   */
  type Pipeline = List[PipelineOperator]
}
