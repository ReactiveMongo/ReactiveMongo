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
   * PipelineOperator(BSONDocument("\$sample" -> BSONDocument("size" -> 2)))
   * }}}
   */
  object PipelineOperator {
    def apply(pipe: => pack.Value): PipelineOperator = new PipelineOperator {
      val makePipe = pipe
    }
  }
}
