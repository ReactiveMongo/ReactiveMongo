package reactivemongo.api.collections

import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{
  Collation,
  CursorOptions,
  ReadConcern,
  ReadPreference,
  SerializationPack,
  WriteConcern
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define bypassDocumentValidationParam the flag to bypass document validation during the operation
 * @define readConcernParam the read concern
 * @define pipelineParam the [[https://docs.mongodb.com/manual/reference/operator/aggregation/ aggregation]] pipeline
 * @define explainParam if true indicates to return the information on the processing
 * @define allowDiskUseParam if true enables writing to temporary files
 * @define resultTParam The type of the result elements. An implicit `Reader[T]` typeclass for handling it has to be in the scope.
 * @define readerParam the result reader
 * @define cursorProducerParam the cursor producer
 * @define aggBatchSizeParam the batch size (for the aggregation cursor; if `None` use the default one)
 * @define collationParam the collation
 * @define hintParam the index to use (either the index name or the index document)
 */
private[collections] trait GenericCollectionWithAggregatorContext[
    P <: SerializationPack] {
  self: GenericCollection[P] with AggregationOps[P] with HintFactory[P] =>

  /**
   * [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregates]] the matching documents.
   *
   * {{{
   * import scala.concurrent.Future
   * import scala.concurrent.ExecutionContext.Implicits.global
   *
   * import reactivemongo.api.Cursor
   * import reactivemongo.api.bson._
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def populatedStates(cities: BSONCollection): Future[List[BSONDocument]] = {
   *   import cities.AggregationFramework
   *   import AggregationFramework.{ Group, Match, SumField }
   *
   *   cities.aggregatorContext[BSONDocument](
   *     List(Group(BSONString(f"$$state"))(
   *       "totalPop" -> SumField("population")),
   *         Match(BSONDocument("totalPop" ->
   *           BSONDocument(f"$$gte" -> 10000000L))))
   *   ).prepared.cursor.collect[List](
   *     maxDocs = 3,
   *     err = Cursor.FailOnError[List[BSONDocument]]()
   *   )
   * }
   * }}}
   *
   * @tparam T $resultTParam
   *
   * @param pipeline $pipelineParam
   * @param cursor aggregation cursor option (optional)
   * @param explain $explainParam of the pipeline (default: `false`)
   * @param allowDiskUse $allowDiskUseParam (default: `false`)
   * @param bypassDocumentValidation $bypassDocumentValidationParam (default: `false`)
   * @param readConcern $readConcernParam
   * @param readPreference $readPrefParam
   * @param writeConcern $writeConcernParam
   * @param batchSize $aggBatchSizeParam
   * @param cursorOptions the options for the result cursor
   * @param maxAwaitTime In practice, this parameter controls the duration of the long-polling behavior of the cursor.
   * @param hint $hintParam
   * @param comment the [[https://docs.mongodb.com/manual/reference/method/cursor.comment/#cursor.comment comment]] to annotation the aggregation command
   * @param collation $collationParam
   * @param reader $readerParam
   * @param cp $cursorProducerParam
   */
  @SuppressWarnings(Array("MaxParameters"))
  def aggregatorContext[T](
      pipeline: List[PipelineOperator] = List.empty,
      explain: Boolean = false,
      allowDiskUse: Boolean = false,
      bypassDocumentValidation: Boolean = false,
      readConcern: ReadConcern = this.readConcern,
      readPreference: ReadPreference = this.readPreference,
      writeConcern: WriteConcern = this.writeConcern,
      batchSize: Option[Int] = None,
      cursorOptions: CursorOptions = CursorOptions.empty,
      @deprecatedName('maxTime) maxAwaitTime: Option[FiniteDuration] = None,
      hint: Option[Hint] = None,
      comment: Option[String] = None,
      collation: Option[Collation] = None
    )(implicit
      reader: pack.Reader[T]
    ): AggregatorContext[T] = {
    new AggregatorContext[T](
      pipeline,
      explain,
      allowDiskUse,
      bypassDocumentValidation,
      readConcern,
      writeConcern,
      readPreference,
      batchSize,
      cursorOptions,
      maxAwaitTime,
      maxTime = None,
      reader,
      hint,
      comment,
      collation
    )
  }
}
