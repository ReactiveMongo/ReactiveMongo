package reactivemongo.api.collections

import scala.language.higherKinds

import reactivemongo.api.{
  ChangeStreams,
  Cursor,
  CursorProducer,
  CursorOptions,
  ReadConcern,
  SerializationPack
}

trait ChangeStreamOps[P <: SerializationPack with Singleton] { collection: GenericCollection[P] =>

  import collection.BatchCommands.AggregationFramework.ChangeStream

  /**
   * Prepares a builder for watching the changeStream of this collection
   * [[https://docs.mongodb.com/manual/changeStreams]] (since MongoDB 3.6).
   *
   * Note: the target mongo instance MUST be a replica-set (even in the case of a single node deployement).
   *
   * @param resumeAfter          The id of the last known Change Event, if any. The stream will resume just after
   *                             that event.
   * @param startAtOperationTime The operation time before which all Change Events are known. Must be in the time range
   *                             of the oplog. (since MongoDB 4.0)
   * @param pipeline             A sequence of aggregation stages to apply on events in the stream (see MongoDB
   *                             documentation for a list of valid stages for a change stream).
   * @param maxAwaitTimeMS       The maximum amount of time in milliseconds the server waits for new data changes
   *                             before returning an empty batch. In practice, this parameter controls the duration
   *                             of the long-polling behavior of the cursor.
   * @param fullDocumentStrategy if set to UpdateLookup, every update change event will be joined with the *current*
   *                             version of the relevant document.
   * @param reader               a reader of the resulting Change Events
   * @tparam T the type into which Change Events are deserialized
   */
  final def watch[T](
    resumeAfter: Option[pack.Value] = None,
    startAtOperationTime: Option[pack.Value] = None,
    pipeline: List[PipelineOperator] = Nil,
    maxAwaitTimeMS: Option[Long] = None,
    fullDocumentStrategy: Option[ChangeStreams.FullDocumentStrategy] = None)(implicit reader: pack.Reader[T]): WatchBuilder[T] = {
    new WatchBuilder[T] {
      protected val context: AggregatorContext[T] = aggregatorContext[T](
        firstOperator = new ChangeStream(resumeAfter, startAtOperationTime, fullDocumentStrategy),
        otherOperators = pipeline,
        readConcern = Some(ReadConcern.Majority),
        cursorOptions = CursorOptions.empty.tailable,
        maxTimeMS = maxAwaitTimeMS)
    }
  }

  /**
   * A builder for the `watch` collection helper, which allows to consume the collection's ChangeStream.
   */
  sealed trait WatchBuilder[T] {

    protected val context: AggregatorContext[T]

    /**
     * Creates a cursor for the changeStream of this collection, as configured by the builder. The resulting cursor
     * implicitly has a `tailable` and `awaitData` behavior and requires some special handling:
     *
     * 1. The cursor will never be exhausted, unless the change stream is invalidated (see
     * [[https://docs.mongodb.com/manual/reference/change-events/#invalidate-event]]).
     * Therefore, you need to ensure that either:
     * - you consume a bounded number of events from the stream (such as when using `collect`, `foldWhile`, etc.)
     * - you close the cursor explicitly when you no longer need it (the cursor provider needs to support such a
     * functionality)
     * - you only start a finite number of unbounded cursors which follow the lifecycle of your whole application (for
     * example they will be shut down along with the driver when the app goes down).
     * In particular, using `fold` with the default unbounded `maxSize` will yield a Future which will never resolve.
     *
     * 2. The cursor may yield no results within any given finite time bounds, if there are no changes in the
     * underlying collection. Therefore, the Futures resulting from the cursor operations may never resolve.
     * Unless you are in a fully reactive scenario, you may want to add some timeout behavior to the resulting Future.
     * In that case, remember to explicitly close the cursor when the timeout triggers, so that you don't leak the
     * cursor (the cursor provider needs to support such a functionality).
     *
     * 3. New change streams return no data when the cursor is initially established (only subsequent GetMore commands
     * will actually return the subsequent events). Therefore, such a cursor `head` will always be empty. Only folding
     * the cursor (directly or through a higher-level cursor provider) will provide the next change event.
     *
     * 4. Resumed change streams (via id or operation time) will return the next event when the cursor is initially
     * established, if there is is some next event. Therefore, `head` is guaranteed to eventually return the next
     * change event beyond the resume point, when such an event appears.
     */
    def cursor[AC[_] <: Cursor.WithOps[_]](implicit cp: CursorProducer.Aux[T, AC]): AC[T] = {
      context.prepared[AC].cursor
    }

  }

}
