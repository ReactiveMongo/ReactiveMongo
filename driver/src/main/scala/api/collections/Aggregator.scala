package reactivemongo.api.collections

import scala.language.higherKinds

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{
  Cursor,
  CursorFlattener,
  CursorProducer,
  DefaultCursor,
  ReadConcern,
  ReadPreference,
  SerializationPack
}

import reactivemongo.api.commands.ResponseResult
import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.core.errors.GenericDriverException

private[collections] trait Aggregator[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] =>

  // ---

  final class AggregatorContext[T](
    val firstOperator: PipelineOperator,
    val otherOperators: List[PipelineOperator],
    val explain: Boolean,
    val allowDiskUse: Boolean,
    val bypassDocumentValidation: Boolean,
    val readConcern: Option[ReadConcern],
    val readPreference: ReadPreference,
    val batchSize: Option[Int],
    val reader: pack.Reader[T]) {
    def prepared[AC[_] <: Cursor[_]](implicit cp: CursorProducer.Aux[T, AC]): Aggregator[T, AC] = new Aggregator[T, AC](this, cp)
  }

  final class Aggregator[T, AC[_] <: Cursor[_]](
    val context: AggregatorContext[T],
    val cp: CursorProducer.Aux[T, AC]) {
    import BatchCommands.AggregationFramework.{ Aggregate, AggregationResult }
    import BatchCommands.{ AggregateWriter, AggregateReader }

    import reactivemongo.core.netty.ChannelBufferWritableBuffer
    import reactivemongo.bson.buffer.WritableBuffer
    import reactivemongo.core.protocol.{ Reply, Response }

    import context._

    @inline private def readPreference = context.readPreference
    implicit private def aggReader: pack.Reader[T] = reader

    private def ver = db.connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    /**
     * @param cf the cursor flattener (by default use the builtin one)
     */
    final def cursor(implicit ec: ExecutionContext, cf: CursorFlattener[AC]): AC[T] = {
      def aggCursor: Future[cp.ProducedCursor] = runWithResponse(
        Aggregate(
          firstOperator :: otherOperators,
          explain, allowDiskUse, Some(BatchCommands.AggregationFramework.Cursor(
            batchSize.getOrElse(defaultCursorBatchSize))), ver, bypassDocumentValidation, readConcern), readPreference).flatMap[cp.ProducedCursor] {
          case ResponseResult(response, numToReturn,
            AggregationResult(firstBatch, Some(resultCursor))) => Future {

            def docs = new ChannelBufferWritableBuffer().
              writeBytes(firstBatch.foldLeft[WritableBuffer](
                new ChannelBufferWritableBuffer())(pack.writeToBuffer).toReadableBuffer).buffer

            def resp = Response(
              response.header,
              Reply(0, resultCursor.cursorId, 0, firstBatch.size),
              docs, response.info)

            cp.produce(DefaultCursor.getMore[P, T](pack, resp,
              resultCursor, numToReturn, readPreference, db.
              connection, failoverStrategy, false))
          }

          case ResponseResult(response, _, _) =>
            Future.failed[cp.ProducedCursor](
              GenericDriverException(s"missing cursor: $response"))
        }

      cf.flatten(aggCursor)
    }
  }
}
