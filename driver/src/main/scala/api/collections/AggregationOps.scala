package reactivemongo.api.collections

import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{
  Collation,
  Cursor,
  CursorOptions,
  CursorProducer,
  PackSupport,
  ReadConcern,
  ReadPreference,
  SerializationPack,
  WriteConcern
}

import reactivemongo.api.commands.{
  AggregationFramework => AggFramework,
  CollectionCommand,
  CommandCodecs,
  CommandWithPack,
  CommandWithResult,
  ResolvedCollectionCommand
}
import reactivemongo.core.protocol.MongoWireVersion

private[collections] trait AggregationOps[P <: SerializationPack] {
  collection: GenericCollection[P] =>

  /** The [[https://docs.mongodb.com/manual/core/aggregation-pipeline/ aggregation framework]] for this collection */
  object AggregationFramework
    extends AggFramework[collection.pack.type]
    with PackSupport[collection.pack.type] {

    val pack: collection.pack.type = collection.pack
  }

  /** Aggregation framework */
  type AggregationFramework = AggregationFramework.type

  /**
   * Aggregation pipeline operator/stage
   */
  type PipelineOperator = AggregationFramework.PipelineOperator

  // ---

  final class AggregatorContext[T] private[reactivemongo] (
    val firstOperator: PipelineOperator,
    val otherOperators: List[PipelineOperator],
    val explain: Boolean,
    val allowDiskUse: Boolean,
    val bypassDocumentValidation: Boolean,
    val readConcern: ReadConcern,
    val writeConcern: WriteConcern,
    val readPreference: ReadPreference,
    val batchSize: Option[Int],
    val cursorOptions: CursorOptions,
    val maxTime: Option[FiniteDuration],
    val reader: pack.Reader[T],
    val hint: Option[Hint],
    val comment: Option[String],
    val collation: Option[Collation]) {

    def prepared[AC[_] <: Cursor.WithOps[_]](
      implicit
      cp: CursorProducer.Aux[T, AC]): Aggregator[T, AC] =
      new Aggregator[T, AC](this, cp)
  }

  private[api] final class Aggregator[T, AC[_] <: Cursor[_]] private[reactivemongo] (
    val context: AggregatorContext[T],
    val cp: CursorProducer.Aux[T, AC]) {

    private def ver = db.connectionState.metadata.maxWireVersion

    def cursor: AC[T] = {
      def batchSz = context.batchSize.getOrElse(defaultCursorBatchSize)
      implicit def writer = commandWriter[T]
      implicit def aggReader: pack.Reader[T] = context.reader

      val cmd = new Aggregate[T](
        context.firstOperator, context.otherOperators, context.explain,
        context.allowDiskUse, batchSz, ver,
        context.bypassDocumentValidation,
        context.readConcern, context.writeConcern,
        context.hint, context.comment, context.collation)

      val cursor = runner.cursor[T, Aggregate[T]](
        collection, cmd, context.cursorOptions,
        context.readPreference, context.maxTime.map(_.toMillis))

      cp.produce(cursor)
    }
  }

  /**
   * @param pipeline the sequence of MongoDB aggregation operations
   * @param explain specifies to return the information on the processing of the pipeline
   * @param allowDiskUse enables writing to temporary files
   * @param batchSize the batch size
   * @param bypassDocumentValidation available only if you specify the \$out aggregation operator
   * @param readConcern the read concern (since MongoDB 3.2)
   */
  private[api] final class Aggregate[T](
    val operator: PipelineOperator,
    val pipeline: Seq[PipelineOperator],
    val explain: Boolean = false,
    val allowDiskUse: Boolean,
    val batchSize: Int,
    val wireVersion: MongoWireVersion,
    val bypassDocumentValidation: Boolean,
    val readConcern: ReadConcern,
    val writeConcern: WriteConcern,
    val hint: Option[Hint],
    val comment: Option[String],
    val collation: Option[Collation]) extends CollectionCommand
    with CommandWithPack[pack.type]
    with CommandWithResult[T]

  private type AggregateCmd[T] = ResolvedCollectionCommand[Aggregate[T]]

  private def commandWriter[T]: pack.Writer[AggregateCmd[T]] = {
    val builder = pack.newBuilder
    val session = collection.db.session.filter(
      _ => (version.compareTo(MongoWireVersion.V36) >= 0))

    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)
    val writeCollation = Collation.serializeWith(pack, _: Collation)(builder)

    pack.writer[AggregateCmd[T]] { agg =>
      import builder.{ boolean, document, elementProducer => element }
      import agg.{ command => cmd }

      val pipeline = builder.array(
        cmd.operator.makePipe,
        cmd.pipeline.map(_.makePipe))

      lazy val isOut: Boolean = cmd.pipeline.lastOption.exists {
        case _: AggregationFramework.Out => true
        case _                           => false
      }

      val writeReadConcern =
        CommandCodecs.writeSessionReadConcern(builder)(session)

      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("aggregate", builder.string(agg.collection)),
        element("pipeline", pipeline),
        element("explain", boolean(cmd.explain)),
        element("allowDiskUse", boolean(cmd.allowDiskUse)),
        element("cursor", document(Seq(
          element("batchSize", builder.int(cmd.batchSize))))))

      if (cmd.wireVersion >= MongoWireVersion.V32) {
        elements += element("bypassDocumentValidation", boolean(
          cmd.bypassDocumentValidation))

        elements ++= writeReadConcern(cmd.readConcern)
      }

      cmd.comment.foreach { comment =>
        elements += element("comment", builder.string(comment))
      }

      cmd.hint.foreach {
        case HintString(str) =>
          elements += element("hint", builder.string(str))

        case HintDocument(doc) =>
          elements += element("hint", doc)

        case _ =>
      }

      cmd.collation.foreach { collation =>
        elements += element("collation", writeCollation(collation))
      }

      if (cmd.wireVersion >= MongoWireVersion.V36 && isOut) {
        elements += element("writeConcern", writeWriteConcern(cmd.writeConcern))
      }

      document(elements.result())
    }
  }
}
