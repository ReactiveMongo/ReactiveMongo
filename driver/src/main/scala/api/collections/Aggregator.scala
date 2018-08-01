package reactivemongo.api.collections

import scala.language.higherKinds

import reactivemongo.api.{
  Cursor,
  CursorProducer,
  ReadConcern,
  ReadPreference,
  SerializationPack
}

import reactivemongo.api.commands.{
  CollectionCommand,
  CommandCodecs,
  CommandWithPack,
  CommandWithResult,
  ResolvedCollectionCommand
}
import reactivemongo.core.protocol.MongoWireVersion

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

    def prepared[AC[_] <: Cursor[_]](
      implicit
      cp: CursorProducer.Aux[T, AC]): Aggregator[T, AC] =
      new Aggregator[T, AC](this, cp)
  }

  final class Aggregator[T, AC[_] <: Cursor[_]](
    val context: AggregatorContext[T],
    val cp: CursorProducer.Aux[T, AC]) {

    import context.{
      allowDiskUse,
      batchSize,
      bypassDocumentValidation,
      explain,
      firstOperator,
      otherOperators,
      reader
    }

    @inline private def readPreference = context.readPreference

    private def ver = db.connectionState.metadata.maxWireVersion

    final def cursor: AC[T] = {
      def batchSz = batchSize.getOrElse(defaultCursorBatchSize)
      implicit def writer = commandWriter[T]
      implicit def aggReader: pack.Reader[T] = reader

      val cmd = new Aggregate[T](
        firstOperator, otherOperators, explain, allowDiskUse, batchSz, ver,
        bypassDocumentValidation, Some(readConcern))

      val cursor = runner.cursor[T, Aggregate[T]](
        collection, cmd, readPreference)

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
  private final class Aggregate[T](
    val operator: PipelineOperator,
    val pipeline: Seq[PipelineOperator],
    val explain: Boolean = false,
    val allowDiskUse: Boolean,
    val batchSize: Int,
    val wireVersion: MongoWireVersion,
    val bypassDocumentValidation: Boolean,
    val readConcern: Option[ReadConcern]) extends CollectionCommand
    with CommandWithPack[pack.type]
    with CommandWithResult[T]

  private type AggregateCmd[T] = ResolvedCollectionCommand[Aggregate[T]]

  private def commandWriter[T]: pack.Writer[AggregateCmd[T]] = {
    val builder = pack.newBuilder
    val writeReadConcern = CommandCodecs.writeReadConcern(builder)

    pack.writer[AggregateCmd[T]] { agg =>
      import builder.{ boolean, document, elementProducer => element }

      val pipeline = builder.array(
        agg.command.operator.makePipe,
        agg.command.pipeline.map(_.makePipe))

      val base = Seq(
        element("aggregate", builder.string(agg.collection)),
        element("pipeline", pipeline),
        element("explain", boolean(agg.command.explain)),
        element("allowDiskUse", boolean(agg.command.allowDiskUse)),
        element("cursor", document(Seq(
          element("batchSize", builder.int(agg.command.batchSize))))))

      val cmd = {
        if (agg.command.wireVersion < MongoWireVersion.V32) {
          base
        } else {
          val byp = element("bypassDocumentValidation", boolean(
            agg.command.bypassDocumentValidation))

          agg.command.readConcern match {
            case Some(concern) => base ++ Seq(
              byp,
              element("readConcern", writeReadConcern(concern)))

            case _ => base :+ byp
          }
        }
      }

      document(cmd)
    }
  }
}
