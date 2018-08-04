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
  ResolvedCollectionCommand,
  WriteConcern
}
import reactivemongo.core.protocol.MongoWireVersion

private[collections] trait Aggregator[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] =>

  // TODO: comment, hint, collation, maxTimeMS

  // ---

  final class AggregatorContext[T](
    val firstOperator: PipelineOperator,
    val otherOperators: List[PipelineOperator],
    val explain: Boolean,
    val allowDiskUse: Boolean,
    val bypassDocumentValidation: Boolean,
    val readConcern: ReadConcern,
    val writeConcern: WriteConcern,
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
        bypassDocumentValidation, readConcern, writeConcern)

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
    val readConcern: ReadConcern,
    val writeConcern: WriteConcern) extends CollectionCommand
    with CommandWithPack[pack.type]
    with CommandWithResult[T]

  private type AggregateCmd[T] = ResolvedCollectionCommand[Aggregate[T]]

  private def commandWriter[T]: pack.Writer[AggregateCmd[T]] = {
    val builder = pack.newBuilder
    val session = collection.db.session.filter( // TODO: Remove
      _ => (version.compareTo(MongoWireVersion.V36) >= 0))

    val writeReadConcern = CommandCodecs.writeSessionReadConcern(
      builder, session)

    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)

    pack.writer[AggregateCmd[T]] { agg =>
      import builder.{ boolean, document, elementProducer => element }
      import agg.{ command => cmd }

      val pipeline = builder.array(
        cmd.operator.makePipe,
        cmd.pipeline.map(_.makePipe))

      lazy val isOut: Boolean = cmd.pipeline.lastOption.exists {
        case BatchCommands.AggregationFramework.Out(_) => true
        case _                                         => false
      }

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

      if (cmd.wireVersion >= MongoWireVersion.V36 && isOut) {
        elements += element("writeConcern", writeWriteConcern(cmd.writeConcern))
      }

      document(elements.result())
    }
  }
}
