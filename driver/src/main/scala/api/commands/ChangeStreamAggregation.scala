package reactivemongo.api.commands

import reactivemongo.api.{ ChangeStreams, SerializationPack }

/**
 * [[https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#server-specification Change stream]]
 */
private[commands] trait ChangeStreamAggregation[P <: SerializationPack] {
  aggregation: AggregationFramework[P] =>

  /**
   * Low level pipeline operator which allows to open a tailable cursor
   * against subsequent [[https://docs.mongodb.com/manual/reference/change-events/ change events]] of a given collection.
   *
   * For common use-cases, you might prefer to use the `watch`
   * operator on a collection.
   *
   * '''Note:''' the target mongo instance MUST be a replica-set
   * (even in the case of a single node deployement).
   *
   * @since MongoDB 3.6
   */
  final class ChangeStream private[api] (
    offset: Option[ChangeStream.Offset],
    fullDocumentStrategy: Option[ChangeStreams.FullDocumentStrategy]) extends PipelineOperator {

    def makePipe: pack.Document = {
      val elms = Seq.newBuilder[pack.ElementProducer]

      offset.foreach {
        case startAfter: ChangeStream.StartAfter =>
          elms += builder.elementProducer("startAfter", startAfter.value)

        case startAt: ChangeStream.StartAt =>
          elms += builder.elementProducer(
            "startAtOperationTime", builder.timestamp(startAt.operationTime))

        case resumeAfter: ChangeStream.ResumeAfter =>
          elms += builder.elementProducer("resumeAfter", resumeAfter.value)

      }

      fullDocumentStrategy.foreach { s =>
        elms += builder.elementProducer("fullDocument", builder.string(s.name))
      }

      pipe(f"$$changeStream", builder.document(elms.result()))
    }

    private lazy val tupled = offset -> fullDocumentStrategy

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        other.tupled == this.tupled

      case _ => false
    }

    override def hashCode: Int = tupled.hashCode
  }

  object ChangeStream {
    sealed trait Offset

    /**
     * Indicates that the change stream must [[https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resumeafter resume after]] the given value.
     */
    final class ResumeAfter private[api] (val value: pack.Value) extends Offset {
      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      @inline override def hashCode: Int =
        if (value == null) -1 else value.hashCode

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          (this.value == null && other.value == null) || (
            this.value != null && this.value.equals(other.value))

        case _ =>
          false
      }

      override def toString = s"ResumeAfter($value)"
    }

    object ResumeAfter {
      /**
       * @param value Represents the ID of the last known change event, if any. The stream will resume just after that event.
       */
      def apply(value: pack.Value): ResumeAfter = new ResumeAfter(value)
    }

    /** Indicates that the change stream must start after the given value. */
    final class StartAfter private[api] (val value: pack.Value) extends Offset {
      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      @inline override def hashCode: Int =
        if (value == null) -1 else value.hashCode

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          (this.value == null && other.value == null) || (
            this.value != null && this.value.equals(other.value))

        case _ =>
          false
      }

      override def toString = s"StartAfter($value)"
    }

    object StartAfter {
      def apply(value: pack.Value): StartAfter = new StartAfter(value)
    }

    /**
     * Indicates that the change stream must [[https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#startatoperationtime start at]] the given operation time.
     *
     * @since MongoDB 4.0
     */
    final class StartAt private[api] (val operationTime: Long) extends Offset {
      @inline override def hashCode: Int = operationTime.toInt

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.operationTime == other.operationTime

        case _ =>
          false
      }

      override def toString = s"StartAt($operationTime)"
    }

    object StartAt {
      /*
     * @param operationTime The operation time before which all change events are known. Must be in the time range of the oplog.
       */
      def apply(operationTime: Long): StartAt = new StartAt(operationTime)
    }

    /**
     * @param offset the offset to manage the [[https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#resume-process resume process]]
     * @param fullDocumentStrategy If set to UpdateLookup, every update change event will be joined with the ''current'' version of the relevant document.
     */
    def apply(
      offset: Option[Offset] = None,
      fullDocumentStrategy: Option[ChangeStreams.FullDocumentStrategy] = None): ChangeStream = new ChangeStream(offset, fullDocumentStrategy)
  }
}
