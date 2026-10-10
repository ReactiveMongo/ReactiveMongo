package reactivemongo.api.commands

import reactivemongo.api.{ PackSupport, SerializationPack }

/**
 * Windowed aggregation helpers and stages.
 *
 * Shared types such as [[TimeUnit]], [[WindowBoundary]] and [[FillOutput]]
 * are declared independently of each stage (same idea as [[SortOrder]]).
 *
 * @define densifyDescription [[https://docs.mongodb.com/manual/reference/operator/aggregation/densify/ \$densify]] aggregation stage
 * @define fillDescription [[https://docs.mongodb.com/manual/reference/operator/aggregation/fill/ \$fill]] aggregation stage
 * @define setWindowFieldsDescription [[https://docs.mongodb.com/manual/reference/operator/aggregation/setWindowFields/ \$setWindowFields]] aggregation stage
 */
private[commands] trait WindowAggregation[P <: SerializationPack] {
  aggregation: PackSupport[P] with AggregationFramework[P] =>

  /**
   * Time unit used by [[Densify]] and [[SetWindowFields]].
   *
   * @param name the MongoDB unit name (e.g. `hour`, `day`)
   */
  final class TimeUnit private[api] (val name: String) {

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.name == null && other.name == null) || (this.name != null && this.name == other.name)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (name == null) -1 else name.hashCode

    override def toString: String = s"TimeUnit($name)"
  }

  /** Factory for [[TimeUnit]]. */
  object TimeUnit {
    val Millisecond: TimeUnit = new TimeUnit("millisecond")
    val Second: TimeUnit = new TimeUnit("second")
    val Minute: TimeUnit = new TimeUnit("minute")
    val Hour: TimeUnit = new TimeUnit("hour")
    val Day: TimeUnit = new TimeUnit("day")
    val Week: TimeUnit = new TimeUnit("week")
    val Month: TimeUnit = new TimeUnit("month")
    val Quarter: TimeUnit = new TimeUnit("quarter")
    val Year: TimeUnit = new TimeUnit("year")

    def apply(name: String): TimeUnit = new TimeUnit(name)
  }

  /**
   * Boundary value for a documents/range window
   * (shared by [[SetWindowFields]]).
   */
  sealed trait WindowBoundary {

    /** BSON value encoded in the window specification. */
    def value: pack.Value
  }

  /** Factory for [[WindowBoundary]]. */
  object WindowBoundary {

    /** Current document boundary (`"current"`). */
    case object Current extends WindowBoundary {
      val value: pack.Value = builder.string("current")
    }

    /** Unbounded boundary (`"unbounded"`). */
    case object Unbounded extends WindowBoundary {
      val value: pack.Value = builder.string("unbounded")
    }

    /**
     * Numeric offset relative to the current document
     * (documents window) or numeric range offset.
     */
    final class Offset private[api] (val amount: Int) extends WindowBoundary {
      val value: pack.Value = builder.int(amount)

      override def equals(that: Any): Boolean = that match {
        case other: this.type => this.amount == other.amount
        case _                => false
      }

      override def hashCode: Int = amount

      override def toString: String = s"Offset($amount)"
    }

    object Offset {
      def apply(amount: Int): Offset = new Offset(amount)
    }

    /** Arbitrary boundary value (e.g. range numeric/double). */
    final class Absolute private[api] (val raw: pack.Value)
        extends WindowBoundary {
      def value: pack.Value = raw

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          (this.raw == null && other.raw == null) || (this.raw != null && this.raw == other.raw)

        case _ =>
          false
      }

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def hashCode: Int =
        if (raw == null) -1 else raw.hashCode

      override def toString: String = s"Absolute($raw)"
    }

    object Absolute {
      def apply(raw: pack.Value): Absolute = new Absolute(raw)
    }
  }

  /**
   * Bounds specification for [[Densify]].
   *
   * Either a keyword (`full` / `partition`) or an inclusive
   * `[lower, upper]` range.
   */
  sealed trait DensifyBounds {

    /** Encoded densify `bounds` value. */
    def value: pack.Value
  }

  /** Factory for [[DensifyBounds]]. */
  object DensifyBounds {

    /** Densify over the full range of values across all documents. */
    case object Full extends DensifyBounds {
      val value: pack.Value = builder.string("full")
    }

    /** Densify independently within each partition. */
    case object Partition extends DensifyBounds {
      val value: pack.Value = builder.string("partition")
    }

    /**
     * Explicit inclusive densify range.
     *
     * @param lower the lower bound
     * @param upper the upper bound
     */
    final class Range private[api] (
        val lower: pack.Value,
        val upper: pack.Value)
        extends DensifyBounds {
      val value: pack.Value = builder.array(Seq(lower, upper))

      private lazy val tupled = lower -> upper

      override def equals(that: Any): Boolean = that match {
        case other: this.type => this.tupled == other.tupled
        case _                => false
      }

      override def hashCode: Int = tupled.hashCode

      override def toString: String = s"Range($lower, $upper)"
    }

    object Range {

      def apply(lower: pack.Value, upper: pack.Value): Range =
        new Range(lower, upper)
    }
  }

  /**
   * Output specification for [[Fill]].
   *
   * Either a constant/expression `value`, or a fill `method`
   * (`locf` / `linear`).
   */
  sealed trait FillOutput {

    /** Encoded field output document. */
    def document: pack.Document
  }

  /** Factory for [[FillOutput]]. */
  object FillOutput {
    import builder.{ elementProducer => element }

    /**
     * Fill missing values with a constant or expression.
     *
     * @param expression the value expression
     */
    final class Value private[api] (val expression: pack.Value)
        extends FillOutput {

      val document: pack.Document =
        builder.document(Seq(element("value", expression)))

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          (this.expression == null && other.expression == null) || (this.expression != null && this.expression == other.expression)

        case _ =>
          false
      }

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def hashCode: Int =
        if (expression == null) -1 else expression.hashCode

      override def toString: String = s"Value($expression)"
    }

    object Value {
      def apply(expression: pack.Value): Value = new Value(expression)
    }

    /**
     * Fill missing values using a method.
     *
     * @param name `locf` or `linear`
     */
    final class Method private[api] (val name: String) extends FillOutput {

      val document: pack.Document =
        builder.document(Seq(element("method", builder.string(name))))

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          (this.name == null && other.name == null) || (this.name != null && this.name == other.name)

        case _ =>
          false
      }

      @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
      override def hashCode: Int =
        if (name == null) -1 else name.hashCode

      override def toString: String = s"Method($name)"
    }

    object Method {
      val Locf: Method = new Method("locf")
      val Linear: Method = new Method("linear")

      def apply(name: String): Method = new Method(name)
    }
  }

  /**
   * One output field produced by [[SetWindowFields]].
   *
   * @param field the output field name
   * @param operator the window operator name (e.g. `\$sum`)
   * @param expression the window operator expression/parameters
   * @param window optional window bounds
   */
  final class WindowOutput private[api] (
      val field: String,
      val operator: String,
      val expression: pack.Value,
      val window: Option[WindowOutput.Window]) {

    private lazy val tupled = Tuple4(field, operator, expression, window)

    override def equals(that: Any): Boolean = that match {
      case other: this.type => this.tupled == other.tupled
      case _                => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"WindowOutput$tupled"
  }

  object WindowOutput {
    import builder.{ elementProducer => element }

    /**
     * Window bounds for a [[WindowOutput]].
     *
     * Specify either `documents` or `range` (optionally with `unit`).
     */
    final class Window private[api] (
        val documents: Option[(WindowBoundary, WindowBoundary)],
        val range: Option[(WindowBoundary, WindowBoundary)],
        val unit: Option[TimeUnit]) {

      private[api] def document: pack.Document = {
        val elms = Seq.newBuilder[pack.ElementProducer]

        documents.foreach {
          case (lower, upper) =>
            elms += element(
              "documents",
              builder.array(Seq(lower.value, upper.value))
            )
        }

        range.foreach {
          case (lower, upper) =>
            elms += element(
              "range",
              builder.array(Seq(lower.value, upper.value))
            )
        }

        unit.foreach { u => elms += element("unit", builder.string(u.name)) }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple3(documents, range, unit)

      override def equals(that: Any): Boolean = that match {
        case other: this.type => this.tupled == other.tupled
        case _                => false
      }

      override def hashCode: Int = tupled.hashCode

      override def toString: String = s"Window$tupled"
    }

    object Window {

      def documents(
          lower: WindowBoundary,
          upper: WindowBoundary
        ): Window =
        new Window(Some(lower -> upper), None, None)

      def range(
          lower: WindowBoundary,
          upper: WindowBoundary,
          unit: Option[TimeUnit] = None
        ): Window =
        new Window(None, Some(lower -> upper), unit)
    }

    def apply(
        field: String,
        operator: String,
        expression: pack.Value,
        window: Option[Window] = None
      ): WindowOutput =
      new WindowOutput(field, operator, expression, window)
  }

  // --- stages ---

  /** $densifyDescription. */
  final class Densify private[api] (
      val field: String,
      val range: Densify.Range,
      val partitionByFields: Seq[String])
      extends PipelineOperator {
    import builder.{ elementProducer => element, string }

    protected[reactivemongo] val makePipe: pack.Document = {
      val elms = Seq.newBuilder[pack.ElementProducer] += element(
        "field",
        string(field)
      )

      if (partitionByFields.nonEmpty) {
        elms += element(
          "partitionByFields",
          builder.array(partitionByFields.map(string))
        )
      }

      elms += element("range", range.document)

      pipe(f"$$densify", builder.document(elms.result()))
    }

    private lazy val tupled = Tuple3(field, range, partitionByFields)

    override def equals(that: Any): Boolean = that match {
      case other: this.type => this.tupled == other.tupled
      case _                => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"Densify$tupled"
  }

  /**
   * $densifyDescription.
   *
   * @since MongoDB 5.1
   */
  object Densify {
    import builder.{ elementProducer => element }

    /**
     * Densify range specification.
     *
     * @param step the increment between generated values
     * @param bounds the densify bounds
     * @param unit optional time unit when densifying dates
     */
    final class Range private[api] (
        val step: pack.Value,
        val bounds: DensifyBounds,
        val unit: Option[TimeUnit]) {

      private[api] def document: pack.Document = {
        val elms = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          element("step", step),
          element("bounds", bounds.value)
        )

        unit.foreach { u => elms += element("unit", builder.string(u.name)) }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple3(step, bounds, unit)

      override def equals(that: Any): Boolean = that match {
        case other: this.type => this.tupled == other.tupled
        case _                => false
      }

      override def hashCode: Int = tupled.hashCode

      override def toString: String = s"Range$tupled"
    }

    object Range {

      def apply(
          step: pack.Value,
          bounds: DensifyBounds,
          unit: Option[TimeUnit] = None
        ): Range = new Range(step, bounds, unit)
    }

    /**
     * @param field the field to densify
     * @param range the densify range
     * @param partitionByFields optional fields used to partition densify
     */
    def apply(
        field: String,
        range: Range,
        partitionByFields: Seq[String] = Seq.empty
      ): Densify = new Densify(field, range, partitionByFields)
  }

  /** $fillDescription. */
  final class Fill private[api] (
      val output: Seq[(String, FillOutput)],
      val sortBy: Seq[SortOrder],
      val partitionBy: Option[pack.Value],
      val partitionByFields: Seq[String])
      extends PipelineOperator {
    import builder.{ elementProducer => element }

    protected[reactivemongo] val makePipe: pack.Document = {
      val elms = Seq.newBuilder[pack.ElementProducer]

      partitionBy.foreach { p => elms += element("partitionBy", p) }

      if (partitionByFields.nonEmpty) {
        elms += element(
          "partitionByFields",
          builder.array(partitionByFields.map(f => builder.string(f"$$${f}")))
        )
      }

      if (sortBy.nonEmpty) {
        elms += element("sortBy", sortSpecification(sortBy))
      }

      elms += element(
        "output",
        builder.document(output.map {
          case (name, out) => element(name, out.document)
        })
      )

      pipe(f"$$fill", builder.document(elms.result()))
    }

    private lazy val tupled =
      Tuple4(output, sortBy, partitionBy, partitionByFields)

    override def equals(that: Any): Boolean = that match {
      case other: this.type => this.tupled == other.tupled
      case _                => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"Fill$tupled"
  }

  /**
   * $fillDescription.
   *
   * @since MongoDB 5.3
   */
  object Fill {

    /**
     * @param output the fields to fill and how to fill them
     * @param sortBy optional sort specification required by some methods
     * @param partitionBy optional partition expression
     * @param partitionByFields optional partition field names
     */
    def apply(
        output: Seq[(String, FillOutput)],
        sortBy: Seq[SortOrder] = Seq.empty,
        partitionBy: Option[pack.Value] = None,
        partitionByFields: Seq[String] = Seq.empty
      ): Fill = new Fill(output, sortBy, partitionBy, partitionByFields)
  }

  /** $setWindowFieldsDescription. */
  final class SetWindowFields private[api] (
      val output: Seq[WindowOutput],
      val partitionBy: Option[pack.Value],
      val sortBy: Seq[SortOrder])
      extends PipelineOperator {
    import builder.{ elementProducer => element }

    protected[reactivemongo] val makePipe: pack.Document = {
      val elms = Seq.newBuilder[pack.ElementProducer]

      partitionBy.foreach { p => elms += element("partitionBy", p) }

      if (sortBy.nonEmpty) {
        elms += element("sortBy", sortSpecification(sortBy))
      }

      elms += element(
        "output",
        builder.document(output.map { out =>
          val fields = Seq.newBuilder[pack.ElementProducer] += element(
            out.operator,
            out.expression
          )

          out.window.foreach { w => fields += element("window", w.document) }

          element(out.field, builder.document(fields.result()))
        })
      )

      pipe(f"$$setWindowFields", builder.document(elms.result()))
    }

    private lazy val tupled = Tuple3(output, partitionBy, sortBy)

    override def equals(that: Any): Boolean = that match {
      case other: this.type => this.tupled == other.tupled
      case _                => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"SetWindowFields$tupled"
  }

  /**
   * $setWindowFieldsDescription.
   *
   * @since MongoDB 5.0
   */
  object SetWindowFields {

    /**
     * @param output the window outputs to append
     * @param partitionBy optional partition expression
     * @param sortBy optional sort specification
     */
    def apply(
        output: Seq[WindowOutput],
        partitionBy: Option[pack.Value] = None,
        sortBy: Seq[SortOrder] = Seq.empty
      ): SetWindowFields =
      new SetWindowFields(output, partitionBy, sortBy)
  }

  /** Encodes a sort specification document from [[SortOrder]] values. */
  private[api] def sortSpecification(
      fields: Seq[SortOrder]
    ): pack.Document = {
    import builder.{ elementProducer => element }

    builder.document(fields.collect {
      case Ascending(field) =>
        element(field, builder.int(1))

      case Descending(field) =>
        element(field, builder.int(-1))

      case MetadataSort(field, keyword) => {
        val meta = builder.document(
          Seq(element(f"$$meta", builder.string(keyword.name)))
        )

        element(field, meta)
      }
    })
  }
}
