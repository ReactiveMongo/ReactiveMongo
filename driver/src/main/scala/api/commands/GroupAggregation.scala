package reactivemongo.api.commands

import reactivemongo.api.{ PackSupport, SerializationPack }

/**
 * @define fieldParam the name of field to group
 */
private[commands] trait GroupAggregation[P <: SerializationPack] {
  aggregation: PackSupport[P] with AggregationFramework[P] =>

  /**
   * Represents one of the group/accumulator operators,
   * for the `\$group` aggregation. Operation.
   * @see https://docs.mongodb.com/manual/reference/operator/aggregation/group/#accumulator-operator
   */
  sealed trait GroupFunction {
    protected[reactivemongo] def makeFunction: pack.Value
  }

  /** Factory to declare custom call to a group function. */
  object GroupFunction {
    /**
     * Creates a call to specified group function with given argument.
     *
     * @param name The name of the group function (e.g. `\$sum`)
     * @param arg The group function argument
     * @return A group function call defined as `{ name: arg }`
     */
    def apply(name: String, arg: pack.Value): GroupFunction =
      new GroupFunction {
        protected[reactivemongo] val makeFunction = pipe(name, arg)
      }
  }

  // ---

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#grp._S_avg \$avg]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class AvgField private (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$avg", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field == other.field)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"AvgField(${field})"
  }

  object AvgField {
    def apply(field: String): AvgField = new AvgField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#grp._S_avg \$avg]] group accumulator.
   */
  final class Avg private (val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$avg", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Avg(${expression})"
  }

  object Avg {
    def apply(expression: pack.Value): Avg = new Avg(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class FirstField private (val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$first", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field == other.field)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"FirstField(${field})"
  }

  object FirstField {
    def apply(field: String): FirstField = new FirstField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   */
  final class First private (val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$first", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"First(${expression})"
  }

  object First {
    def apply(expression: pack.Value): First = new First(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class LastField private (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$last", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field == other.field)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"LastField(${field})"
  }

  object LastField {
    def apply(field: String): LastField = new LastField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   */
  final class Last private (val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$last", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Last(${expression})"
  }

  object Last {
    def apply(expression: pack.Value): Last = new Last(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class MaxField private (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$max", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field == other.field)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"MaxField(${field})"
  }

  object MaxField {
    def apply(field: String): MaxField =
      new MaxField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param maxExpr the `\$max` expression
   */
  final class Max private (val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$max", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Max(${expression})"
  }

  object Max {
    def apply(expression: pack.Value): Max = new Max(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/#exp._S_mergeObjects \$mergeObjects]] group accumulator.
   *
   * @param mergeExpr the `\$mergeObjects` expression
   */
  final class MergeObjects private (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction =
      pipe(f"$$mergeObjects", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"MergeObjects(${expression})"
  }

  object MergeObjects {
    def apply(expression: pack.Value): MergeObjects =
      new MergeObjects(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class MinField private (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$min", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          ==(other.field))

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"MinField(${field})"
  }

  object MinField {
    def apply(field: String): MinField = new MinField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param minExpr the `\$min` expression
   */
  final class Min(val expression: pack.Value) extends GroupFunction {
    protected[reactivemongo] val makeFunction = pipe(f"$$min", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Min(${expression})"
  }

  object Min {
    def apply(expression: pack.Value): Min = new Min(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class PushField private (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$push", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          ==(other.field))

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"PushField(${field})"
  }

  object PushField {
    def apply(field: String): PushField = new PushField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param pushExpr the `\$push` expression
   */
  final class Push private (val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$push", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Push(${expression})"
  }

  object Push {
    def apply(expression: pack.Value): Push = new Push(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class AddFieldToSet private (val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$addToSet", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          ==(other.field))

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"AddFieldToSet(${field})"
  }

  object AddFieldToSet {
    def apply(field: String): AddFieldToSet = new AddFieldToSet(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param addToSetExpr the `\$addToSet` expression
   */
  final class AddToSet private (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$addToSet", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"AddToSet(${expression})"
  }

  object AddToSet {
    def apply(expression: pack.Value): AddToSet = new AddToSet(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] group accumulator.
   *
   * @since MongoDB 3.2
   */
  final class StdDevPop private (
    val expression: pack.Value) extends GroupFunction {
    protected[reactivemongo] val makeFunction = pipe(f"$$stdDevPop", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"StdDevPop(${expression})"
  }

  object StdDevPop {
    def apply(expression: pack.Value): StdDevPop = new StdDevPop(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] for a single field.
   *
   * @since MongoDB 3.2
   * @param field $fieldParam
   */
  final class StdDevPopField private (
    val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction =
      pipe(f"$$stdDevPop", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          ==(other.field))

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"StdDevPopField(${field})"
  }

  object StdDevPopField {
    def apply(field: String): StdDevPopField = new StdDevPopField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] group accumulator
   *
   * @since MongoDB 3.2
   */
  final class StdDevSamp private (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction =
      pipe(f"$$stdDevSamp", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"StdDevSamp(${expression})"
  }

  object StdDevSamp {
    def apply(expression: pack.Value): StdDevSamp = new StdDevSamp(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] for a single field
   *
   * @since MongoDB 3.2
   * @param field $fieldParam
   */
  final class StdDevSampField private (
    val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction =
      pipe(f"$$stdDevSamp", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          ==(other.field))

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"StdDevSampField(${field})"
  }

  object StdDevSampField {
    def apply(field: String): StdDevSampField = new StdDevSampField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class SumField private (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$sum", builder.string("$" + field))

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          ==(other.field))

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"SumField(${field})"
  }

  object SumField {
    def apply(field: String): SumField = new SumField(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param sumExpr the `\$sum` expression
   */
  final class Sum private (val expression: pack.Value) extends GroupFunction {
    protected[reactivemongo] val makeFunction: pack.Document =
      pipe(f"$$sum", expression)

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression == other.expression)

      case _ =>
        false
    }

    @SuppressWarnings(Array("ComparingUnrelatedTypes", "NullParameter"))
    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Sum(${expression})"
  }

  object Sum {
    def apply(expression: pack.Value): Sum = new Sum(expression)
  }

  /** The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum `\$sum: 1`]] group accumulator. */
  case object SumAll extends GroupFunction {
    protected[reactivemongo] val makeFunction = pipe(f"$$sum", builder.int(1))
  }

  final class SumValue(val value: Int) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$sum", builder.int(value))

    override def hashCode: Int = value

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.value == other.value

      case _ =>
        false
    }

    override def toString = s"SumValue(${value.toString})"
  }
}
