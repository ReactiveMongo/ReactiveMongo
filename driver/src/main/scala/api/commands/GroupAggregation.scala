package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * @define fieldParam the field name
 */
private[commands] trait GroupAggregation[P <: SerializationPack] {
  aggregation: AggregationFramework[P] =>

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
  final class AvgField private[api] (val field: String)
    extends GroupFunction {

    protected[reactivemongo] val makeFunction =
      pipe(f"$$avg", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"AvgField(${field})"
  }

  object AvgField {
    def apply(field: String): AvgField =
      new AvgField(field)

    def unapply(avgField: AvgField): Option[String] =
      Option(avgField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#grp._S_avg \$avg]] group accumulator.
   */
  final class Avg private[api] (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$avg", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Avg(${expression})"
  }

  object Avg {
    def apply(expression: pack.Value): Avg =
      new Avg(expression)

    def unapply(avg: Avg): Option[pack.Value] =
      Option(avg).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class FirstField private[api] (val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$first", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"FirstField(${field})"
  }

  object FirstField {
    def apply(field: String): FirstField =
      new FirstField(field)

    def unapply(firstField: FirstField): Option[String] =
      Option(firstField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   */
  final class First private[api] (val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$first", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"First(${expression})"
  }

  object First extends scala.runtime.AbstractFunction1[pack.Value, First] {
    def apply(expression: pack.Value): First =
      new First(expression)

    def unapply(first: First): Option[pack.Value] =
      Option(first).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class LastField private[api] (val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$last", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"LastField(${field})"
  }

  object LastField {
    def apply(field: String): LastField =
      new LastField(field)

    def unapply(lastField: LastField): Option[String] =
      Option(lastField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   */
  final class Last private[api] (val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$last", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Last(${expression})"
  }

  object Last {
    def apply(expression: pack.Value): Last =
      new Last(expression)

    def unapply(last: Last): Option[pack.Value] =
      Option(last).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class MaxField private[api] (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction = pipe(f"$$max", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"MaxField(${field})"
  }

  object MaxField {
    def apply(field: String): MaxField =
      new MaxField(field)

    def unapply(maxField: MaxField): Option[String] =
      Option(maxField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param maxExpr the `\$max` expression
   */
  final class Max private[api] (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$max", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Max(${expression})"
  }

  object Max {
    def apply(expression: pack.Value): Max =
      new Max(expression)

    def unapply(max: Max): Option[pack.Value] =
      Option(max).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/#exp._S_mergeObjects \$mergeObjects]] group accumulator.
   *
   * @param mergeExpr the `\$mergeObjects` expression
   */
  final class MergeObjects private[api] (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction =
      pipe(f"$$mergeObjects", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"MergeObjects(${expression})"
  }

  object MergeObjects {
    def apply(expression: pack.Value): MergeObjects =
      new MergeObjects(expression)

    def unapply(mergeObjects: MergeObjects): Option[pack.Value] =
      Option(mergeObjects).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class MinField private[api] (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction =
      pipe(f"$$min", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"MinField(${field})"
  }

  object MinField {
    def apply(field: String): MinField =
      new MinField(field)

    def unapply(minField: MinField): Option[String] =
      Option(minField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param minExpr the `\$min` expression
   */
  final class Min(val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$min", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Min(${expression})"
  }

  object Min {
    def apply(expression: pack.Value): Min =
      new Min(expression)

    def unapply(min: Min): Option[pack.Value] =
      Option(min).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class PushField private[api] (val field: String) extends GroupFunction {
    protected[reactivemongo] val makeFunction = pipe(f"$$push", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"PushField(${field})"
  }

  object PushField {
    def apply(field: String): PushField =
      new PushField(field)

    def unapply(pushField: PushField): Option[String] =
      Option(pushField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param pushExpr the `\$push` expression
   */
  final class Push private[api] (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$push", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Push(${expression})"
  }

  object Push {
    def apply(expression: pack.Value): Push =
      new Push(expression)

    def unapply(push: Push): Option[pack.Value] =
      Option(push).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class AddFieldToSet private[api] (
    val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$addToSet", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"AddFieldToSet(${field})"
  }

  object AddFieldToSet {
    def apply(field: String): AddFieldToSet =
      new AddFieldToSet(field)

    def unapply(addFieldToSet: AddFieldToSet): Option[String] =
      Option(addFieldToSet).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param addToSetExpr the `\$addToSet` expression
   */
  final class AddToSet private[api] (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$addToSet", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"AddToSet(${expression})"
  }

  object AddToSet {
    def apply(expression: pack.Value): AddToSet =
      new AddToSet(expression)

    def unapply(addToSet: AddToSet): Option[pack.Value] =
      Option(addToSet).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] group accumulator.
   *
   * @since MongoDB 3.2
   */
  final class StdDevPop private[api] (
    val expression: pack.Value) extends GroupFunction {
    protected[reactivemongo] val makeFunction = pipe(f"$$stdDevPop", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"StdDevPop(${expression})"
  }

  object StdDevPop extends {
    def apply(expression: pack.Value): StdDevPop =
      new StdDevPop(expression)

    def unapply(stdDevPop: StdDevPop): Option[pack.Value] =
      Option(stdDevPop).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] for a single field.
   *
   * @since MongoDB 3.2
   * @param field $fieldParam
   */
  final class StdDevPopField private[api] (
    val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$stdDevPop", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"StdDevPopField(${field})"
  }

  object StdDevPopField {
    def apply(field: String): StdDevPopField =
      new StdDevPopField(field)

    def unapply(stdDevPopField: StdDevPopField): Option[String] =
      Option(stdDevPopField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] group accumulator
   *
   * @since MongoDB 3.2
   */
  final class StdDevSamp private[api] (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$stdDevSamp", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"StdDevSamp(${expression})"
  }

  object StdDevSamp {
    def apply(expression: pack.Value): StdDevSamp =
      new StdDevSamp(expression)

    def unapply(stdDevSamp: StdDevSamp): Option[pack.Value] =
      Option(stdDevSamp).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] for a single field
   *
   * @since MongoDB 3.2
   * @param field $fieldParam
   */
  final class StdDevSampField private[api] (
    val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$stdDevSamp", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"StdDevSampField(${field})"
  }

  object StdDevSampField {
    def apply(field: String): StdDevSampField =
      new StdDevSampField(field)

    def unapply(stdDevSampField: StdDevSampField): Option[String] =
      Option(stdDevSampField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param field $fieldParam
   */
  final class SumField private[api] (val field: String) extends GroupFunction {

    protected[reactivemongo] val makeFunction = pipe(f"$$sum", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"SumField(${field})"
  }

  object SumField {
    def apply(field: String): SumField =
      new SumField(field)

    def unapply(sumField: SumField): Option[String] =
      Option(sumField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param sumExpr the `\$sum` expression
   */
  final class Sum private[api] (
    val expression: pack.Value) extends GroupFunction {

    protected[reactivemongo] val makeFunction: pack.Document =
      pipe(f"$$sum", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Sum(${expression})"
  }

  object Sum {
    def apply(expression: pack.Value): Sum =
      new Sum(expression)

    def unapply(sum: Sum): Option[pack.Value] =
      Option(sum).map(_.expression)
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
