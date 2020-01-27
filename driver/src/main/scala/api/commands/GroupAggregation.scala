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
    def makeFunction: pack.Value
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
        val makeFunction = pipe(name, arg)
      }
  }

  // ---

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#grp._S_avg \$avg]] group accumulator.
   *
   * @param field $fieldParam
   */
  class AvgField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$avg", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: AvgField => true
      case _           => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: AvgField =>
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

  object AvgField extends scala.runtime.AbstractFunction1[String, AvgField] {
    def apply(field: String): AvgField =
      new AvgField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(avgField: AvgField): Option[String] =
      Option(avgField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#grp._S_avg \$avg]] group accumulator.
   */
  class Avg private[api] (@deprecatedName(Symbol("avgExpr")) val expression: pack.Value) extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction = pipe(f"$$avg", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Avg => true
      case _      => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Avg =>
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

  object Avg extends scala.runtime.AbstractFunction1[pack.Value, Avg] {
    def apply(expression: pack.Value): Avg =
      new Avg(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(avg: Avg): Option[pack.Value] =
      Option(avg).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  class FirstField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$first", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: FirstField => true
      case _             => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: FirstField =>
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

  object FirstField extends scala.runtime.AbstractFunction1[String, FirstField] {
    def apply(field: String): FirstField =
      new FirstField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(firstField: FirstField): Option[String] =
      Option(firstField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   */
  class First private[api] (@deprecatedName(Symbol("firstExpr")) val expression: pack.Value) extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction = pipe(f"$$first", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: First => true
      case _        => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: First =>
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

    @deprecated("No longer a case class", "0.20.3")
    def unapply(first: First): Option[pack.Value] =
      Option(first).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  class LastField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$last", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: LastField => true
      case _            => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: LastField =>
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

  object LastField extends scala.runtime.AbstractFunction1[String, LastField] {
    def apply(field: String): LastField =
      new LastField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(lastField: LastField): Option[String] =
      Option(lastField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   */
  class Last private[api] (@deprecatedName(Symbol("lastExpr")) val expression: pack.Value) extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction = pipe(f"$$last", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Last => true
      case _       => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Last =>
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

  object Last extends scala.runtime.AbstractFunction1[pack.Value, Last] {
    def apply(expression: pack.Value): Last =
      new Last(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(last: Last): Option[pack.Value] =
      Option(last).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param field $fieldParam
   */
  class MaxField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$max", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: MaxField => true
      case _           => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: MaxField =>
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

  object MaxField extends scala.runtime.AbstractFunction1[String, MaxField] {
    def apply(field: String): MaxField =
      new MaxField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(maxField: MaxField): Option[String] =
      Option(maxField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param maxExpr the `\$max` expression
   */
  class Max(@deprecatedName(Symbol("maxExpr")) val expression: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$max", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Max => true
      case _      => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Max =>
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

  object Max extends scala.runtime.AbstractFunction1[pack.Value, Max] {
    def apply(expression: pack.Value): Max =
      new Max(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(max: Max): Option[pack.Value] =
      Option(max).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/#exp._S_mergeObjects \$mergeObjects]] group accumulator.
   *
   * @param mergeExpr the `\$mergeObjects` expression
   */
  class MergeObjects private[api] (@deprecatedName(Symbol("mergeExpr")) val expression: pack.Value) extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction = pipe(f"$$mergeObjects", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: MergeObjects => true
      case _               => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: MergeObjects =>
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

  object MergeObjects extends scala.runtime.AbstractFunction1[pack.Value, MergeObjects] {
    def apply(expression: pack.Value): MergeObjects =
      new MergeObjects(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(mergeObjects: MergeObjects): Option[pack.Value] =
      Option(mergeObjects).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param field $fieldParam
   */
  class MinField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$min", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: MinField => true
      case _           => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: MinField =>
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

  object MinField extends scala.runtime.AbstractFunction1[String, MinField] {
    def apply(field: String): MinField =
      new MinField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(minField: MinField): Option[String] =
      Option(minField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param minExpr the `\$min` expression
   */
  class Min(@deprecatedName(Symbol("minExpr")) val expression: pack.Value)
    extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction = pipe(f"$$min", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Min => true
      case _      => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Min =>
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

  object Min extends scala.runtime.AbstractFunction1[pack.Value, Min] {
    def apply(expression: pack.Value): Min =
      new Min(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(min: Min): Option[pack.Value] =
      Option(min).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param field $fieldParam
   */
  class PushField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$push", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: PushField => true
      case _            => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: PushField =>
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

  object PushField extends scala.runtime.AbstractFunction1[String, PushField] {
    def apply(field: String): PushField =
      new PushField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(pushField: PushField): Option[String] =
      Option(pushField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param pushExpr the `\$push` expression
   */
  class Push private[api] (@deprecatedName(Symbol("pushExpr")) val expression: pack.Value) extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction = pipe(f"$$push", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Push => true
      case _       => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Push =>
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

  object Push extends scala.runtime.AbstractFunction1[pack.Value, Push] {
    def apply(expression: pack.Value): Push =
      new Push(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(push: Push): Option[pack.Value] =
      Option(push).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param field $fieldParam
   */
  class AddFieldToSet private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$addToSet", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: AddFieldToSet => true
      case _                => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: AddFieldToSet =>
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

  object AddFieldToSet extends scala.runtime.AbstractFunction1[String, AddFieldToSet] {
    def apply(field: String): AddFieldToSet =
      new AddFieldToSet(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(addFieldToSet: AddFieldToSet): Option[String] =
      Option(addFieldToSet).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param addToSetExpr the `\$addToSet` expression
   */
  class AddToSet private[api] (@deprecatedName(Symbol("addToSetExpr")) val expression: pack.Value) extends GroupFunction with Product1[pack.Value] with Serializable {
    val makeFunction = pipe(f"$$addToSet", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: AddToSet => true
      case _           => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: AddToSet =>
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

  object AddToSet extends scala.runtime.AbstractFunction1[pack.Value, AddToSet] {
    def apply(expression: pack.Value): AddToSet =
      new AddToSet(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(addToSet: AddToSet): Option[pack.Value] =
      Option(addToSet).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] group accumulator.
   *
   * @since MongoDB 3.2
   */
  class StdDevPop private[api] (val expression: pack.Value) extends GroupFunction
    with Product1[pack.Value] with Serializable {
    val makeFunction = pipe(f"$$stdDevPop", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: StdDevPop => true
      case _            => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: StdDevPop =>
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

  object StdDevPop extends scala.runtime.AbstractFunction1[pack.Value, StdDevPop] {
    def apply(expression: pack.Value): StdDevPop =
      new StdDevPop(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(stdDevPop: StdDevPop): Option[pack.Value] =
      Option(stdDevPop).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] for a single field.
   *
   * @since MongoDB 3.2
   * @param field $fieldParam
   */
  class StdDevPopField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$stdDevPop", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: StdDevPopField => true
      case _                 => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: StdDevPopField =>
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

  object StdDevPopField extends scala.runtime.AbstractFunction1[String, StdDevPopField] {
    def apply(field: String): StdDevPopField =
      new StdDevPopField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(stdDevPopField: StdDevPopField): Option[String] =
      Option(stdDevPopField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] group accumulator
   *
   * @since MongoDB 3.2
   */
  class StdDevSamp private[api] (val expression: pack.Value)
    extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction = pipe(f"$$stdDevSamp", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: StdDevSamp => true
      case _             => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: StdDevSamp =>
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

  object StdDevSamp extends scala.runtime.AbstractFunction1[pack.Value, StdDevSamp] {
    def apply(expression: pack.Value): StdDevSamp =
      new StdDevSamp(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(stdDevSamp: StdDevSamp): Option[pack.Value] =
      Option(stdDevSamp).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] for a single field
   *
   * @since MongoDB 3.2
   * @param field $fieldParam
   */
  class StdDevSampField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$stdDevSamp", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: StdDevSampField => true
      case _                  => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: StdDevSampField =>
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

  object StdDevSampField extends scala.runtime.AbstractFunction1[String, StdDevSampField] {
    def apply(field: String): StdDevSampField =
      new StdDevSampField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(stdDevSampField: StdDevSampField): Option[String] =
      Option(stdDevSampField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param field $fieldParam
   */
  class SumField private[api] (val field: String) extends GroupFunction
    with Product1[String] with Serializable {

    val makeFunction = pipe(f"$$sum", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: SumField => true
      case _           => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: SumField =>
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

  object SumField extends scala.runtime.AbstractFunction1[String, SumField] {
    def apply(field: String): SumField =
      new SumField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(sumField: SumField): Option[String] =
      Option(sumField).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param sumExpr the `\$sum` expression
   */
  class Sum private[api] (@deprecatedName(Symbol("sumExpr")) val expression: pack.Value) extends GroupFunction with Product1[pack.Value] with Serializable {

    val makeFunction: pack.Document = pipe(f"$$sum", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Sum => true
      case _      => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Sum =>
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

  object Sum extends scala.runtime.AbstractFunction1[pack.Value, Sum] {
    def apply(expression: pack.Value): Sum =
      new Sum(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(sum: Sum): Option[pack.Value] =
      Option(sum).map(_.expression)
  }

  /** The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum `\$sum: 1`]] group accumulator. */
  case object SumAll extends GroupFunction {
    val makeFunction = pipe(f"$$sum", builder.int(1))
  }

  @deprecated("Use `SumAll`", "0.12.0")
  case class SumValue(value: Int) extends GroupFunction {
    val makeFunction = pipe(f"$$sum", builder.int(value))
  }
}
