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
  case class AvgField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$avg", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#grp._S_avg \$avg]] group accumulator.
   */
  case class Avg(avgExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$avg", avgExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  case class FirstField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$first", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/first/#grp._S_first \$field]] group accumulator.
   */
  case class First(firstExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$first", firstExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   *
   * @param field $fieldParam
   */
  case class LastField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$last", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/last/#grp._S_last \$field]] group accumulator.
   */
  case class Last(lastExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$last", lastExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param field $fieldParam
   */
  case class MaxField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$max", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/max/#grp._S_max \$max]] group accumulator.
   *
   * @param maxExpr the `\$max` expression
   */
  case class Max(maxExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$max", maxExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/#exp._S_mergeObjects \$mergeObjects]]
   * group accumulator.
   *
   * @param mergeExpr the `\$mergeObjects` expression
   */
  case class MergeObjects(mergeExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$mergeObjects", mergeExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param field $fieldParam
   */
  case class MinField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$min", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/min/#grp._S_min \$min]] group accumulator.
   *
   * @param minExpr the `\$min` expression
   */
  case class Min(minExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$min", minExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param field $fieldParam
   */
  case class PushField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$push", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/push/#grp._S_push \$push]] group accumulator.
   *
   * @param pushExpr the `\$push` expression
   */
  case class Push(pushExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$push", pushExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param field $fieldParam
   */
  case class AddFieldToSet(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$addToSet", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/addToSet/ \$addToSet]] group accumulator.
   *
   * @param addToSetExpr the `\$addToSet` expression
   */
  case class AddToSet(addToSetExpr: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$addToSet", addToSetExpr)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] group accumulator
   * (since MongoDB 3.2)
   */
  case class StdDevPop(expression: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$stdDevPop", expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] for a single field (since MongoDB 3.2)
   *
   * @param field $fieldParam
   */
  case class StdDevPopField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$stdDevPop", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] group accumulator
   * (since MongoDB 3.2)
   */
  case class StdDevSamp(expression: pack.Value) extends GroupFunction {
    val makeFunction = pipe(f"$$stdDevSamp", expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] for a single field
   * (since MongoDB 3.2)
   *
   * @param field $fieldParam
   */
  case class StdDevSampField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$stdDevSamp", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param field $fieldParam
   */
  case class SumField(field: String) extends GroupFunction {
    val makeFunction = pipe(f"$$sum", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sum/#grp._S_sum \$sum]] group accumulator.
   *
   * @param sumExpr the `\$sum` expression
   */
  case class Sum(sumExpr: pack.Value) extends GroupFunction {
    val makeFunction: pack.Document = pipe(f"$$sum", sumExpr)
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
