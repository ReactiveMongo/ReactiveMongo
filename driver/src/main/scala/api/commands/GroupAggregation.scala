package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

private[commands] trait GroupAggregation[P <: SerializationPack] {
  aggregation: AggregationFramework[P] =>

  @inline private def document(name: String, arg: pack.Value): pack.Document =
    builder.document(Seq(builder.elementProducer(name, arg)))

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
        val makeFunction = document(name, arg)
      }
  }

  // ---

  case class SumField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$sum", builder.string("$" + field))
  }

  /**
   * @param sumExpr the `\$sum` expression
   */
  case class Sum(sumExpr: pack.Value) extends GroupFunction {
    val makeFunction: pack.Document = document(f"$$sum", sumExpr)
  }

  /** Sum operation of the form `\$sum: 1` */
  case object SumAll extends GroupFunction {
    val makeFunction = document(f"$$sum", builder.int(1))
  }

  @deprecated("Use [[SumAll]]", "0.12.0")
  case class SumValue(value: Int) extends GroupFunction {
    val makeFunction = document(f"$$sum", builder.int(value))
  }

  case class AvgField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$avg", builder.string("$" + field))
  }

  case class Avg(avgExpr: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$avg", avgExpr)
  }

  case class FirstField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$first", builder.string("$" + field))
  }

  case class First(firstExpr: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$first", firstExpr)
  }

  case class LastField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$last", builder.string("$" + field))
  }

  case class Last(lastExpr: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$last", lastExpr)
  }

  case class MaxField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$max", builder.string("$" + field))
  }

  /**
   * @param maxExpr the `\$max` expression
   */
  case class Max(maxExpr: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$max", maxExpr)
  }

  case class MinField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$min", builder.string("$" + field))
  }

  /**
   * @param minExpr the `\$min` expression
   */
  case class Min(minExpr: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$min", minExpr)
  }

  case class PushField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$push", builder.string("$" + field))
  }

  /**
   * @param pushExpr the `\$push` expression
   */
  case class Push(pushExpr: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$push", pushExpr)
  }

  /**
   * Since MongoDB 3.4
   *
   * @param specifications The fields to include. The resulting objects will also contain these fields.
   * @see https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/
   */
  case class AddFields(specifications: pack.Document) extends PipelineOperator {
    val makePipe = document(f"$$addFields", specifications)
  }

  case class AddFieldToSet(field: String) extends GroupFunction {
    val makeFunction = document(f"$$addToSet", builder.string("$" + field))
  }

  /**
   * @param addToSetExpr the `\$addToSet` expression
   */
  case class AddToSet(addToSetExpr: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$addToSet", addToSetExpr)
  }

  /** The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] group accumulator (since MongoDB 3.2) */
  case class StdDevPop(expression: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$stdDevPop", expression)
  }

  /** The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]] for a single field (since MongoDB 3.2) */
  case class StdDevPopField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$stdDevPop", builder.string("$" + field))
  }

  /** The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] group accumulator (since MongoDB 3.2) */
  case class StdDevSamp(expression: pack.Value) extends GroupFunction {
    val makeFunction = document(f"$$stdDevSamp", expression)
  }

  /** The [[https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]] for a single field (since MongoDB 3.2) */
  case class StdDevSampField(field: String) extends GroupFunction {
    val makeFunction = document(f"$$stdDevSamp", builder.string("$" + field))
  }
}
