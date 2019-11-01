package reactivemongo.core.commands

import reactivemongo.bson._

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Aggregate(
  collectionName: String,
  pipeline: Seq[PipelineOperator]) extends Command[Stream[BSONDocument]] {
  override def makeDocuments =
    BSONDocument(
      "aggregate" -> BSONString(collectionName),
      "pipeline" -> BSONArray(
        { for (pipe <- pipeline) yield pipe.makePipe }.toStream))

  val ResultMaker = Aggregate
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
object Aggregate extends BSONCommandResultMaker[Stream[BSONDocument]] {
  def apply(document: BSONDocument) =
    CommandError.checkOk(document, Some("aggregate")).toLeft(document.get("result").get.asInstanceOf[BSONArray].values.map(_.asInstanceOf[BSONDocument]))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
sealed trait PipelineOperator {
  def makePipe: BSONValue
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Project(fields: (String, BSONValue)*) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$project" -> BSONDocument(
    { for (field <- fields) yield field._1 -> field._2 }.toStream))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Match(predicate: BSONDocument) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$match" -> predicate)
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Limit(limit: Int) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$limit" -> BSONInteger(limit))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Skip(skip: Int) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$skip" -> BSONInteger(skip))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Unwind(field: String) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$unwind" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class GroupField(idField: String)(ops: (String, GroupFunction)*) extends PipelineOperator {
  override val makePipe = Group(BSONString("$" + idField))(ops: _*).makePipe
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class GroupMulti(idField: (String, String)*)(ops: (String, GroupFunction)*) extends PipelineOperator {
  override val makePipe = Group(BSONDocument(
    idField.map {
      case (alias, attribute) => alias -> BSONString("$" + attribute)
    }.toStream))(ops: _*).makePipe
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Group(identifiers: BSONValue)(ops: (String, GroupFunction)*) extends PipelineOperator {
  override val makePipe = BSONDocument(
    f"$$group" -> BSONDocument(
      {
        "_id" -> identifiers
      } +:
        {
          ops.map {
            case (field, operator) => field -> operator.makeFunction
          }
        }.toStream))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Sort(fields: Seq[SortOrder]) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$sort" -> BSONDocument(fields.map {
    case Ascending(field)  => field -> BSONInteger(1)
    case Descending(field) => field -> BSONInteger(-1)
  }.toStream))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
sealed trait SortOrder

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Ascending(field: String) extends SortOrder

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Descending(field: String) extends SortOrder

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class ReplaceRootField(newRoot: String) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$replaceRoot" -> BSONDocument("newRoot" -> BSONString("$" + newRoot)))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class ReplaceRoot(newRoot: BSONDocument) extends PipelineOperator {
  override val makePipe = BSONDocument(f"$$replaceRoot" -> BSONDocument("newRoot" -> newRoot))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
sealed trait GroupFunction {
  def makeFunction: BSONValue
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
object GroupFunction {
  def apply(name: String, arg: BSONValue): GroupFunction = new GroupFunction {
    val makeFunction = BSONDocument(name -> arg)
  }
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class AddToSet(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$addToSet" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class First(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$first" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Last(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$last" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Max(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$max" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Min(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$min" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Avg(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$avg" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class Push(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$push" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class PushMulti(fields: (String, String)*) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$push" -> BSONDocument(
    fields.map(field => field._1 -> BSONString("$" + field._2))))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class SumField(field: String) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$sum" -> BSONString("$" + field))
}

@deprecated(
  message = "Use [[reactivemongo.api.collections.GenericCollection.aggregateWith]]",
  since = "0.12-RC5")
case class SumValue(value: Int) extends GroupFunction {
  val makeFunction = BSONDocument(f"$$sum" -> BSONInteger(value))
}
