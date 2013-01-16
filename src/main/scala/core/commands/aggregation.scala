package core.commands

import reactivemongo.bson._
import reactivemongo.bson.BSONString
import reactivemongo.core.commands.{CommandError, BSONCommandResultMaker, Command}

case class Aggregate (
  collectionName: String,
  pipeline: Seq[PipelineOperator]
) extends Command[BSONValue] {
  override def makeDocuments =
    BSONDocument(
      "aggregate" -> BSONString(collectionName),
      "pipeline" -> BSONArray(
        {for (pipe <- pipeline) yield pipe.makePipe} : _*
      )
    )

  val ResultMaker = Aggregate
}

object Aggregate extends BSONCommandResultMaker[BSONValue] {
  def apply(document: TraversableBSONDocument) =
    CommandError.checkOk(document, Some("aggregate")).toLeft(document.get("result").get)
}

sealed trait PipelineOperator {
  def makePipe: BSONValue
}

case class Project(fields: String*) extends PipelineOperator{
  override val makePipe = BSONDocument("$project" -> BSONDocument(
    {for (field <- fields) yield field -> BSONInteger(1)} : _*
  ))
}

case class Match(predicate: BSONDocument) extends PipelineOperator{
  override val makePipe = BSONDocument("$match" -> predicate)
}

case class Limit(limit: Int) extends PipelineOperator{
  override val makePipe = BSONDocument("$limit" -> BSONInteger(limit))
}

case class Skip(skip: Int) extends PipelineOperator{
  override val makePipe = BSONDocument("$skip" -> BSONInteger(skip))
}

case class Unwind(field: String) extends PipelineOperator{
  override val makePipe = BSONDocument("$unwind" -> BSONString("$" + field))
}

case class GroupField(idField: String)(ops: (String, GroupFunction)*) extends PipelineOperator {
  override val makePipe = BSONDocument(
    "$group" -> BSONDocument(
    {"_id" -> BSONString(idField)}
      +: {ops.map{
        case (field, operator) => field -> operator.makeFunction
      }}:_*
    )
  )
}

case class GroupMulti(idField: (String, String)*)(ops: (String, GroupFunction)*) extends PipelineOperator {
  override val makePipe = BSONDocument(
    "$group" -> BSONDocument(
      {"_id" -> BSONDocument(
        idField.map{
          case (alias, attribute) => alias -> BSONString("$" + attribute)
        }:_*
      )} +:
      {ops.map{
        case (field, operator) => field -> operator.makeFunction
      }}:_*
    )
  )
}

case class Sort(fields: Seq[SortOrder]) extends PipelineOperator{
  override val makePipe = BSONDocument("$sort" -> BSONDocument(fields.map{
    case Ascending(field) => field -> BSONInteger(1)
    case Descending(field) => field -> BSONInteger(-1)
  } : _*))
}

sealed trait SortOrder
case class Ascending(field: String) extends SortOrder
case class Descending(field: String) extends SortOrder

sealed trait GroupFunction {
  def makeFunction: BSONValue
}

case class AddToSet(field: String) extends GroupFunction{
  def makeFunction = BSONDocument("$addToSet" -> BSONString(field))
}

case class First(field: String) extends GroupFunction {
  def makeFunction = BSONDocument("$first" -> BSONString("$" + field))
}

case class Last(field: String) extends GroupFunction {
  def makeFunction = BSONDocument("$last" -> BSONString("$" + field))
}

case class Max(field: String) extends GroupFunction {
  def makeFunction = BSONDocument("$max" -> BSONString("$" + field))
}

case class Min(field: String) extends GroupFunction {
  def makeFunction = BSONDocument("$min" -> BSONString("$" + field))
}

case class Avg(field: String) extends GroupFunction {
  def makeFunction = BSONDocument("$avg" -> BSONString("$" + field))
}

case class Push(field: String) extends GroupFunction {
  def makeFunction = BSONDocument("$push" -> BSONString("$" + field))
}

case class SumField(field: String) extends GroupFunction {
  def makeFunction = BSONDocument("$sum" -> BSONString("$" + field))
}

case class SumValue(value: Int) extends GroupFunction {
  def makeFunction = BSONDocument("$sum" -> BSONInteger(value))
}
