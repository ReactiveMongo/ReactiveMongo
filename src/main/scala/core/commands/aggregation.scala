package core.commands

import reflect.macros.Context
import language.experimental.macros

import reactivemongo.bson._
import reactivemongo.bson.BSONString
import reactivemongo.core.commands.{BSONCommandResultMaker, Command}

case class Aggregate[T](
  name: String
)(
  collectionName: String,
  pipeline: Seq[PipelineOperator]
) extends Command[T] {
  override def makeDocuments =
    BSONDocument(
      "aggregate" -> BSONString(collectionName),
      "pipeline" -> BSONArray(
        {for (pipe <- pipeline) yield pipe.makePipe} : _*
      )
    )

  val ResultMaker = throw new UnsupportedOperationException

  def rm_impl(c: Context)()
}

sealed trait PipelineOperator {
  def makePipe: BSONValue
}

case class Project(fields: Seq[String]) extends PipelineOperator{
  override val makePipe = BSONDocument(
    {for (field <- fields) yield field -> BSONInteger(1)} : _*
  )
}

case class Match(predicate: BSONDocument) extends PipelineOperator{
  override val makePipe = predicate
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

case class Group(idField: String*)(ops: (String, GroupFunction)*) extends PipelineOperator{
  override val makePipe = BSONDocument(
    "$group" -> BSONDocument(
      ops.map{
        case (field, operator) => field -> operator.makeFunction
      }:_*
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
