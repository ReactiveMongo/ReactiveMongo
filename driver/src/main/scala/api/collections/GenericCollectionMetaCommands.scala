package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{ Collation, SerializationPack }

import reactivemongo.api.commands.{
  CollectionCommand,
  Command,
  CommandKind,
  CommandWithResult,
  ResolvedCollectionCommand
}

/** The meta commands for collection that require the serialization pack. */
private[reactivemongo] trait GenericCollectionMetaCommands[P <: SerializationPack] { self: GenericCollection[P] =>

  /**
   * [[https://docs.mongodb.com/manual/reference/method/db.createView/ Creates a view]] on this collection, using an aggregation pipeline.
   *
   * @since MongoDB 3.4
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.{ BSONDocument, BSONString }
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def foo(coll: BSONCollection)(implicit ec: ExecutionContext) = {
   *   import coll.AggregationFramework
   *   import AggregationFramework.{ Group, Match, SumField }
   *
   *   // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-states-with-populations-above-10-million
   *
   *   // Create 'myview'
   *   coll.createView(
   *     name = "myview",
   *     operator = Group(BSONString(f"$$state"))(
   *       "totalPop" -> SumField("population")),
   *     pipeline = Seq(Match(
   *       BSONDocument("totalPop" -> BSONDocument(f"$$gte" -> 10000000L)))))
   *
   *   // Then the view can be resolved as any collection
   *   // (but won't be writeable)
   *   coll.db[BSONCollection]("myview")
   * }
   * }}}
   *
   * @param name the name of the view to be created
   * @param operator the first (required) operator for the aggregation pipeline
   * @param pipeline the other pipeline operators
   * @param collation the view collation
   */
  def createView(
    name: String,
    operator: PipelineOperator,
    pipeline: Seq[PipelineOperator],
    collation: Option[Collation] = None)(implicit ec: ExecutionContext): Future[Unit] = {

    val cmd = new CreateView(name, operator, pipeline, collation)

    // Command codecs
    implicit def writer: pack.Writer[CreateViewCommand] = createViewWriter

    command(self, cmd, writePreference)
  }

  // ---

  private final class CreateView(
    val viewName: String,
    val operator: PipelineOperator,
    val pipeline: Seq[PipelineOperator],
    val collation: Option[Collation]) extends CollectionCommand with CommandWithResult[Unit] {
    val commandKind = CommandKind.CreateView
  }

  private type CreateViewCommand = ResolvedCollectionCommand[CreateView]

  private lazy val createViewWriter: pack.Writer[CreateViewCommand] = {
    val builder = pack.newBuilder
    import builder.{ elementProducer => element, string }

    pack.writer { (cmd: CreateViewCommand) =>
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("create", string(cmd.command.viewName)),
        element("viewOn", string(cmd.collection)))

      val pipeline = builder.array(
        cmd.command.operator.makePipe +: cmd.command.pipeline.map(_.makePipe))

      elements += element("pipeline", pipeline)

      cmd.command.collation.foreach { collation =>
        elements += element("collation", Collation.serialize(pack, collation))
      }

      builder.document(elements.result())
    }
  }

  // Command runner
  private lazy val command = Command.run(pack, failoverStrategy)
}
