package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.SerializationPack

import reactivemongo.api.commands.{
  CollectionCommand,
  Command,
  CommandWithResult,
  Collation,
  ResolvedCollectionCommand,
  UnitBox
}

// TODO: Move to CollectionMetaCommands in parent package
/** The meta commands for collection that require the serialization pack. */
private[reactivemongo] trait GenericCollectionMetaCommands[P <: SerializationPack with Singleton] { self: GenericCollection[P] =>

  /**
   * Creates a view on this collection, using an aggregation pipeline.
   *
   * {{{
   * import coll.BatchCommands.AggregationFramework
   * import AggregationFramework.{ Group, Match, SumField }
   *
   * // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-states-with-populations-above-10-million
   *
   * // Create 'myview'
   * coll.createView(
   *   name = "myview",
   *   operator = Group(BSONString(f"$$state"))(
   *     "totalPop" -> SumField("population")),
   * pipeline = Seq(
   * Match(document("totalPop" -> document(f"$$gte" -> 10000000L)))))
   *
   *
   * // Then the view can be resolved as any collection
   * // (but won't be writeable)
   * val view: BSONCollection = db("myview")
   * }}}
   *
   * @param name the name of the view to be created
   * @param operator the first (required) operator for the aggregation pipeline
   * @param pipeline the other pipeline operators
   * @param collation the view collation
   *
   * @see [[https://docs.mongodb.com/manual/reference/method/db.createView/ db.createView]]
   */
  def createView( // TODO: Avoid GenericCollection with write op for view
    name: String,
    operator: PipelineOperator,
    pipeline: Seq[PipelineOperator],
    collation: Option[Collation] = None)(implicit ec: ExecutionContext): Future[Unit] = {

    val cmd = new CreateView(name, operator, pipeline, collation)

    // Command codecs
    implicit def writer = createViewWriter

    command.unboxed(self, cmd, writePreference)
  }

  // ---

  private final class CreateView(
    val viewName: String,
    val operator: PipelineOperator,
    val pipeline: Seq[PipelineOperator],
    val collation: Option[Collation]) extends CollectionCommand with CommandWithResult[UnitBox.type]

  private type CreateViewCommand = ResolvedCollectionCommand[CreateView]

  private lazy val createViewWriter: pack.Writer[CreateViewCommand] = {
    val builder = pack.newBuilder
    import builder.{ elementProducer => element, string }

    pack.writer { cmd: CreateViewCommand =>
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("create", string(cmd.command.viewName)),
        element("viewOn", string(cmd.collection)))

      val pipeline = builder.array(
        cmd.command.operator.makePipe,
        cmd.command.pipeline.map(_.makePipe))

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
