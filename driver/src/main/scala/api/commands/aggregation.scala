package reactivemongo.api.commands

import scala.concurrent.Future
import reactivemongo.api.{ BSONSerializationPack, Cursor, SerializationPack }

// TODO
trait AggregationFramework[P <: SerializationPack] {
  case class AggregateCursorOptions(batchSize: Int = 0)

  // pipeline should be written by the writer by merging all documents into one
  case class Aggregate(pipeline: Stream[P#Document], cursorOptions: Option[AggregateCursorOptions] = Some(AggregateCursorOptions())) extends CollectionCommand with CommandWithPack[P] with CursorCommand {
    final def needsCursor = cursorOptions.isDefined
  }

  //case class BSONAgg(pipeline: Stream[BSONSerializationPack.Document], cursorOptions: Option[AggregateCursorOptions] = Some(AggregateCursorOptions())) extends Aggregate[BSONSerializationPack.type](pipeline, cursorOptions)

  object Aggregate {
    def apply(stages: PipelineStageDocumentProducer*): Aggregate = Aggregate(stages.toStream.map(_.produce), Some(AggregateCursorOptions()))
    def apply(cursorOptions: Option[AggregateCursorOptions], stages: PipelineStageDocumentProducer*): Aggregate = Aggregate(stages.toStream.map(_.produce), cursorOptions)
  }

  trait PipelineStageDocumentProducer {
    def produce: P#Document
  }

  //case class DocumentStagePipelineStageDocumentProducern

  object PipelineStageDocumentProducer {
    implicit def make[A <: PipelineStage with DocumentStage](ds: A)(implicit writer: P#Writer[A]) = new PipelineStageDocumentProducer {
      def produce: P#Document = ds.document
    }
  }

  trait PipelineStage {
    def name: String
  }
  trait DocumentStage { self: PipelineStage =>
    def document: P#Document
  }
  trait DocumentStageCompanion[D <: DocumentStage] extends (P#Document => D) { self: Singleton  =>
    def apply(doc: P#Document): D
    def apply[A](a: A)(implicit writer: P#Writer[A]): D = apply(??? : P#Document)
  }

  case class Project(document: P#Document) extends PipelineStage with DocumentStage {
    final def name = "$project"
  }
  object Project extends DocumentStageCompanion[Project]

  case class Match(document: P#Document) extends PipelineStage with DocumentStage {
    final def name = "$match"
  }
  object Match extends DocumentStageCompanion[Match]

  case class Redact(document: P#Document) extends PipelineStage with DocumentStage {  // mongo 2.6
    final def name = "$redact"
  }
  object Redact extends DocumentStageCompanion[Redact]

  case class Limit(n: Int) extends PipelineStage { final def name = "$limit" }

  case class Skip(n: Int) extends PipelineStage { final def name = "$skip" }

  class Unwind(field: String) extends PipelineStage {
    final def name = "$unwind"
    val prefixedField = if(field.startsWith("$")) field else ("$" + field)
    override def equals(any: Any) = any match {
      case Unwind(p) => p == prefixedField
      case _ => false
    }
    override def hashCode =  41 * (41 + prefixedField.hashCode)
  }
  object Unwind {
    def apply(field: String): Unwind = new Unwind(field)
    def unapply(u: Unwind): Option[String] = Some(u.prefixedField)
  }

  case class Group(document: P#Document) extends PipelineStage with DocumentStage {
    final def name = "$group"
  }
  object Group extends DocumentStageCompanion[Group]

  case class Sort(document: P#Document) extends PipelineStage with DocumentStage {
    final def name = "$sort"
  }
  object Sort extends DocumentStageCompanion[Sort]

  case class GeoNear(document: P#Document) extends PipelineStage with DocumentStage {
    final def name = "$geoNear"
  }
  object GeoNear extends DocumentStageCompanion[GeoNear]

  case class Out(collection: String) extends PipelineStage {
    final def name = "$out"
  }
  /*
    { aggregate: "records",
       pipeline: [
          { $project: { name: 1, email: 1, _id: 0 } },
          { $sort: { name: 1 } }
       ],
       cursor: { batchSize: 0 }
     }
  */
}

object tst2 {
  import reactivemongo.bson._
  val bsonAgg = new AggregationFramework[BSONSerializationPack.type] {}
  import bsonAgg._
  class Toto
  val toto = new Toto
  implicit val www: BSONDocumentWriter[Toto] = ???
  implicit val ttt: BSONDocumentWriter[Project] = ???
  Aggregate(None, Project(BSONDocument()))
}
