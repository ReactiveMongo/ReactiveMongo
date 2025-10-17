package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{
  Collation,
  ReadConcern,
  ReadPreference,
  SerializationPack
}
import reactivemongo.api.commands.{
  CollectionCommand,
  CommandCodecs,
  CommandKind,
  CommandWithResult,
  ResolvedCollectionCommand
}

private[api] trait CountOp[P <: SerializationPack] {
  collection: GenericCollection[P] =>

  implicit private lazy val countWriter: pack.Writer[CountCmd] = commandWriter
  implicit private lazy val countReader: pack.Reader[Long] = resultReader

  protected[api] def countDocuments(
      query: Option[pack.Document],
      limit: Option[Int],
      skip: Int,
      hint: Option[Hint],
      readConcern: ReadConcern,
      maxTime: Option[FiniteDuration],
      collation: Option[Collation],
      comment: Option[String],
      readPreference: ReadPreference
    )(implicit
      ec: ExecutionContext
    ): Future[Long] = runCommand(
    new CountCommand(
      query,
      limit,
      skip,
      hint,
      readConcern,
      maxTimeMS = maxTime.map(_.toMillis),
      collation,
      comment
    ),
    readPreference
  )

  private final class CountCommand(
      val query: Option[pack.Document],
      val limit: Option[Int],
      val skip: Int,
      val hint: Option[Hint],
      val readConcern: ReadConcern,
      val maxTimeMS: Option[Long],
      val collation: Option[Collation],
      val comment: Option[String])
      extends CollectionCommand
      with CommandWithResult[Long] {
    val commandKind = CommandKind.Count
  }

  private type CountCmd = ResolvedCollectionCommand[CountCommand]

  private def commandWriter: pack.Writer[CountCmd] = {
    val builder = pack.newBuilder
    val session = collection.db.session.filter(_ =>
      (version.compareTo(MongoWireVersion.V36) >= 0)
    )

    val writeReadConcern =
      CommandCodecs.writeSessionReadConcern(builder)(session)

    pack.writer[CountCmd] { count =>
      import builder.{ document, elementProducer => element, int, string, long }
      import count.command

      val elements = Seq.newBuilder[pack.ElementProducer]

      elements += element("count", string(count.collection))
      elements += element("skip", int(command.skip))

      command.query.foreach { query => elements += element("query", query) }

      command.limit.foreach { limit =>
        elements += element("limit", int(limit))
      }

      command.hint.foreach {
        case HintString(name) =>
          elements += element("hint", string(name))

        case HintDocument(spec) =>
          elements += element("hint", spec)
      }

      elements ++= writeReadConcern(count.command.readConcern)

      command.maxTimeMS.foreach { maxTimeMs =>
        elements += element("maxTimeMS", long(maxTimeMs))
      }

      command.collation.foreach { c =>
        elements += element(
          "collation",
          Collation.serializeWith(pack, c)(builder)
        )
      }

      command.comment.foreach { comment =>
        elements += element("comment", string(comment))
      }

      document(elements.result())
    }
  }

  private def resultReader: pack.Reader[Long] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandExceptionsReaderOpt(pack) {
      decoder.long(_, "n")
    }
  }
}
