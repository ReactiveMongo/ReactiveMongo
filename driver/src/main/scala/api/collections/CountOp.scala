package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{ ReadConcern, ReadPreference, SerializationPack }

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
    readPreference: ReadPreference)(
    implicit
    ec: ExecutionContext): Future[Long] = runCommand(
    new CountCommand(query, limit, skip, hint, readConcern),
    readPreference)

  private final class CountCommand(
    val query: Option[pack.Document],
    val limit: Option[Int],
    val skip: Int,
    val hint: Option[Hint],
    val readConcern: ReadConcern)
    extends CollectionCommand with CommandWithResult[Long] {
    val commandKind = CommandKind.Count
  }

  private type CountCmd = ResolvedCollectionCommand[CountCommand]

  private def commandWriter: pack.Writer[CountCmd] = {
    val builder = pack.newBuilder
    val session = collection.db.session.filter(
      _ => (version.compareTo(MongoWireVersion.V36) >= 0))

    val writeReadConcern =
      CommandCodecs.writeSessionReadConcern(builder)(session)

    pack.writer[CountCmd] { count =>
      import builder.{
        document,
        elementProducer => element,
        int,
        string
      }

      val elements = Seq.newBuilder[pack.ElementProducer]

      elements += element("count", string(count.collection))
      elements += element("skip", int(count.command.skip))

      count.command.query.foreach { query =>
        elements += element("query", query)
      }

      count.command.limit.foreach { limit =>
        elements += element("limit", int(limit))
      }

      count.command.hint.foreach {
        case HintString(name) =>
          elements += element("hint", string(name))

        case HintDocument(spec) =>
          elements += element("hint", spec)

        case hint => println(s"Unsupported count hint: $hint") // should never
      }

      elements ++= writeReadConcern(count.command.readConcern)

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
