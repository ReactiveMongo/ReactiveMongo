package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{ ReadConcern, SerializationPack }

import reactivemongo.api.commands.{
  CollectionCommand,
  CommandCodecs,
  CommandWithResult,
  ResolvedCollectionCommand
}

/**
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 */
private[api] trait CountOp[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] =>

  implicit private lazy val countWriter: pack.Writer[CountCmd] = commandWriter
  implicit private lazy val countReader: pack.Reader[Long] = resultReader

  protected def countDocuments(
    query: Option[pack.Document],
    limit: Option[Int],
    skip: Int,
    hint: Option[Hint[pack.type]],
    readConcern: Option[ReadConcern])(
    implicit
    ec: ExecutionContext): Future[Long] = {

    runCommand(
      new CountCommand(query, limit, skip, hint, readConcern),
      collection.readPreference)
  }

  private final class CountCommand(
    val query: Option[pack.Document],
    val limit: Option[Int],
    val skip: Int,
    val hint: Option[Hint[pack.type]],
    val readConcern: Option[ReadConcern])
    extends CollectionCommand with CommandWithResult[Long]

  private type CountCmd = ResolvedCollectionCommand[CountCommand]

  protected lazy val version =
    collection.db.connectionState.metadata.maxWireVersion

  // TODO: Unit test
  private def commandWriter: pack.Writer[CountCmd] = {
    val builder = pack.newBuilder
    val writeReadConcern = CommandCodecs.writeReadConcern(builder)

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

      count.command.readConcern.foreach { rc =>
        elements += element("readConcern", writeReadConcern(rc))
      }

      if (version.compareTo(MongoWireVersion.V36) >= 0) {
        collection.db.session.foreach { session =>
          CommandCodecs.writeSession(builder)(elements)(session)
        }
      }

      document(elements.result())
    }
  }

  private def resultReader: pack.Reader[Long] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandErrorsReader(pack) {
      decoder.long(_, "n").get
    }
  }
}
