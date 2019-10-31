package reactivemongo.api.collections

import scala.language.higherKinds

import scala.util.{ Failure, Success, Try }

import scala.collection.mutable.Builder

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{ ReadConcern, SerializationPack }

import reactivemongo.api.commands.{
  Collation,
  CollectionCommand,
  CommandCodecs,
  CommandWithResult,
  CommandWithPack,
  ResolvedCollectionCommand
}

private[api] trait DistinctOp[P <: SerializationPack with Singleton] extends DistinctOpCompat[P] {
  collection: GenericCollection[P] =>

  implicit private lazy val distinctWriter: pack.Writer[DistinctCmd] = commandWriter

  implicit private lazy val distinctReader: pack.Reader[DistinctResult] = resultReader

  private type DistinctCmd = ResolvedCollectionCommand[Distinct]

  protected def distinctDocuments[T, M[_] <: Iterable[_]](
    key: String,
    query: Option[pack.Document],
    readConcern: ReadConcern,
    collation: Option[Collation],
    builder: Builder[T, M[T]])(implicit
    reader: pack.NarrowValueReader[T],
    ec: ExecutionContext): Future[M[T]] = {

    val widenReader = pack.widenReader(reader)
    val cmd = Distinct(key, query, readConcern, collation)

    runCommand(cmd, readPreference).flatMap {
      _.result[T, M](widenReader, builder) match {
        case Failure(cause)  => Future.failed[M[T]](cause)
        case Success(result) => Future.successful(result)
      }
    }
  }

  // ---

  private case class Distinct(
    key: String,
    query: Option[pack.Document],
    readConcern: ReadConcern,
    collation: Option[Collation]) extends CollectionCommand
    with CommandWithPack[pack.type] with CommandWithResult[DistinctResult]

  /**
   * @param values the raw values (should not contain duplicate)
   */
  protected case class DistinctResult(values: Traversable[pack.Value]) {
    @annotation.tailrec
    protected final def result[T, M[_]](
      values: Traversable[pack.Value],
      reader: pack.WidenValueReader[T],
      out: Builder[T, M[T]]): Try[M[T]] = values.headOption match {
      case Some(t) => pack.readValue(t, reader) match {
        case Failure(e) => Failure(e)
        case Success(v) => result(values.tail, reader, out += v)
      }

      case _ => Success(out.result())
    }

    @inline def result[T, M[_]](
      reader: pack.WidenValueReader[T],
      cbf: Builder[T, M[T]]): Try[M[T]] =
      result(values, reader, cbf)
  }

  private def commandWriter: pack.Writer[DistinctCmd] = {
    val builder = pack.newBuilder
    val session = collection.db.session.filter( // TODO: Remove
      _ => (version.compareTo(MongoWireVersion.V36) >= 0))

    val writeReadConcern =
      CommandCodecs.writeSessionReadConcern(builder)(session)

    import builder.{ document, elementProducer => element, string }

    pack.writer[DistinctCmd] {
      if (version.compareTo(MongoWireVersion.V32) >= 0) {
        { distinct: DistinctCmd =>
          val elements = Seq.newBuilder[pack.ElementProducer]

          elements += element("distinct", string(distinct.collection))
          elements += element("key", string(distinct.command.key))

          distinct.command.query.foreach { query =>
            elements += element("query", query)
          }

          elements ++= writeReadConcern(distinct.command.readConcern)

          document(elements.result())
        }
      } else { distinct: DistinctCmd =>
        val elements = Seq.newBuilder[pack.ElementProducer]

        elements += element("distinct", string(distinct.collection))
        elements += element("key", string(distinct.command.key))

        distinct.command.query.foreach { query =>
          elements += element("query", query)
        }

        document(elements.result())
      }
    }
  }

  private def resultReader: pack.Reader[DistinctResult] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandErrorsReader(pack) { doc =>
      decoder.array(doc, "values").map(DistinctResult(_)).get
    }
  }
}
