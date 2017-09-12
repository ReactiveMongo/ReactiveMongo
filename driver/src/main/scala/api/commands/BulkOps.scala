package reactivemongo.api.commands

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.SerializationPack

/** Internal bulk operations for the given `pack`. */
trait BulkOps[P <: SerializationPack with Singleton] {
  val pack: P

  /**
   * Applies the given function `f` on all the bulks,
   * and collects the results, in an `ordered` way or not.
   *
   * @param recoverOrdered the function to recover some failure while applying `f` on some bulk, if and only if the [[https://docs.mongodb.com/manual/reference/method/db.collection.insert/#perform-an-unordered-insert ordered]] behaviour is selected
   */
  private[reactivemongo] def bulkApply[T](producer: BulkProducer)(f: Iterable[pack.Document] => Future[T], recoverUnordered: Option[Exception => Future[T]] = Option.empty)(implicit ec: ExecutionContext): Future[Seq[T]] =
    recoverUnordered match {
      case Some(recover) => unorderedApply[T](producer, Seq.empty)(f, recover)
      case _             => orderedApply[T](producer, Seq.empty)(f)
    }

  /**
   * Returns an [[Iterator]] over `documents` grouped in bulks,
   * according the specified `maxBsonSize` and `maxBulkSize`.
   *
   * @param documents the documents to be grouped in bulks
   * @param maxBsonSize see [[reactivemongo.core.nodeset.ProtocolMetadata.maxBsonSize]]
   * @param maxBulkSize see [[reactivemongo.core.nodeset.ProtocolMetadata.maxBulkSize]]
   */
  private[reactivemongo] def bulks(
    documents: Iterable[pack.Document],
    maxBsonSize: Int,
    maxBulkSize: Int): BulkProducer =
    new BulkProducer(0, documents, maxBsonSize, maxBulkSize)

  /** Bulk stage */
  case class BulkStage(
    bulk: Iterable[pack.Document],
    next: Option[BulkProducer])

  /** Bulk producer */
  final class BulkProducer(
    offset: Int,
    documents: Iterable[pack.Document],
    maxBsonSize: Int,
    maxBulkSize: Int) extends (() => Either[String, BulkStage]) {

    /**
     * Returns either an error message, or the next stage.
     */
    def apply(): Either[String, BulkStage] = go(documents, 0, 0, Seq.empty)

    @annotation.tailrec
    def go(
      input: Iterable[pack.Document],
      docs: Int,
      bsonSize: Int,
      bulk: Seq[pack.Document]): Either[String, BulkStage] =
      input.headOption match {
        case Some(doc) => {
          val bsz = pack.bsonSize(doc)

          if (bsz > maxBsonSize) {
            Left(s"size of document #${offset + docs} exceed the maxBsonSize: $bsz > $maxBsonSize")
          } else {
            val nc = docs + 1
            val nsz = bsonSize + bsz

            if (nsz > maxBsonSize) {
              Right(BulkStage(
                bulk = bulk.reverse,
                next = Some(new BulkProducer(
                  offset + nc, input, maxBsonSize, maxBulkSize))))

            } else if (nc == maxBulkSize || nsz == maxBsonSize) {
              Right(BulkStage(
                bulk = (doc +: bulk).reverse,
                next = Some(new BulkProducer(
                  offset + nc, input.tail, maxBsonSize, maxBulkSize))))

            } else {
              go(input.tail, nc, nsz, doc +: bulk)
            }
          }
        }

        case _ => Right(BulkStage(bulk.reverse, None))
      }

    override val toString = s"BulkProducer(offset = $offset)"
  }

  // ---

  private def unorderedApply[T](current: BulkProducer, tasks: Seq[Future[T]])(f: Iterable[pack.Document] => Future[T], recover: Exception => Future[T])(implicit ec: ExecutionContext): Future[Seq[T]] = current() match {
    case Left(cause) => Future.failed[Seq[T]](GenericDriverException(cause))

    case Right(BulkStage(bulk, _)) if bulk.isEmpty =>
      Future.sequence(tasks.reverse)

    case Right(BulkStage(bulk, Some(next))) =>
      unorderedApply(next, f(bulk).recoverWith({
        case cause: Exception => recover(cause)
      }) +: tasks)(f, recover)

    case Right(BulkStage(bulk, _)) =>
      Future.sequence((f(bulk).recoverWith({
        case cause: Exception => recover(cause)
      }) +: tasks).reverse)
  }

  private def orderedApply[T](current: BulkProducer, values: Seq[T])(f: Iterable[pack.Document] => Future[T])(implicit ec: ExecutionContext): Future[Seq[T]] =
    current() match {
      case Left(cause) => Future.failed[Seq[T]](GenericDriverException(cause))

      case Right(BulkStage(bulk, _)) if bulk.isEmpty =>
        Future.successful(values.reverse)

      case Right(BulkStage(bulk, Some(next))) =>
        f(bulk).flatMap { v => orderedApply(next, v +: values)(f) }

      case Right(BulkStage(bulk, _)) =>
        f(bulk).map { v => (v +: values).reverse }
    }

  private lazy val logger = reactivemongo.util.LazyLogger(
    s"reactivemongo.api.commands.BulkOps[${pack}]")

}
