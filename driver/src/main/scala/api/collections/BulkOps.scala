package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.errors.GenericDriverException

/** Internal bulk operations */
private[reactivemongo] object BulkOps {

  /**
   * Applies the given function `f` on all the bulks,
   * and collects the results, in an `ordered` way or not.
   *
   * @param recoverOrdered the function to recover some failure while applying `f` on some bulk, if and only if the [[https://docs.mongodb.com/manual/reference/method/db.collection.insert/#perform-an-unordered-insert ordered]] behaviour is selected
   */
  def bulkApply[I, O](producer: BulkProducer[I])(f: Iterable[I] => Future[O], recoverUnordered: Option[Exception => Future[O]] = Option.empty)(implicit ec: ExecutionContext): Future[Seq[O]] =
    recoverUnordered match {
      case Some(recover) =>
        unorderedApply[I, O](producer, Seq.empty)(f, recover)

      case _ =>
        orderedApply[I, O](producer, Seq.empty)(f)
    }

  /**
   * Returns an [[Iterator]] over `documents` grouped in bulks,
   * according the specified `maxBsonSize` and `maxBulkSize`.
   *
   * @param documents the documents to be grouped in bulks
   * @param maxBsonSize see [[reactivemongo.core.nodeset.ProtocolMetadata.maxBsonSize]]
   * @param maxBulkSize see [[reactivemongo.core.nodeset.ProtocolMetadata.maxBulkSize]]
   * @param sz the function used to determine the BSON size of each document
   * @tparam I the input (document) type
   */
  def bulks[I](
    documents: Iterable[I],
    maxBsonSize: Int,
    maxBulkSize: Int)(sz: I => Int): BulkProducer[I] =
    new BulkProducer[I](0, documents, sz, maxBsonSize, maxBulkSize)

  /** Bulk stage */
  case class BulkStage[I](
    bulk: Iterable[I],
    next: Option[BulkProducer[I]])

  /** Bulk producer */
  final class BulkProducer[I](
    offset: Int,
    documents: Iterable[I],
    sz: I => Int,
    maxBsonSize: Int,
    maxBulkSize: Int) extends (() => Either[String, BulkStage[I]]) {

    /**
     * Returns either an error message, or the next stage.
     */
    def apply(): Either[String, BulkStage[I]] = go(documents, 0, 0, Seq.empty)

    @annotation.tailrec
    def go(
      input: Iterable[I],
      docs: Int,
      bsonSize: Int,
      bulk: Seq[I]): Either[String, BulkStage[I]] = input.headOption match {
      case Some(doc) => {
        val bsz = sz(doc)

        // Total minimal size is key '1' size (1 byte) + type prefix (2 bytes)
        if (bsz + 1 + 2 > maxBsonSize) {
          Left(s"size of document #${offset + docs} exceed the maxBsonSize: $bsz > $maxBsonSize")
        } else {
          val nc = docs + 1
          val nsz = bsonSize + bsz

          if (nsz > maxBsonSize) {
            Right(BulkStage[I](
              bulk = bulk.reverse,
              next = Some(new BulkProducer[I](
                offset + nc, input, sz, maxBsonSize, maxBulkSize))))

          } else if (nc == maxBulkSize || nsz == maxBsonSize) {
            Right(BulkStage[I](
              bulk = (doc +: bulk).reverse,
              next = Some(new BulkProducer[I](
                offset + nc, input.tail, sz, maxBsonSize, maxBulkSize))))

          } else {
            go(input.tail, nc, nsz, doc +: bulk)
          }
        }
      }

      case _ => Right(BulkStage[I](bulk.reverse, None))
    }

    override val toString = s"BulkProducer(offset = $offset)"
  }

  // ---

  private def unorderedApply[I, O](current: BulkProducer[I], tasks: Seq[Future[O]])(f: Iterable[I] => Future[O], recover: Exception => Future[O])(implicit ec: ExecutionContext): Future[Seq[O]] = current() match {
    case Left(cause) => Future.failed[Seq[O]](GenericDriverException(cause))

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

  private def orderedApply[I, O](current: BulkProducer[I], values: Seq[O])(f: Iterable[I] => Future[O])(implicit ec: ExecutionContext): Future[Seq[O]] =
    current() match {
      case Left(cause) => Future.failed[Seq[O]](GenericDriverException(cause))

      case Right(BulkStage(bulk, _)) if bulk.isEmpty =>
        Future.successful(values.reverse)

      case Right(BulkStage(bulk, Some(next))) =>
        f(bulk).flatMap { v => orderedApply(next, v +: values)(f) }

      case Right(BulkStage(bulk, _)) =>
        f(bulk).map { v => (v +: values).reverse }
    }
}
