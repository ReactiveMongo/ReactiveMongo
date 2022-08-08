package reactivemongo.api

/**
 * [[https://docs.mongodb.com/manual/changeStreams Change stream]] utilities.
 *
 * {{{
 * import scala.concurrent.{ ExecutionContext, Future }
 *
 * import reactivemongo.api.ChangeStreams.FullDocumentStrategy
 *
 * import reactivemongo.api.bson.BSONDocument
 * import reactivemongo.api.bson.collection.BSONCollection
 *
 * def peekNext(
 *   coll: BSONCollection,
 *   strategy: FullDocumentStrategy)(
 *   implicit ec: ExecutionContext): Future[Option[BSONDocument]] =
 *   coll.watch[BSONDocument](fullDocumentStrategy = Some(strategy)).
 *     cursor.headOption
 *
 * def doIt(coll: BSONCollection)(
 *   implicit ec: ExecutionContext): Future[Unit] = for {
 *     _ <- peekNext(coll, FullDocumentStrategy.Default)
 *     _ <- peekNext(coll, FullDocumentStrategy.UpdateLookup)
 *   } yield ()
 * }}}
 *
 * @see [[reactivemongo.api.collections.GenericCollection.watch]]
 */
object ChangeStreams {

  /**
   * Defines the lookup strategy of a change stream.
   */
  sealed abstract class FullDocumentStrategy(val name: String) {

    final override def equals(that: Any): Boolean = that match {
      case other: FullDocumentStrategy => this.name == other.name
      case _                           => false
    }

    final override def hashCode: Int = name.hashCode

    override def toString = s"FullDocumentStrategy($name)"
  }

  /** [[FullDocumentStrategy]] utilities */
  object FullDocumentStrategy {

    /**
     * Default lookup strategy. Insert and Replace events contain
     * the full document at the time of the event.
     */
    case object Default extends FullDocumentStrategy("default")

    /**
     * In this strategy, in addition to the default behavior,
     * Update change events will be joined with the *current*
     * version of the related document (which is thus not necessarily
     * the value at the time of the event).
     */
    case object UpdateLookup extends FullDocumentStrategy("updateLookup")
  }
}
