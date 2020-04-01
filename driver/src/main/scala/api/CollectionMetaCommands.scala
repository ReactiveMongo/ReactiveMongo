package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.commands._

import reactivemongo.api.indexes.CollectionIndexesManager

/**
 * A mixin that provides commands about this Collection itself.
 *
 * @define createDescription Creates this collection
 * @define cappedSizeParam the size of the collection (number of bytes)
 * @define cappedMaxParam the maximum number of documents this capped collection can contain
 */
private[api] trait CollectionMetaCommands { self: Collection =>
  private implicit lazy val unitBoxReader =
    CommandCodecs.unitBoxReader(command.pack)

  private implicit lazy val createWriter = CreateCollection.writer(command.pack)

  /**
   * $createDescription.
   *
   * The returned future will be completed,
   * with an error if this collection already exists.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.collections.GenericCollection
   * import reactivemongo.api.commands.CommandError
   *
   * def createColl(
   *   coll: GenericCollection[_])(implicit ec: ExecutionContext) =
   *   coll.create().recover {
   *     case CommandError.Code(48) => // NamespaceExists
   *       println(s"Collection \\${coll} already exists")
   *   }
   * }}}
   */
  def create()(implicit ec: ExecutionContext): Future[Unit] =
    command.unboxed(self, Create(None, false), ReadPreference.primary)

  /**
   * $createDescription.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.collections.GenericCollection
   *
   * def createIfNotExists(coll: GenericCollection[_])(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   coll.create(failsIfExists = true)
   * }}}
   *
   * @param failsIfExists if true fails if the collection already exists (default: false)
   */
  def create(failsIfExists: Boolean = false)(implicit ec: ExecutionContext): Future[Unit] = create().recover {
    case CommandError.Code(48 /* already exists */ ) if !failsIfExists => ()

    case CommandError.Message(
      "collection already exists") if !failsIfExists => ()

  }

  /**
   * $createDescription as a capped one.
   *
   * The returned future will be completed with an error if this collection already exists.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * def capped(coll: reactivemongo.api.bson.collection.BSONCollection)(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   coll.createCapped(size = 10, maxDocuments = Some(100))
   * }}}
   *
   * @param size $cappedSizeParam
   * @param maxDocuments $cappedMaxParam
   * @param autoIndexId $autoIndexIdParam
   *
   * @see [[convertToCapped]]
   */
  def createCapped(
    size: Long,
    maxDocuments: Option[Int],
    autoIndexId: Boolean = false)(implicit ec: ExecutionContext): Future[Unit] =
    command.unboxed(
      self,
      Create(Some(new Capped(size, maxDocuments)), autoIndexId),
      ReadPreference.primary)

  /**
   * Drops this collection.
   *
   * The returned future will be completed with an error
   * if this collection does not exist.
   */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    drop(false).map(_ => {})

  private implicit lazy val dropWriter = DropCollection.writer(command.pack)

  /**
   * Drops this collection.
   *
   * If the collection existed and is successfully dropped,
   * the returned future will be completed with true.
   *
   * If `failIfNotFound` is false and the collection doesn't exist,
   * the returned future will be completed with false.
   *
   * Otherwise in case, the future will be completed with the encountered error.
   *
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.collections.GenericCollection
   *
   * def dropIfNotFound(coll: GenericCollection[_])(
   *   implicit ec: ExecutionContext): Future[Boolean] =
   *   coll.drop(failIfNotFound = true)
   * }}}
   *
   * @param failIfNotFound the flag to request whether it should fail
   */
  def drop(failIfNotFound: Boolean)(implicit ec: ExecutionContext): Future[Boolean] = {
    command(self, DropCollection, ReadPreference.primary).
      map(_ => true).recoverWith {
        case CommandError.Code(26) if !failIfNotFound =>
          Future.successful(false)

      }
  }

  private implicit lazy val convertWriter = ConvertToCapped.writer(command.pack)

  /**
   * Converts this collection to a [[https://docs.mongodb.com/manual/core/capped-collections/ capped one]].
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * def capColl(coll: reactivemongo.api.bson.collection.BSONCollection)(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   coll.convertToCapped(size = 10L, maxDocuments = Some(100))
   * }}}
   *
   * @param size $cappedSizeParam
   * @param maxDocuments $cappedMaxParam
   */
  def convertToCapped(size: Long, maxDocuments: Option[Int])(implicit ec: ExecutionContext): Future[Unit] = command.unboxed(self, new ConvertToCapped(new Capped(size, maxDocuments)), ReadPreference.primary)

  private implicit lazy val statsWriter = CollStats.writer(command.pack)

  private implicit lazy val statsReader = CollStats.reader(command.pack)

  /**
   * Returns various information about this collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * def isCapped(coll: reactivemongo.api.bson.collection.BSONCollection)(
   *   implicit ec: ExecutionContext): Future[Boolean] =
   *   coll.stats().map(_.capped)
   * }}}
   */
  def stats()(implicit ec: ExecutionContext): Future[CollectionStats] =
    command(self, new CollStats(None), ReadPreference.primary)

  /**
   * Returns various information about this collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * def getSize(coll: reactivemongo.api.bson.collection.BSONCollection)(
   *   implicit ec: ExecutionContext): Future[Double] =
   *   coll.stats(scale = 1024).map(_.size)
   * }}}
   *
   * @param scale the scale factor (for example, to get all the sizes in kilobytes)
   */
  def stats(scale: Int)(implicit ec: ExecutionContext): Future[CollectionStats] = command(self, new CollStats(Some(scale)), ReadPreference.primary)

  /**
   * Returns an index manager for this collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.collections.GenericCollection
   *
   * def listIndexes(coll: GenericCollection[_])(
   *   implicit ec: ExecutionContext): Future[List[String]] =
   *   coll.indexesManager.list().map(_.flatMap { idx =>
   *     idx.name.toList
   *   })
   * }}}
   */
  def indexesManager(implicit ec: ExecutionContext): CollectionIndexesManager.Aux[Serialization.Pack] = CollectionIndexesManager(self.db, name)

  // Command runner
  private lazy val command =
    Command.run(Serialization.internalSerializationPack, failoverStrategy)
}
