package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.errors.GenericDatabaseException

import reactivemongo.api.commands._
import reactivemongo.api.commands.bson._

import reactivemongo.api.indexes.CollectionIndexesManager

/**
 * A mixin that provides commands about this Collection itself.
 *
 * @define autoIndexIdParam If true should automatically add an index on the `_id` field. By default, regular collections will have an indexed `_id` field, in contrast to capped collections. This MongoDB option is deprecated and will be removed in a future release.
 * @define cappedSizeParam the size of the collection (number of bytes)
 * @define cappedMaxParam the maximum number of documents this capped collection can contain
 */
trait CollectionMetaCommands { self: Collection =>
  import CommonImplicits._
  import BSONCreateImplicits._
  import BSONEmptyCappedImplicits._
  import BSONCollStatsImplicits._
  import BSONRenameCollectionImplicits._
  import BSONConvertToCappedImplicits._

  /**
   * Creates this collection.
   *
   * The returned future will be completed with an error if
   * this collection already exists.
   *
   * {{{
   * coll.create().recover {
   *   case CommandError.Code(48 /*NamespaceExists*/ ) =>
   *     println(s"Collection \${coll.fullCollectionName} already exists")
   * }
   * }}}
   */
  def create()(implicit ec: ExecutionContext): Future[Unit] =
    command.unboxed(self, Create(None, false), ReadPreference.primary)

  /**
   * @param autoIndexId DEPRECATED: $autoIndexIdParam
   */
  @deprecated("Use `create` without deprecated `autoIndexId`", "0.14.0")
  def create(autoIndexId: Boolean = false)(implicit ec: ExecutionContext): Future[Unit] = command.unboxed(self, Create(None, autoIndexId), ReadPreference.primary)

  /**
   * Creates this collection as a capped one.
   *
   * The returned future will be completed with an error if this collection already exists.
   *
   * @param size $cappedSizeParam
   * @param maxDocuments $cappedMaxParam
   * @param autoIndexId $autoIndexIdParam
   */
  def createCapped(size: Long, maxDocuments: Option[Int], autoIndexId: Boolean = false)(implicit ec: ExecutionContext): Future[Unit] =
    command.unboxed(
      self,
      Create(Some(Capped(size, maxDocuments)), autoIndexId),
      ReadPreference.primary)

  /**
   * Drops this collection.
   *
   * The returned future will be completed with an error
   * if this collection does not exist.
   */
  @deprecated("Use `drop(Boolean)`", "0.12.0")
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    drop(true).map(_ => {})

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
   */
  def drop(failIfNotFound: Boolean)(implicit ec: ExecutionContext): Future[Boolean] = {
    import BSONDropCollectionImplicits._

    command(self, DropCollection, ReadPreference.primary).flatMap {
      case DropCollectionResult(false) if failIfNotFound =>
        Future.failed[Boolean](GenericDatabaseException(
          s"fails to drop collection: $name", Some(26)))

      case DropCollectionResult(dropped) =>
        Future.successful(dropped)
    }
  }

  /**
   * If this collection is capped, removes all the documents it contains.
   *
   * Deprecated because it became an internal command, unavailable by default.
   */
  @deprecated("Deprecated because emptyCapped became an internal command, unavailable by default.", "0.9")
  def emptyCapped()(implicit ec: ExecutionContext): Future[Unit] =
    command.unboxed(self, EmptyCapped, ReadPreference.primary)

  /**
   * Converts this collection to a capped one.
   *
   * @param size $cappedSizeParam
   * @param maxDocuments $cappedMaxParam
   */
  def convertToCapped(size: Long, maxDocuments: Option[Int])(implicit ec: ExecutionContext): Future[Unit] = command.unboxed(self, ConvertToCapped(Capped(size, maxDocuments)), ReadPreference.primary)

  /**
   * Renames this collection.
   *
   * @param to the new name of this collection
   * @param dropExisting if a collection of name `to` already exists, then drops that collection before renaming this one
   *
   * @return a failure if the dropExisting option is false and the target collection already exists
   */
  @deprecated(message = "Use `reactivemongo.api.DBMetaCommands.renameCollection on the admin database instead.", since = "0.12.4")
  def rename(to: String, dropExisting: Boolean = false)(implicit ec: ExecutionContext): Future[Unit] = command.unboxed(self.db, RenameCollection(db.name + "." + name, db.name + "." + to, dropExisting), ReadPreference.primary)

  /**
   * Returns various information about this collection.
   */
  def stats()(implicit ec: ExecutionContext): Future[CollStatsResult] =
    command(self, CollStats(None), ReadPreference.primary)

  /**
   * Returns various information about this collection.
   *
   * @param scale the scale factor (for example, to get all the sizes in kilobytes)
   */
  def stats(scale: Int)(implicit ec: ExecutionContext): Future[CollStatsResult] = command(self, CollStats(Some(scale)), ReadPreference.primary)

  /** Returns an index manager for this collection. */
  def indexesManager(implicit ec: ExecutionContext): CollectionIndexesManager =
    CollectionIndexesManager(self.db, name)

  // Command runner
  private lazy val command =
    Command.run(BSONSerializationPack, failoverStrategy)
}
