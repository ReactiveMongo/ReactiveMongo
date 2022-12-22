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
 * @define writeConcernParam the write concern for collection operations
 */
private[api] trait CollectionMetaCommands { self: Collection =>
  import command.pack

  private implicit lazy val unitBoxReader: pack.Reader[Unit] =
    CommandCodecs.unitReader(pack)

  private implicit lazy val createWriter: pack.Writer[ResolvedCollectionCommand[Create]] =
    CreateCollection.writer(pack)

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
   * import reactivemongo.api.commands.CommandException
   *
   * def createColl(
   *   coll: GenericCollection[_])(implicit ec: ExecutionContext) =
   *   coll.create().recover {
   *     case CommandException.Code(48) => // NamespaceExists
   *       println(s"Collection \\${coll} already exists")
   *   }
   * }}}
   */
  final def create()(implicit ec: ExecutionContext): Future[Unit] =
    command(self, Create(None), ReadPreference.primary)

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
   * @param writeConcern $writeConcernParam
   */
  final def create(
      failsIfExists: Boolean = false,
      writeConcern: WriteConcern = db.defaultWriteConcern
    )(implicit
      ec: ExecutionContext
    ): Future[Unit] = command(
    self,
    Create(capped = None, writeConcern = writeConcern),
    ReadPreference.primary
  ).recover {
    case CommandException.Code(48 /* already exists */ ) if !failsIfExists => ()

    case CommandException.Message("collection already exists")
        if !failsIfExists =>
      ()

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
   *
   * @see [[convertToCapped]]
   */
  final def createCapped(
      size: Long,
      maxDocuments: Option[Int]
    )(implicit
      ec: ExecutionContext
    ): Future[Unit] =
    command(
      self,
      Create(Some(new Capped(size, maxDocuments))),
      ReadPreference.primary
    )

  /**
   * Drops this collection.
   *
   * The returned future will be completed with an error
   * if this collection does not exist.
   */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    drop(false).map(_ => {})

  private implicit lazy val dropWriter: pack.Writer[ResolvedCollectionCommand[DropCollection.type]] =
    DropCollection.writer(command.pack)

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
  final def drop(
      failIfNotFound: Boolean
    )(implicit
      ec: ExecutionContext
    ): Future[Boolean] = {
    command(self, DropCollection, ReadPreference.primary)
      .map(_ => true)
      .recoverWith {
        case CommandException.Code(26) if !failIfNotFound =>
          Future.successful(false)

      }
  }

  private implicit lazy val convertWriter: pack.Writer[ResolvedCollectionCommand[ConvertToCapped]] =
    ConvertToCapped.writer(command.pack)

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
  final def convertToCapped(
      size: Long,
      maxDocuments: Option[Int]
    )(implicit
      ec: ExecutionContext
    ): Future[Unit] = command(
    self,
    new ConvertToCapped(new Capped(size, maxDocuments)),
    ReadPreference.primary
  )

  private implicit lazy val statsWriter: pack.Writer[ResolvedCollectionCommand[CollStats]] =
    CollStats.writer(command.pack)

  private implicit lazy val statsReader: pack.Reader[CollectionStats] =
    CollStats.reader(command.pack)

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
  final def stats()(implicit ec: ExecutionContext): Future[CollectionStats] =
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
  final def stats(
      scale: Int
    )(implicit
      ec: ExecutionContext
    ): Future[CollectionStats] =
    command(self, new CollStats(Some(scale)), ReadPreference.primary)

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
  final def indexesManager(
      implicit
      ec: ExecutionContext
    ): CollectionIndexesManager.Aux[Serialization.Pack] =
    CollectionIndexesManager(self.db, name)

  // Command runner
  private lazy val command =
    Command.run(Serialization.internalSerializationPack, failoverStrategy)
}
