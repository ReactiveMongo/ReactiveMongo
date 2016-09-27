/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.api

import reactivemongo.core.errors.GenericDatabaseException

/**
 * A MongoDB Collection, resolved from a [[reactivemongo.api.DefaultDB]].
 *
 * You should consider the generic API
 * ([[reactivemongo.api.collections.GenericCollection]])
 * and the default [[reactivemongo.api.collections.bson.BSONCollection]].
 *
 * {{{
 * import reactivemongo.bson._
 * import reactivemongo.api.collections.bson.BSONCollection
 *
 * object Samples {
 *
 *   val connection = MongoConnection(List("localhost"))
 *
 *   // Gets a reference to the database "plugin"
 *   val db = connection("plugin")
 *
 *   // Gets a reference to the collection "acoll"
 *   // By default, you get a BSONCollection.
 *   val collection = db[BSONCollection]("acoll")
 *
 *   def listDocs() = {
 *     // Select only the documents which field 'firstName' equals 'Jack'
 *     val query = BSONDocument("firstName" -> "Jack")
 *     // select only the field 'lastName'
 *     val filter = BSONDocument(
 *       "lastName" -> 1,
 *       "_id" -> 0)
 *
 *     // Get a cursor of BSONDocuments
 *     val cursor = collection.find(query, filter).cursor[BSONDocument]
 *     // Let's enumerate this cursor and print a readable representation of each document in the response
 *     cursor.enumerate().apply(Iteratee.foreach { doc =>
 *       println("found document: " + BSONDocument.pretty(doc))
 *     })
 *
 *     // Or, the same with getting a list
 *     val cursor2 = collection.find(query, filter).cursor[BSONDocument]
 *     val futureList = cursor.collect[List]()
 *     futureList.map { list =>
 *       list.foreach { doc =>
 *         println("found document: " + BSONDocument.pretty(doc))
 *       }
 *     }
 *   }
 * }
 * }}}
 */
trait Collection {
  /** The database which this collection belong to. */
  def db: DB

  /** The name of the collection. */
  def name: String

  /** The default failover strategy for the methods of this collection. */
  def failoverStrategy: FailoverStrategy

  /** Gets the full qualified name of this collection. */
  def fullCollectionName = db.name + "." + name

  /**
   * Gets another implementation of this collection.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.bson.BSONCollection]]).
   *
   * @param failoverStrategy Overrides the default strategy.
   */
  def as[C <: Collection](failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C = {
    producer.apply(db, name, failoverStrategy)
  }

  /**
   * Gets another collection in the current database.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.bson.BSONCollection]]).
   *
   * @param name The other collection name.
   * @param failoverStrategy Overrides the default strategy.
   */
  @deprecated("Consider using `sibling` instead", "0.10")
  def sister[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C =
    sibling(name, failoverStrategy)

  /**
   * Gets another collection in the current database.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.bson.BSONCollection]]).
   *
   * @param name The other collection name.
   * @param failoverStrategy Overrides the default strategy.
   */
  def sibling[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C =
    producer.apply(db, name, failoverStrategy)
}

/**
 * A Producer of [[Collection]] implementation.
 *
 * This is used to get an implementation implicitly when getting a reference of a [[Collection]].
 */
trait CollectionProducer[+C <: Collection] {
  def apply(db: DB, name: String, failoverStrategy: FailoverStrategy = FailoverStrategy()): C
}

/**
 * A mixin that provides commands about this Collection itself.
 *
 * @define autoIndexIdParam If true should automatically add an index on the `_id` field. By default, regular collections will have an indexed `_id` field, in contrast to capped collections. This MongoDB option is deprecated and will be removed in a future release.
 * @define cappedSizeParam the size of the collection (number of bytes)
 * @define cappedMaxParam the maximum number of documents this capped collection can contain
 */
trait CollectionMetaCommands { self: Collection =>
  import scala.concurrent.{ ExecutionContext, Future }
  import reactivemongo.api.commands._
  import reactivemongo.api.commands.bson._
  import CommonImplicits._
  import BSONCreateImplicits._
  import BSONEmptyCappedImplicits._
  import BSONCollStatsImplicits._
  import BSONRenameCollectionImplicits._
  import BSONConvertToCappedImplicits._
  import reactivemongo.api.indexes.CollectionIndexesManager

  /**
   * Creates this collection.
   *
   * The returned future will be completed with an error if this collection already exists.
   *
   * @param autoIndexId $autoIndexIdParam
   */
  def create(autoIndexId: Boolean = true)(implicit ec: ExecutionContext): Future[Unit] = Command.run(BSONSerializationPack).unboxed(self, Create(None, autoIndexId), ReadPreference.primary)

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
    Command.run(BSONSerializationPack).unboxed(
      self,
      Create(Some(Capped(size, maxDocuments)), autoIndexId),
      ReadPreference.primary
    )

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

    Command.run(BSONSerializationPack)(
      self, DropCollection, ReadPreference.primary
    ).flatMap {
      case DropCollectionResult(false) if failIfNotFound =>
        Future.failed[Boolean](GenericDatabaseException(
          s"fails to drop collection: $name", Some(26)
        ))

      case DropCollectionResult(dropped) => Future.successful(dropped)
    }
  }

  /**
   * If this collection is capped, removes all the documents it contains.
   *
   * Deprecated because it became an internal command, unavailable by default.
   */
  @deprecated("Deprecated because emptyCapped became an internal command, unavailable by default.", "0.9")
  def emptyCapped()(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(BSONSerializationPack).unboxed(
      self, EmptyCapped, ReadPreference.primary
    )

  /**
   * Converts this collection to a capped one.
   *
   * @param size $cappedSizeParam
   * @param maxDocuments $cappedMaxParam
   */
  def convertToCapped(size: Long, maxDocuments: Option[Int])(implicit ec: ExecutionContext): Future[Unit] = Command.run(BSONSerializationPack).unboxed(self, ConvertToCapped(Capped(size, maxDocuments)), ReadPreference.primary)

  /**
   * Renames this collection.
   *
   * @param to The new name of this collection.
   * @param dropExisting If a collection of name `to` already exists, then drops that collection before renaming this one.
   *
   * @return a failure if the dropExisting option is false and the target collection already exists
   */
  def rename(to: String, dropExisting: Boolean = false)(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(BSONSerializationPack).unboxed(self.db, RenameCollection(db.name + "." + name, db.name + "." + to, dropExisting), ReadPreference.primary)

  /**
   * Returns various information about this collection.
   */
  def stats()(implicit ec: ExecutionContext): Future[CollStatsResult] =
    Command.run(BSONSerializationPack)(
      self, CollStats(None), ReadPreference.primary
    )

  /**
   * Returns various information about this collection.
   *
   * @param scale A scale factor (for example, to get all the sizes in kilobytes).
   */
  def stats(scale: Int)(implicit ec: ExecutionContext): Future[CollStatsResult] = Command.run(BSONSerializationPack)(self, CollStats(Some(scale)), ReadPreference.primary)

  /** Returns an index manager for this collection. */
  def indexesManager(implicit ec: ExecutionContext): CollectionIndexesManager =
    CollectionIndexesManager(self.db, name)
}
