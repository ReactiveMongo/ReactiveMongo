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

/**
 * A MongoDB Collection. You should consider the default implementation, [[reactivemongo.api.collections.default.BSONCollection]].
 *
 * Example using the default implementation (BSONCollection):
 *
 * {{{
 * object Samples {
 *
 *   val connection = MongoConnection(List("localhost"))
 *
 *   // Gets a reference to the database "plugin"
 *   val db = connection("plugin")
 *
 *   // Gets a reference to the collection "acoll"
 *   // By default, you get a BSONCollection.
 *   val collection = db("acoll")
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
 *     cursor.enumerate.apply(Iteratee.foreach { doc =>
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
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.default.BSONCollection]]).
   *
   * @param failoverStrategy Overrides the default strategy.
   */
  def as[C <: Collection](failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.default.BSONCollectionProducer): C = {
    producer.apply(db, name, failoverStrategy)
  }

  /**
   * Gets another collection in the current database.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.default.BSONCollection]]).
   *
   * @param name The other collection name.
   * @param failoverStrategy Overrides the default strategy.
   */
  @deprecated("Consider using `sibling` instead", "0.10")
  def sister[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.default.BSONCollectionProducer): C =
    sibling(name, failoverStrategy)

  /**
   * Gets another collection in the current database.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.default.BSONCollection]]).
   *
   * @param name The other collection name.
   * @param failoverStrategy Overrides the default strategy.
   */
  def sibling[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.default.BSONCollectionProducer): C =
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

/** A mixin that provides commands about this Collection itself. */
trait CollectionMetaCommands {
  self: Collection =>

  import concurrent.{ ExecutionContext, Future }
  import reactivemongo.core.commands._
  import reactivemongo.api.indexes.{ CollectionIndexesManager, IndexesManager }

  /**
   * Creates this collection.
   *
   * The returned future will be completed with an error if this collection already exists.
   *
   * @param autoIndexId States if should automatically add an index on the _id field. By default, regular collections will have an indexed _id field, in contrast to capped collections.
   */
  def create(autoIndexId: Boolean = true)(implicit ec: ExecutionContext): Future[Boolean] = db.command(new CreateCollection(name, None, if (autoIndexId) None else Some(false)))

  /**
   * Creates this collection as a capped one.
   *
   * The returned future will be completed with an error if this collection already exists.
   *
   * @param autoIndexId States if should automatically add an index on the _id field. By default, capped collections will NOT have an indexed _id field, in contrast to regular collections.
   */
  def createCapped(size: Long, maxDocuments: Option[Int], autoIndexId: Boolean = false)(implicit ec: ExecutionContext): Future[Boolean] = db.command(new CreateCollection(name, Some(CappedOptions(size, maxDocuments)), if (autoIndexId) Some(true) else None))

  /**
   * Drops this collection.
   *
   * The returned future will be completed with an error if this collection does not exist.
   */
  def drop()(implicit ec: ExecutionContext): Future[Boolean] = db.command(new Drop(name))

  /**
   * If this collection is capped, removes all the documents it contains.
   *
   * Deprecated because it became an internal command, unavailable by default.
   */
  @deprecated("Deprecated because emptyCapped became an internal command, unavailable by default.", "0.9")
  def emptyCapped()(implicit ec: ExecutionContext): Future[Boolean] = db.command(new EmptyCapped(name))

  /**
   * Converts this collection to a capped one.
   *
   * @param size The size of this capped collection, in bytes.
   * @param maxDocuments The maximum number of documents this capped collection can contain.
   */
  def convertToCapped(size: Long, maxDocuments: Option[Int])(implicit ec: ExecutionContext): Future[Boolean] = db.command(new ConvertToCapped(name, CappedOptions(size, maxDocuments)))

  /**
   * Renames this collection.
   *
   * @param to The new name of this collection.
   * @param dropExisting If a collection of name `to` already exists, then drops that collection before renaming this one.
   */
  def rename(to: String, dropExisting: Boolean = false)(implicit ec: ExecutionContext): Future[Boolean] = db.command(new RenameCollection(db.name + "." + name, db.name + "." + to, dropExisting))

  /**
   * Returns various information about this collection.
   */
  def stats()(implicit ec: ExecutionContext): Future[CollStatsResult] = db.command(new CollStats(name))

  /**
   * Returns various information about this collection.
   *
   * @param scale A scale factor (for example, to get all the sizes in kilobytes).
   */
  def stats(scale: Int)(implicit ec: ExecutionContext): Future[CollStatsResult] = db.command(new CollStats(name, Some(scale)))

  /** Returns an index manager for this collection. */
  def indexesManager(implicit ec: ExecutionContext): CollectionIndexesManager = new IndexesManager(self.db).onCollection(name)
}
