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
 * Basic information resolved for a MongoDB Collection, resolved from a [[reactivemongo.api.DefaultDB]].
 *
 * For collection operations, you should consider the generic API
 * ([[reactivemongo.api.collections.GenericCollection]]).
 */
trait Collection {
  import collections.bson.BSONCollectionProducer

  /** The database which this collection belongs to. */
  def db: DB

  /** The name of the collection. */
  def name: String

  /** The default failover strategy for the methods of this collection. */
  private[reactivemongo] def failoverStrategy: FailoverStrategy

  /** Gets the full qualified name of this collection. */
  @inline private[reactivemongo] final def fullCollectionName =
    db.name + "." + name

}

/**
 * A Producer of [[Collection]] implementation.
 *
 * This is used to get an implementation implicitly when getting a reference of a [[Collection]].
 */
trait CollectionProducer[+C <: Collection] {
  /**
   * Resolve a [[Collection]] reference.
   *
   * @param db the database which this collection belong to
   * @param name the name of the collection
   * @param failoverStrategy the failover strategy for the collection operations
   */
  def apply(db: DB, name: String, failoverStrategy: FailoverStrategy = FailoverStrategy()): C
}
