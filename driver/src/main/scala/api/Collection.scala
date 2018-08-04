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
 * A MongoDB Collection, resolved from a [[reactivemongo.api.DefaultDB]].
 *
 * You should consider the generic API
 * ([[reactivemongo.api.collections.GenericCollection]])
 * and the default [[reactivemongo.api.collections.bson.BSONCollection]].
 *
 * @define failoverStrategy the failover strategy to override the default one
 * @define otherName the name of another collection
 */
trait Collection {
  import collections.bson.BSONCollectionProducer

  /** The database which this collection belong to. */
  def db: DB

  /** The name of the collection. */
  def name: String

  /** The default failover strategy for the methods of this collection. */
  @deprecated("Will be private", "0.16.0")
  def failoverStrategy: FailoverStrategy

  /** Gets the full qualified name of this collection. */
  @inline final def fullCollectionName = db.name + "." + name

  /**
   * Gets another implementation of this collection.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.bson.BSONCollection]]).
   *
   * @param failoverStrategy $failoverStrategy
   */
  @deprecated("Resolve the collection from DB", "0.13.0")
  def as[C <: Collection](failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = BSONCollectionProducer): C = producer.apply(db, name, failoverStrategy)

  /**
   * Gets another collection in the current database.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.bson.BSONCollection]]).
   *
   * @param name $otherName
   * @param failoverStrategy $failoverStrategy
   */
  def sibling[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = BSONCollectionProducer): C = producer.apply(db, name, failoverStrategy)
}

/**
 * A Producer of [[Collection]] implementation.
 *
 * This is used to get an implementation implicitly when getting a reference of a [[Collection]].
 */
trait CollectionProducer[+C <: Collection] {
  def apply(db: DB, name: String, failoverStrategy: FailoverStrategy = FailoverStrategy()): C
}
