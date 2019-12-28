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
package reactivemongo.api.collections.bson

import reactivemongo.api.{ DB, FailoverStrategy, ReadPreference }
import reactivemongo.api.collections.{
  GenericCollection,
  GenericCollectionProducer
}
import reactivemongo.api.BSONSerializationPack

@deprecated("Will be removed", "0.19.0")
object `package` {
  implicit object BSONCollectionProducer extends GenericCollectionProducer[BSONSerializationPack.type, BSONCollection] {
    val pack = BSONSerializationPack

    def apply(db: DB, name: String, failoverStrategy: FailoverStrategy = FailoverStrategy()): BSONCollection = new BSONCollection(db, name, failoverStrategy, db.defaultReadPreference)
  }
}

@deprecated("Use `reactivemongo.api.bson.collection.BSONCollection`", "0.19.0")
@SerialVersionUID(1382847900L)
final class BSONCollection @deprecated("Internal: will be made private", "0.17.0") (
  val db: DB,
  val name: String,
  val failoverStrategy: FailoverStrategy,
  override val readPreference: ReadPreference) extends Product with GenericCollection[BSONSerializationPack.type]
  with scala.Serializable with java.io.Serializable {

  @transient val pack = BSONSerializationPack
  @transient val BatchCommands = BSONBatchCommands

  override lazy val toString =
    s"BSONCollection('${db.name}'.'$name', $failoverStrategy)"

  @deprecated(message = "No longer a ReactiveMongo case class", since = "0.12-RC2")
  def canEqual(that: Any): Boolean = that match {
    case _: BSONCollection => true
    case _                 => false
  }

  @deprecated(message = "No longer a ReactiveMongo case class", since = "0.12-RC2")
  val productArity = 3

  @deprecated(message = "No longer a ReactiveMongo case class", since = "0.12-RC2")
  def productElement(n: Int) = n match {
    case 0 => db
    case 1 => name
    case _ => failoverStrategy
  }

  @deprecated(message = "No longer a ReactiveMongo case class", since = "0.12-RC2")
  def copy(
    db: DB = this.db,
    name: String = this.name,
    failoverStrategy: FailoverStrategy = this.failoverStrategy): BSONCollection = new BSONCollection(
    db, name, failoverStrategy, readPreference)

  def withReadPreference(pref: ReadPreference): BSONCollection =
    new BSONCollection(db, name, failoverStrategy, pref)
}

/** Factory and extractors */
@deprecated("Case accessors will be removed", "0.17.0")
object BSONCollection extends scala.runtime.AbstractFunction3[DB, String, FailoverStrategy, BSONCollection] {

  def apply(db: DB, name: String, failoverStrategy: FailoverStrategy): BSONCollection = new BSONCollection(db, name, failoverStrategy, db.defaultReadPreference)

  def unapply(c: BSONCollection): Option[(DB, String, FailoverStrategy)] = Some((c.db, c.name, c.failoverStrategy))
}
