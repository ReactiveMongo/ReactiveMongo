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
import reactivemongo.api.commands.bson._
import reactivemongo.api.collections.{
  BatchCommands,
  GenericCollection,
  GenericCollectionProducer
}
import reactivemongo.api.BSONSerializationPack

object `package` {
  implicit object BSONCollectionProducer extends GenericCollectionProducer[BSONSerializationPack.type, BSONCollection] {
    def apply(db: DB, name: String, failoverStrategy: FailoverStrategy = FailoverStrategy()): BSONCollection = new BSONCollection(db, name, failoverStrategy, db.defaultReadPreference)
  }
}

object BSONBatchCommands extends BatchCommands[BSONSerializationPack.type] {
  val pack = BSONSerializationPack

  val CountCommand = BSONCountCommand
  implicit def CountWriter = BSONCountCommandImplicits.CountWriter
  implicit def CountResultReader = BSONCountCommandImplicits.CountResultReader

  val DistinctCommand = BSONDistinctCommand
  implicit def DistinctWriter = BSONDistinctCommandImplicits.DistinctWriter
  implicit def DistinctResultReader = BSONDistinctCommandImplicits.DistinctResultReader

  val InsertCommand = BSONInsertCommand
  implicit def InsertWriter = BSONInsertCommandImplicits.InsertWriter

  val UpdateCommand = BSONUpdateCommand
  implicit def UpdateWriter = BSONUpdateCommandImplicits.UpdateWriter
  implicit def UpdateReader = BSONUpdateCommandImplicits.UpdateResultReader

  val DeleteCommand = BSONDeleteCommand
  implicit def DeleteWriter = BSONDeleteCommandImplicits.DeleteWriter
  implicit def DefaultWriteResultReader = BSONCommonWriteCommandsImplicits.DefaultWriteResultReader

  val FindAndModifyCommand = BSONFindAndModifyCommand
  implicit def FindAndModifyWriter = BSONFindAndModifyImplicits.FindAndModifyWriter
  implicit def FindAndModifyReader = BSONFindAndModifyImplicits.FindAndModifyResultReader

  val AggregationFramework = BSONAggregationFramework
  implicit def AggregateWriter = BSONAggregationImplicits.AggregateWriter
  implicit def AggregateReader =
    BSONAggregationImplicits.AggregationResultReader

  implicit def LastErrorReader = BSONGetLastErrorImplicits.LastErrorReader
}

@SerialVersionUID(1382847900L)
final class BSONCollection(
  val db: DB,
  val name: String,
  val failoverStrategy: FailoverStrategy,
  override val readPreference: ReadPreference) extends Product with GenericCollection[BSONSerializationPack.type]
  with scala.Serializable with java.io.Serializable {

  @transient val pack = BSONSerializationPack
  @transient val BatchCommands = BSONBatchCommands
  def genericQueryBuilder = BSONQueryBuilder(this, failoverStrategy)

  override lazy val toString =
    s"BSONCollection('${db.name}'.'$name', $failoverStrategy)"

  @deprecated(message = "No longer a case class", since = "0.12-RC2")
  def canEqual(that: Any): Boolean = that match {
    case _: BSONCollection => true
    case _                 => false
  }

  @deprecated(message = "No longer a case class", since = "0.12-RC2")
  val productArity = 3

  @deprecated(message = "No longer a case class", since = "0.12-RC2")
  def productElement(n: Int) = n match {
    case 0 => db
    case 1 => name
    case _ => failoverStrategy
  }

  @deprecated(message = "No longer a case class", since = "0.12-RC2")
  def copy(
    db: DB = this.db,
    name: String = this.name,
    failoverStrategy: FailoverStrategy = this.failoverStrategy): BSONCollection = new BSONCollection(
    db, name, failoverStrategy, readPreference)

  def withReadPreference(pref: ReadPreference): BSONCollection =
    new BSONCollection(db, name, failoverStrategy, pref)
}

/** Factory and extractors */
object BSONCollection extends scala.runtime.AbstractFunction3[DB, String, FailoverStrategy, BSONCollection] {

  @deprecated(message = "Use the class constructor", since = "0.12-RC2")
  def apply(db: DB, name: String, failoverStrategy: FailoverStrategy): BSONCollection = new BSONCollection(db, name, failoverStrategy, db.defaultReadPreference)

  def unapply(c: BSONCollection): Option[(DB, String, FailoverStrategy)] = Some((c.db, c.name, c.failoverStrategy))
}
