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
package reactivemongo.api.indexes

import reactivemongo.api._
import reactivemongo.bson._
import DefaultBSONHandlers._
import reactivemongo.core.commands.{ DeleteIndex, LastError }
import reactivemongo.utils.option
import reactivemongo.core.netty._
import scala.concurrent.{ Future, ExecutionContext }

/** Type of Index */
sealed trait IndexType {
  /** Value of the index (`{fieldName: value}`). */
  def value: BSONValue
  private[indexes] def valueStr: String
}

object IndexType {
  object Ascending extends IndexType {
    def value = BSONInteger(1)
    def valueStr = "1"
  }

  object Descending extends IndexType {
    def value = BSONInteger(-1)
    def valueStr = "-1"
  }

  object Geo2D extends IndexType {
    def value = BSONString("2d")
    def valueStr = "2d"
  }

  object GeoHaystack extends IndexType {
    def value = BSONString("geoHaystack")
    def valueStr = "geoHaystack"
  }

  def apply(value: BSONValue) = value match {
    case BSONInteger(i) if i > 0 => Ascending
    case BSONInteger(i) if i < 0 => Descending
    case BSONString(s) if s == "2d" => Geo2D
    case BSONString(s) if s == "geoHaystack" => GeoHaystack
    case _ => throw new IllegalArgumentException("unsupported index type")
  }
}

/**
 * A MongoDB index (excluding the namespace).
 *
 * Consider reading [[http://www.mongodb.org/display/DOCS/Indexes the documentation about indexes in MongoDB]].
 *
 * @param key The index key (it can be composed of multiple fields). This list should not be empty!
 * @param name The name of this index. If you provide none, a name will be computed for you.
 * @param unique Enforces uniqueness.
 * @param background States if this index should be built in background. You should read [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-background%3Atrue the documentation about background indexing]] before using it.
 * @param dropDups States if duplicates should be discarded (if unique = true). Warning: you should read [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-dropDups%3Atrue the documentation]].
 * @param sparse States if the index to build should only consider the documents that have the indexed fields. See [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-sparse%3Atrue the documentation]] on the consequences of such an index.
 * @param version Indicates the [[http://www.mongodb.org/display/DOCS/Index+Versions version]] of the index (1 for >= 2.0, else 0). You should let MongoDB decide.
 * @param options Optional parameters for this index (typically specific to an IndexType like Geo2D).
 */
case class Index(
    key: List[(String, IndexType)],
    name: Option[String] = None,
    unique: Boolean = false,
    background: Boolean = false,
    dropDups: Boolean = false,
    sparse: Boolean = false,
    version: Option[Int] = None, // let MongoDB decide
    options: BSONDocument = BSONDocument()) {
  /** The name of the index (a default one is computed if none). */
  lazy val eventualName: String = name.getOrElse(key.foldLeft("") { (name, kv) =>
    name + (if (name.length > 0) "_" else "") + kv._1 + "_" + kv._2.valueStr
  })
}

/**
 * A MongoDB namespaced index.
 * A MongoDB index is composed with the namespace (the fully qualified collection name) and the other fields of [[reactivemongo.api.indexes.Index]].
 *
 * Consider reading [[http://www.mongodb.org/display/DOCS/Indexes the documentation about indexes in MongoDB]].
 *
 * @param namespace The fully qualified name of the indexed collection.
 * @param index The other fields of the index.
 */
case class NSIndex(
    namespace: String,
    index: Index) {
  val (dbName: String, collectionName: String) = {
    val spanned = namespace.span(_ != '.')
    spanned._1 -> spanned._2.drop(1)
  }
}

/**
 * A helper class to manage the indexes on the given database.
 *
 * @param db The subject database.
 */
class IndexesManager(db: DB)(implicit context: ExecutionContext) {
  val collection = db("system.indexes")

  /** Gets a future list of all the index on this database. */
  def list(): Future[List[NSIndex]] = {
    implicit val reader = IndexesManager.NSIndexReader
    val cursor: Cursor[NSIndex] = collection.find(BSONDocument()).cursor
    cursor.toList()
  }

  /**
   * Creates the given index only if it does not exist on this database.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param nsIndex The index to create.
   *
   * @return a future containing true if the index was created, false if it already exists.
   */
  def ensure(nsIndex: NSIndex): Future[Boolean] = {
    val query = BSONDocument(
      "ns" -> BSONString(nsIndex.namespace),
      "name" -> BSONString(nsIndex.index.eventualName))

    collection.find(query).one.flatMap { opt =>
      if (!opt.isDefined)
        create(nsIndex).map(_ => true)
      // there is a match, returning a future ok. TODO
      else Future(false)
    }
  }

  /**
   * Creates the given index.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param nsIndex The index to create.
   */
  def create(nsIndex: NSIndex): Future[LastError] = {
    implicit val writer = IndexesManager.NSIndexWriter
    collection.insert(nsIndex)
  }

  /**
   * Deletes the given index on that database.
   *
   * @return The deleted index number.
   */
  def delete(nsIndex: NSIndex): Future[Int] = delete(nsIndex.collectionName, nsIndex.index.eventualName)

  /**
   * Deletes the given index on that database.
   *
   * @return The deleted index number.
   */
  def delete(collectionName: String, indexName: String): Future[Int] = db.command(DeleteIndex(collectionName, indexName))

  /** Gets a manager for the given collection. */
  def onCollection(name: String) = new CollectionIndexesManager(db.name + "." + name, this)
}

class CollectionIndexesManager(fqName: String, manager: IndexesManager)(implicit context: ExecutionContext) {
  lazy val collectionName = {
    val (_, r) = fqName.span(_ != '.')
    r.drop(1)
  }
  def list(): Future[List[Index]] = manager.list.map { list =>
    list.filter(nsIndex =>
      nsIndex.namespace == fqName).map(_.index)
  }

  /**
   * Creates the given index only if it does not exist on this collection.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param index The index to create.
   *
   * @return a future containing true if the index was created, false if it already exists.
   */
  def ensure(index: Index): Future[Boolean] =
    manager.ensure(NSIndex(fqName, index))

  /**
   * Creates the given index.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param index The index to create.
   */
  def create(index: Index): Future[LastError] =
    manager.create(NSIndex(fqName, index))

  /**
   * Deletes the given index on that collection.
   *
   * @return The deleted index number.
   */
  def delete(index: Index) = manager.delete(NSIndex(collectionName, index))

  /**
   * Deletes the given index on that collection.
   *
   * @return The deleted index number.
   */
  def delete(name: String) = manager.delete(collectionName, name)
}

object IndexesManager {
  protected def toBSONDocument(nsIndex: NSIndex) = {
    BSONDocument(
      "ns" -> BSONString(nsIndex.namespace),
      "name" -> BSONString(nsIndex.index.eventualName),
      "key" -> BSONDocument(
        (for (kv <- nsIndex.index.key)
          yield kv._1 -> kv._2.value).toStream),
      "background" -> option(nsIndex.index.background, BSONBoolean(true)),
      "dropDups" -> option(nsIndex.index.dropDups, BSONBoolean(true)),
      "sparse" -> option(nsIndex.index.sparse, BSONBoolean(true)),
      "unique" -> option(nsIndex.index.unique, BSONBoolean(true))) ++ nsIndex.index.options
  }

  implicit object NSIndexWriter extends BSONDocumentWriter[NSIndex] {
    import org.jboss.netty.buffer._
    def write(nsIndex: NSIndex): BSONDocument = {
      if (nsIndex.index.key.isEmpty)
        throw new RuntimeException("the key should not be empty!")
      toBSONDocument(nsIndex)
    }
  }

  implicit object NSIndexReader extends BSONDocumentReader[NSIndex] {
    import org.jboss.netty.buffer._
    def read(doc: BSONDocument): NSIndex = {
      val options = doc.elements.filterNot { element =>
        element._1 == "ns" || element._1 == "key" || element._1 == "name" || element._1 == "unique" ||
          element._1 == "background" || element._1 == "dropDups" || element._1 == "sparse" || element._1 == "v"
      }.toSeq
      NSIndex(
        doc.getAs[BSONString]("ns").map(_.value).get,
        Index(
          doc.getAs[BSONDocument]("key").get.elements.map { elem =>
            elem._1 -> IndexType(elem._2)
          }.toList,
          doc.getAs[BSONString]("name").map(_.value),
          doc.getAs[BSONBoolean]("unique").map(_.value).getOrElse(false),
          doc.getAs[BSONBoolean]("background").map(_.value).getOrElse(false),
          doc.getAs[BSONBoolean]("dropDups").map(_.value).getOrElse(false),
          doc.getAs[BSONBoolean]("sparse").map(_.value).getOrElse(false),
          doc.getAs[BSONInteger]("v").map(_.value),
          BSONDocument(options.toStream)))
    }
  }
}