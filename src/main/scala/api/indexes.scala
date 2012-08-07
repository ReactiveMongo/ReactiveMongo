package org.asyncmongo.api.indexes

import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.bson.handlers._
import org.asyncmongo.core.commands.{FindAndModify, LastError, Update}
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.util.Duration
import scala.concurrent.util.duration._
import org.asyncmongo.core.commands.FindAndModify
import org.asyncmongo.core.commands.DeleteIndex

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
 */
case class Index(
  key: List[(String, Boolean)], // true -> ascending (1)
  name: Option[String] = None,
  unique: Boolean = false,
  background: Boolean = false,
  dropDups: Boolean = false,
  sparse: Boolean = false,
  version: Option[Int] = None // let MongoDB decide
) {
  /** The name of the index (a default one is computed if none). */
  lazy val eventualName :String = name.getOrElse(key.foldLeft("") { (name, kv) =>
    name + (if(name.length > 0) "_" else "") + kv._1 + "_" + (if(kv._2) 1 else -1)
  })
}

/**
 * A MongoDB namespaced index.
 * A MongoDB index is composed with the namespace (the fully qualified collection name) and the other fields of [[org.asyncmongo.api.indexes.Index]].
 *
 * Consider reading [[http://www.mongodb.org/display/DOCS/Indexes the documentation about indexes in MongoDB]].
 *
 * @param namespace The fully qualified name of the indexed collection.
 * @param index The other fields of the index.
 */
case class NSIndex(
  namespace: String,
  index: Index
) {
  val (dbName :String, collectionName :String) = {
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
  import DefaultBSONHandlers._

  val collection = db("system.indexes")

  /** Gets a future list of all the index on this database. */
  def list() :Future[List[NSIndex]] = {
    implicit val reader = IndexesManager.NSIndexReader
    val cursor :Cursor[NSIndex] = collection.find(BSONDocument())
    cursor.toList()
  }

  /**
   * Creates the given index only if it does not exist on this database.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param The index to create.
   *
   * @return a future containing true if the index was created, false if it already exists.
   */
  def ensure(nsIndex: NSIndex) :Future[Boolean] = {
    val query = BSONDocument(
      "ns" -> BSONString(nsIndex.namespace),
      "name" -> BSONString(nsIndex.index.eventualName))

    collection.find(query).headOption.flatMap { opt =>
      if(!opt.isDefined)
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
   * @param The index to create.
   */
  def create(nsIndex: NSIndex) :Future[LastError] = {
    implicit val writer = IndexesManager.NSIndexWriter
    collection.insert(nsIndex).filter(!_.inError)
  }

  /**
   * Deletes the given index on that database.
   *
   * @return The deleted index number.
   */
  def delete(nsIndex: NSIndex) :Future[Int] = delete(nsIndex.collectionName, nsIndex.index.eventualName)

  /**
   * Deletes the given index on that database.
   *
   * @return The deleted index number.
   */
  def delete(collectionName: String, indexName: String) :Future[Int] = db(collectionName).command(DeleteIndex(collectionName, indexName))

  /** Gets a manager for the given collection. */
  def onCollection(name: String) = new CollectionIndexesManager(db.dbName + "." + name, this)
}

class CollectionIndexesManager(fqName: String, manager: IndexesManager)(implicit timeout :Duration = 5 seconds, context: ExecutionContext) {
  lazy val collectionName = {
    val (_, r) = fqName.span(_ != '.')
    r.drop(1)
  }
  def list() :Future[List[Index]] = manager.list.map { list =>
    list.filter( nsIndex =>
      nsIndex.dbName == fqName).map(_.index)
  }

  /**
   * Creates the given index only if it does not exist on this collection.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param The index to create.
   *
   * @return a future containing true if the index was created, false if it already exists.
   */
  def ensure(index: Index) :Future[Boolean] =
    manager.ensure(NSIndex(fqName, index))

  /**
   * Creates the given index.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param The index to create.
   */
  def create(index: Index) :Future[LastError] =
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
  protected def toBSONDocument(nsIndex :NSIndex) = {
    val doc = BSONDocument(
      "ns" -> BSONString(nsIndex.namespace),
      "name" -> BSONString(nsIndex.index.eventualName),
      "key" -> {
        val doc = BSONDocument()
        for(kv <- nsIndex.index.key)
          doc += kv._1 -> BSONInteger(if(kv._2) 1 else -1)
        doc
      })
    if(nsIndex.index.background)
      doc += "background" -> BSONBoolean(true)
    if(nsIndex.index.dropDups)
      doc += "dropDups" -> BSONBoolean(true)
    if(nsIndex.index.sparse)
      doc += "sparse" -> BSONBoolean(true)
    doc
  }

  implicit object NSIndexWriter extends BSONWriter[NSIndex] {
    import org.jboss.netty.buffer._
    def write(nsIndex: NSIndex) :ChannelBuffer = {
      if(nsIndex.index.key.isEmpty)
        throw new RuntimeException("the key should not be empty!")
      toBSONDocument(nsIndex).makeBuffer
    }
  }

  implicit object NSIndexReader extends BSONReader[NSIndex] {
    import org.jboss.netty.buffer._
    def read(buffer: ChannelBuffer) :NSIndex = {
      val doc = DefaultBSONHandlers.DefaultBSONDocumentReader.read(buffer)
      NSIndex(
        doc.getAs[BSONString]("ns").map(_.value).get,
        Index(
          doc.getAs[TraversableBSONDocument]("key").get.bsonIterator.toList.map { elem =>
            elem.name -> (elem.value.asInstanceOf[BSONInteger].value == 1)
          },
          doc.getAs[BSONString]("name").map(_.value),
          doc.getAs[BSONBoolean]("unique").map(_.value).getOrElse(false),
          doc.getAs[BSONBoolean]("background").map(_.value).getOrElse(false),
          doc.getAs[BSONBoolean]("dropDups").map(_.value).getOrElse(false),
          doc.getAs[BSONBoolean]("sparse").map(_.value).getOrElse(false),
          doc.getAs[BSONInteger]("v").map(_.value)
        )
      )
    }
  }
}