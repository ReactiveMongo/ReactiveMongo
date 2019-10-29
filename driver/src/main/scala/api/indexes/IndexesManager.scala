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

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{
  Compat,
  DB,
  DBMetaCommands,
  Cursor,
  CursorProducer,
  ReadPreference,
  SerializationPack
}

import reactivemongo.api.commands.{ CommandError, DropIndexes, WriteResult }

import scala.concurrent.{ Future, ExecutionContext }

/**
 * Indexes manager at database level.
 *
 * @define collectionNameParam the collection name
 * @define nsIndexToCreate the index to create
 * @define droppedCount The number of indexes that were dropped.
 */
sealed trait IndexesManager {

  /** Gets a future list of all the index on this database. */
  def list(): Future[List[NSIndex]]

  /**
   * Creates the given index only if it does not exist on this database.
   *
   * The following rules are used to check the matching index:
   * - if `nsIndex.isDefined`, it checks using the index name,
   * - otherwise it checks using the key.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param nsIndex $nsIndexToCreate
   *
   * @return true if the index was created, false if it already exists.
   */
  def ensure(nsIndex: NSIndex): Future[Boolean]

  /**
   * Creates the given index.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param nsIndex $nsIndexToCreate
   */
  def create(nsIndex: NSIndex): Future[WriteResult]

  /**
   * Drops the given index on the given collection.
   *
   * @return $droppedCount
   */
  def drop(nsIndex: NSIndex): Future[Int] =
    drop(nsIndex.collectionName, nsIndex.index.eventualName)

  /**
   * Drops the given index on the given collection.
   *
   * @param collectionName $collectionNameParam
   * @param indexName the name of the index to be dropped
   * @return $droppedCount
   */
  def drop(collectionName: String, indexName: String): Future[Int]

  /**
   * Drops all the indexes on the given collection.
   *
   * @param collectionName $collectionNameParam
   */
  def dropAll(collectionName: String): Future[Int]

  /**
   * Returns a manager for the given collection.
   *
   * @param collectionName $collectionNameParam
   */
  def onCollection(@deprecatedName(Symbol("name")) collectionName: String): CollectionIndexesManager
}

/**
 * A helper class to manage the indexes on a Mongo 2.x database.
 *
 * @param db The subject database.
 */
final class LegacyIndexesManager(db: DB)(
  implicit
  ec: ExecutionContext) extends IndexesManager {

  val collection = db("system.indexes")(Compat.defaultCollectionProducer)
  import collection.pack

  private lazy val builder = pack.newBuilder

  def list(): Future[List[NSIndex]] = collection.find(builder.document(Seq.empty), Option.empty[pack.Document]).cursor(db.connection.options.readPreference)(IndexesManager.nsIndexReader, CursorProducer.defaultCursorProducer).collect[List](-1, Cursor.FailOnError[List[NSIndex]]())

  def ensure(nsIndex: NSIndex): Future[Boolean] = {
    import builder.string

    val query = builder.document(Seq(
      builder.elementProducer("ns", string(nsIndex.namespace)),
      builder.elementProducer("name", string(nsIndex.index.eventualName))))

    collection.find(query, Option.empty[pack.Document]).one.flatMap { idx =>
      if (!idx.isDefined) {
        create(nsIndex).map(_ => true)
      } else {
        Future.successful(false)
      }
    }
  }

  private[reactivemongo] implicit lazy val nsIndexWriter =
    IndexesManager.nsIndexWriter(pack)

  def create(nsIndex: NSIndex): Future[WriteResult] =
    collection.insert.one(nsIndex)

  private implicit lazy val dropWriter = DropIndexes.writer(pack)

  private lazy val dropReader = DropIndexes.reader(pack)

  def drop(collectionName: String, indexName: String): Future[Int] = {
    implicit def reader = dropReader

    db.collection(collectionName).
      runValueCommand(DropIndexes(indexName), ReadPreference.primary)
  }

  def dropAll(collectionName: String): Future[Int] = drop(collectionName, "*")

  def onCollection(@deprecatedName(Symbol("name")) collectionName: String): CollectionIndexesManager = new LegacyCollectionIndexesManager(db.name, collectionName, this)
}

/**
 * A helper class to manage the indexes on a Mongo 3.x database.
 *
 * @param db The subject database.
 */
final class DefaultIndexesManager(db: DB with DBMetaCommands)(
  implicit
  ec: ExecutionContext) extends IndexesManager {

  private def listIndexes(collections: List[String], indexes: List[NSIndex]): Future[List[NSIndex]] = collections match {
    case c :: cs => onCollection(c).list().flatMap(ix =>
      listIndexes(cs, indexes ++ ix.map(NSIndex(s"${db.name}.$c", _))))

    case _ => Future.successful(indexes)
  }

  def list(): Future[List[NSIndex]] =
    db.collectionNames.flatMap(listIndexes(_, Nil))

  def ensure(nsIndex: NSIndex): Future[Boolean] =
    onCollection(nsIndex.collectionName).ensure(nsIndex.index)

  def create(nsIndex: NSIndex): Future[WriteResult] =
    onCollection(nsIndex.collectionName).create(nsIndex.index)

  def drop(collectionName: String, indexName: String): Future[Int] =
    onCollection(collectionName).drop(indexName)

  def dropAll(collectionName: String): Future[Int] =
    onCollection(collectionName).dropAll()

  def onCollection(@deprecatedName(Symbol("name")) collectionName: String): CollectionIndexesManager = new DefaultCollectionIndexesManager(db, collectionName)
}

/**
 * @define the index to create
 * @define droppedCount The number of indexes that were dropped.
 * @define indexToCreate the index to create
 */
sealed trait CollectionIndexesManager {
  /** Returns the list of indexes for the current collection. */
  def list(): Future[List[Index]]

  /**
   * Creates the given index only if it does not exist on this collection.
   *
   * The following rules are used to check the matching index:
   * - if `nsIndex.isDefined`, it checks using the index name,
   * - otherwise it checks using the key.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param index $indexToCreate
   *
   * @return true if the index was created, false if it already exists.
   */
  def ensure(index: Index): Future[Boolean]

  /**
   * Creates the given index.
   *
   * Warning: given the options you choose, and the data to index, it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param index $indexToCreate
   */
  def create(index: Index): Future[WriteResult]

  /**
   * Drops the given index on that collection.
   *
   * @param indexName the name of the index to be dropped
   * @return $droppedCount
   */
  def drop(indexName: String): Future[Int]

  /**
   * Drops all the indexes on that collection.
   * @return $droppedCount
   */
  def dropAll(): Future[Int]
}

private class LegacyCollectionIndexesManager(
  db: String, collectionName: String, legacy: LegacyIndexesManager)(
  implicit
  ec: ExecutionContext) extends CollectionIndexesManager {

  val fqName = db + "." + collectionName

  def list(): Future[List[Index]] =
    legacy.list.map(_.filter(_.namespace == fqName).map(_.index))

  def ensure(index: Index): Future[Boolean] =
    legacy.ensure(NSIndex(fqName, index))

  def create(index: Index): Future[WriteResult] =
    legacy.create(NSIndex(fqName, index))

  def drop(indexName: String): Future[Int] =
    legacy.drop(collectionName, indexName)

  def dropAll(): Future[Int] = legacy.dropAll(collectionName)
}

private class DefaultCollectionIndexesManager(db: DB, collectionName: String)(
  implicit
  ec: ExecutionContext) extends CollectionIndexesManager {

  import reactivemongo.api.commands.{
    CreateIndexes,
    Command,
    ListIndexes
  }

  import Compat.{ internalSerializationPack, writeResultReader }

  private lazy val collection = db(collectionName)
  private lazy val listCommand = ListIndexes(db.name)

  private lazy val runner =
    Command.run(internalSerializationPack, db.failoverStrategy)

  private implicit lazy val listWriter =
    ListIndexes.writer(internalSerializationPack)

  private implicit lazy val indexReader =
    IndexesManager.indexReader[Compat.SerializationPack](
      internalSerializationPack)

  private implicit lazy val listReader =
    ListIndexes.reader(internalSerializationPack)

  def list(): Future[List[Index]] =
    runner(collection, listCommand, ReadPreference.primary).recoverWith {
      case CommandError.Code(26 /* no database or collection */ ) =>
        Future.successful(List.empty[Index])

      case err => Future.failed(err)
    }

  def ensure(index: Index): Future[Boolean] = list().flatMap { indexes =>
    val idx = index.name match {
      case Some(n) => indexes.find(_.name.exists(_ == n))
      case _       => indexes.find(_.key == index.key)
    }

    if (!idx.isDefined) {
      create(index).map(_ => true)
    } else {
      Future.successful(false)
    }
  }

  private implicit val createWriter =
    CreateIndexes.writer(internalSerializationPack)

  def create(index: Index): Future[WriteResult] =
    runner(
      collection,
      CreateIndexes(db.name, List(index)),
      ReadPreference.primary)

  private implicit def dropWriter = IndexesManager.dropWriter
  private implicit def dropReader = IndexesManager.dropReader

  def drop(indexName: String): Future[Int] = {
    runner(
      collection, DropIndexes(indexName), ReadPreference.primary).map(_.value)
  }

  @inline def dropAll(): Future[Int] = drop("*")
}

/** Factory for indexes manager scoped with a specified collection. */
object CollectionIndexesManager {
  /**
   * Returns an indexes manager for specified collection.
   *
   * @param db the database
   * @param collectionName the collection name
   */
  def apply(db: DB, collectionName: String)(implicit ec: ExecutionContext): CollectionIndexesManager = {
    val wireVer = db.connectionState.metadata.maxWireVersion

    if (wireVer >= MongoWireVersion.V30) {
      new DefaultCollectionIndexesManager(db, collectionName)
    } else new LegacyCollectionIndexesManager(db.name, collectionName,
      new LegacyIndexesManager(db))
  }
}

object IndexesManager {
  /**
   * Returns an indexes manager for specified database.
   *
   * @param db the database
   */
  def apply(db: DB with DBMetaCommands)(implicit ec: ExecutionContext): IndexesManager = {
    val wireVer = db.connectionState.metadata.maxWireVersion

    if (wireVer >= MongoWireVersion.V30) new DefaultIndexesManager(db)
    else new LegacyIndexesManager(db)
  }

  @deprecated("Will be internal", "0.19.0")
  object NSIndexWriter extends reactivemongo.bson.BSONDocumentWriter[NSIndex] {
    private val underlying =
      nsIndexWriter(reactivemongo.api.BSONSerializationPack)

    def write(nsIndex: NSIndex): reactivemongo.bson.BSONDocument =
      underlying.write(nsIndex)
  }

  private[reactivemongo] def nsIndexWriter[P <: SerializationPack](pack: P): pack.Writer[NSIndex] = {
    val builder = pack.newBuilder
    val decoder = pack.newDecoder
    val writeIndexType = IndexType.write(pack)(builder)

    import builder.{ boolean, document, elementProducer => element, string }

    pack.writer[NSIndex] { nsIndex =>
      import nsIndex.index

      if (index.key.isEmpty) {
        throw new RuntimeException("the key should not be empty!")
      }

      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("ns", string(nsIndex.namespace)),
        element("name", string(index.eventualName)),
        element("key", document(index.key.collect {
          case (k, v) => element(k, writeIndexType(v))
        })))

      if (index.background) {
        elements += element("background", boolean(true))
      }

      if (index.sparse) {
        elements += element("sparse", boolean(true))
      }

      if (index.unique) {
        elements += element("unique", boolean(true))
      }

      index.partialFilter.foreach { partialFilter =>
        elements += element(
          "partialFilterExpression", pack.document(partialFilter))
      }

      val opts = pack.document(index.options)
      decoder.names(opts).foreach { nme =>
        decoder.get(opts, nme).foreach { v =>
          elements += element(nme, v)
        }
      }

      document(elements.result())
    }
  }

  @deprecated("Will be internal", "0.19.0")
  object IndexReader extends reactivemongo.bson.BSONDocumentReader[Index] {
    private val underlying =
      indexReader(reactivemongo.api.BSONSerializationPack)

    def read(doc: reactivemongo.bson.BSONDocument): Index = underlying.read(doc)
  }

  private[reactivemongo] def indexReader[P <: SerializationPack](pack: P): pack.Reader[Index] = {
    val decoder = pack.newDecoder
    val builder = pack.newBuilder

    pack.reader[Index] { doc =>
      decoder.child(doc, "key").fold[Index](
        throw new Exception("the key must be defined")) { k =>
          val ks = decoder.names(k).flatMap { nme =>
            decoder.get(k, nme).map { v =>
              nme -> IndexType(pack.bsonValue(v))
            }
          }

          val key = decoder.child(doc, "weights").fold(ks) { w =>
            val fields = decoder.names(w)

            (ks, fields).zipped.map {
              case ((_, tpe), name) => name -> tpe
            }
          }.toSeq

          val opts = builder.document(decoder.names(doc).flatMap {
            case "ns" | "key" | "name" | "unique" |
              "background" | "dropDups" | "sparse" | "v" | "partialFilterExpression" =>
              Seq.empty[pack.ElementProducer]

            case nme =>
              decoder.get(doc, nme).map { v =>
                builder.elementProducer(nme, v)
              }
          }.toSeq)

          val options = pack.bsonValue(opts) match {
            case legacyDoc: reactivemongo.bson.BSONDocument =>
              legacyDoc

            case _ =>
              reactivemongo.bson.BSONDocument.empty
          }

          val name = decoder.string(doc, "name")
          val unique = decoder.booleanLike(doc, "unique").getOrElse(false)
          val background = decoder.booleanLike(doc, "background").getOrElse(false)
          val dropDups = decoder.booleanLike(doc, "dropDups").getOrElse(false)
          val sparse = decoder.booleanLike(doc, "sparse").getOrElse(false)
          val version = decoder.int(doc, "v")
          val partialFilter =
            decoder.child(doc, "partialFilterExpression").
              map(pack.bsonValue).collect {
                case legacyDoc: reactivemongo.bson.BSONDocument =>
                  legacyDoc
              }

          Index(key, name, unique, background, dropDups,
            sparse, version, partialFilter, options)
        }
    }
  }

  @deprecated("Will be private", "0.19.0")
  object NSIndexReader extends reactivemongo.bson.BSONDocumentReader[NSIndex] {
    private val underlying =
      nsIndexReader(reactivemongo.api.BSONSerializationPack)

    def read(doc: reactivemongo.bson.BSONDocument): NSIndex =
      underlying.read(doc)
  }

  private[reactivemongo] def nsIndexReader[P <: SerializationPack](pack: P): pack.Reader[NSIndex] = {
    val decoder = pack.newDecoder
    val indexReader: pack.Reader[Index] = this.indexReader(pack)

    pack.reader[NSIndex] { doc =>
      decoder.string(doc, "ns").fold[NSIndex](
        throw new Exception("the namespace ns must be defined")) { ns =>
          NSIndex(ns, pack.deserialize(doc, indexReader))
        }
    }
  }

  private[api] implicit lazy val nsIndexReader =
    nsIndexReader[Compat.SerializationPack](Compat.internalSerializationPack)

  private[api] lazy val dropWriter =
    DropIndexes.writer(Compat.internalSerializationPack)

  private[api] lazy val dropReader =
    DropIndexes.reader(Compat.internalSerializationPack)

}
