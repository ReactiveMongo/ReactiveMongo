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
  DB,
  DBMetaCommands,
  BSONSerializationPack => LegacyPack,
  Collation,
  Collection,
  Cursor,
  CursorProducer,
  FailoverStrategy,
  QueryOpts,
  ReadPreference,
  Serialization,
  SerializationPack,
  WriteConcern
}
import reactivemongo.api.collections.GenericQueryBuilder

import reactivemongo.api.commands.{
  Command,
  CommandCodecs,
  CommandError,
  DropIndexes,
  InsertCommand,
  ResolvedCollectionCommand,
  WriteResult
}

import scala.concurrent.{ Future, ExecutionContext }

/**
 * Indexes manager at database level.
 *
 * @define createDescription Creates the given index
 * @define dropDescription Drops the specified index
 * @define collectionNameParam the collection name
 * @define nsIndexToCreate the index to create
 * @define droppedCount The number of indexes that were dropped.
 */
sealed trait IndexesManager {
  type Pack <: SerializationPack with Singleton

  final type NSIndex = reactivemongo.api.indexes.NSIndex.Aux[Pack]

  /**
   * Lists all the index on this database.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.DefaultDB
   * import reactivemongo.api.indexes.NSIndex
   *
   * def listIndexes(db: DefaultDB)(
   *   implicit ec: ExecutionContext): Future[List[String]] =
   *   db.indexesManager.list().map(_.flatMap { ni: NSIndex =>
   *     ni.index.name.toList
   *   })
   * }}}
   */
  def list(): Future[List[NSIndex]]

  /**
   * $createDescription only if it does not exist on this database.
   *
   * The following rules are used to check the matching index:
   * - if `nsIndex.isDefined`, it checks using the index name,
   * - otherwise it checks using the key.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.DefaultDB
   * import reactivemongo.api.indexes.NSIndex
   *
   * def ensureIndexes(
   *   db: DefaultDB, is: Seq[NSIndex.Default])(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   Future.sequence(
   *     is.map(idx => db.indexesManager.ensure(idx))).map(_ => {})
   * }}}
   *
   * _Warning_: given the options you choose, and the data to index,
   * it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param nsIndex $nsIndexToCreate
   * @return true if the index was created, false if it already exists.
   */
  def ensure(nsIndex: NSIndex): Future[Boolean]

  /**
   * $createDescription.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.DefaultDB
   * import reactivemongo.api.indexes.NSIndex
   *
   * def createIndexes(
   *   db: DefaultDB, is: Seq[NSIndex.Default])(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   Future.sequence(
   *     is.map(idx => db.indexesManager.create(idx))).map(_ => {})
   * }}}
   *
   * _Warning_: given the options you choose, and the data to index,
   * it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param nsIndex $nsIndexToCreate
   */
  def create(nsIndex: NSIndex): Future[WriteResult]

  /**
   * $dropDescription.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.DefaultDB
   * import reactivemongo.api.indexes.NSIndex
   *
   * def dropIndex(db: DefaultDB, idx: NSIndex.Default)(
   *   implicit ec: ExecutionContext): Future[Int] =
   *   db.indexesManager.drop(idx)
   * }}}
   *
   * @return $droppedCount
   */
  def drop(nsIndex: NSIndex): Future[Int] =
    drop(nsIndex.collectionName, nsIndex.index.eventualName)

  /**
   * $dropDescription on the given collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DefaultDB
   *
   * def dropIndex(db: DefaultDB, name: String)(
   *   implicit ec: ExecutionContext): Future[Int] =
   *   db.indexesManager.drop("myColl", name)
   * }}}
   *
   * @param collectionName $collectionNameParam
   * @param indexName the name of the index to be dropped
   * @return $droppedCount
   */
  def drop(collectionName: String, indexName: String): Future[Int]

  /**
   * Drops all the indexes on the specified collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DefaultDB
   *
   * def dropAllIndexes(db: DefaultDB)(
   *   implicit ec: ExecutionContext): Future[Int] =
   *   db.indexesManager.dropAll("myColl")
   * }}}
   *
   * @param collectionName $collectionNameParam
   * @return $droppedCount
   */
  def dropAll(collectionName: String): Future[Int]

  /**
   * Returns a manager for the specified collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DefaultDB
   *
   * def countCollIndexes(db: DefaultDB, collName: String)(
   *   implicit ec: ExecutionContext): Future[Int] =
   *   db.indexesManager.onCollection(collName).list().map(_.size)
   * }}}
   *
   * @param collectionName $collectionNameParam
   */
  def onCollection(@deprecatedName(Symbol("name")) collectionName: String): CollectionIndexesManager.Aux[Pack]
}

/**
 * A helper class to manage the indexes on a Mongo 2.x database.
 *
 * @param db The subject database.
 */
@deprecated("Internal: will be made private", "1.0.0-rc.1")
private[api] sealed abstract class AbstractLegacyManager(db: DB)( // TODO: Remove
  implicit
  ec: ExecutionContext) extends IndexesManager { self =>

  protected val pack: Pack

  protected lazy val collection: Collection =
    db("system.indexes")(Serialization.defaultCollectionProducer)

  private lazy val builder = pack.newBuilder

  def list(): Future[List[NSIndex]] = {
    val query = new QueryBuilder(None)
    val reader = IndexesManager.nsIndexReader[Pack](query.pack)

    query.cursor(db.connection.options.readPreference)(
      reader,
      CursorProducer.defaultCursorProducer).
      collect[List](-1, Cursor.FailOnError[List[NSIndex]]())
  }

  def ensure(nsIndex: NSIndex): Future[Boolean] = {
    import builder.string

    val query = new QueryBuilder(Some(
      builder.document(Seq(
        builder.elementProducer("ns", string(nsIndex.namespace)),
        builder.elementProducer("name", string(nsIndex.index.eventualName))))))

    val reader = IndexesManager.nsIndexReader[Pack](query.pack)

    query.one(reader, ec).flatMap { idx =>
      if (!idx.isDefined) {
        create(nsIndex).map(_ => true)
      } else {
        Future.successful(false)
      }
    }
  }

  private[reactivemongo] lazy val nsIndexWriter =
    IndexesManager.nsIndexWriter[Pack](pack)

  private lazy val runner = Command.run[Pack](pack, FailoverStrategy.default)

  private object InsertCmd extends InsertCommand[pack.type] {
    val pack: self.pack.type = self.pack
  }

  private implicit lazy val insertWriter = {
    val w = InsertCommand.writer[Pack](pack)(InsertCmd)(None)
    pack.writer[ResolvedCollectionCommand[InsertCmd.Insert]](w)
  }

  private implicit lazy val insertReader: pack.Reader[InsertCmd.InsertResult] =
    CommandCodecs.defaultWriteResultReader[Pack](pack)

  def create(nsIndex: NSIndex): Future[WriteResult] =
    Future(pack.serialize(nsIndex, nsIndexWriter)).flatMap { doc =>
      runner(collection, new InsertCmd.Insert(
        head = doc,
        tail = Seq.empty,
        ordered = false,
        writeConcern = WriteConcern.Default,
        bypassDocumentValidation = false),
        ReadPreference.primary)
    }

  private implicit def dropWriter = DropIndexes.writer[Pack](pack)
  private implicit def dropReader = DropIndexes.reader[Pack](pack)

  def drop(collectionName: String, indexName: String): Future[Int] =
    runner.unboxed(
      db.collection(collectionName),
      DropIndexes(indexName),
      ReadPreference.primary)

  def dropAll(collectionName: String): Future[Int] = drop(collectionName, "*")

  def onCollection(@deprecatedName(Symbol("name")) collectionName: String): CollectionIndexesManager.Aux[Pack] = new CollectionManager(collectionName)

  // ---

  private final class QueryBuilder(
    query: Option[pack.Document])
    extends GenericQueryBuilder[self.pack.type] {

    val pack: self.pack.type = self.pack
    override type Self = GenericQueryBuilder[pack.type]

    @inline def queryOption: Option[pack.Document] = query
    val sortOption = Option.empty[pack.Document]
    val projectionOption = Option.empty[pack.Document]
    val hintOption = Option.empty[pack.Document]
    val explainFlag: Boolean = false
    val snapshotFlag: Boolean = false
    val commentString = Some("LegacyIndexesManager")
    val options = QueryOpts()
    val failoverStrategy = FailoverStrategy.default
    val maxTimeMsOption = Option.empty[Long]
    val version = self.db.connectionState.metadata.maxWireVersion

    @inline override def collection: Collection = self.collection

    def copy( // TODO: Remove
      queryOption: Option[pack.Document],
      sortOption: Option[pack.Document],
      projectionOption: Option[pack.Document],
      hintOption: Option[pack.Document],
      explainFlag: Boolean,
      snapshotFlag: Boolean,
      commentString: Option[String],
      options: QueryOpts,
      failoverStrategy: FailoverStrategy,
      maxTimeMsOption: Option[Long]): Self = this

  }

  private final class CollectionManager(collectionName: String)
    extends CollectionIndexesManager {

    type Pack = self.Pack

    protected val pack: Pack = self.pack

    lazy val fqName = self.db.name + "." + collectionName

    def list(): Future[List[Index]] =
      self.list.map(_.filter(_.namespace == fqName).map(_.idx))

    def ensure(index: Index): Future[Boolean] =
      self.ensure(NSIndex.at[Pack](fqName, index))

    def create(index: Index): Future[WriteResult] =
      self.create(NSIndex.at[Pack](fqName, index))

    def drop(indexName: String): Future[Int] =
      self.drop(collectionName, indexName)

    def dropAll(): Future[Int] = self.dropAll(collectionName)
  }
}

/**
 * A helper class to manage the indexes on a Mongo 2.x database.
 *
 * @param db The subject database.
 */
@deprecated("Internal: will be made private", "1.0.0-rc.1")
class LegacyIndexesManager(db: DB)( // TODO: Remove
  implicit
  ec: ExecutionContext) extends AbstractLegacyManager(db) { self =>

  type Pack = Serialization.Pack

  protected val pack: Pack = Serialization.internalSerializationPack
}

private[api] sealed abstract class AbstractIndexesManager(
  db: DB with DBMetaCommands)(
  implicit
  ec: ExecutionContext) extends IndexesManager { self =>

  protected val pack: Pack

  private def listIndexes(collections: List[String], indexes: List[NSIndex]): Future[List[NSIndex]] = collections match {
    case c :: cs => onCollection(c).list().flatMap(ix =>
      listIndexes(cs, indexes ++ ix.map(NSIndex.at[Pack](s"${db.name}.$c", _))))

    case _ => Future.successful(indexes)
  }

  def list(): Future[List[NSIndex]] =
    db.collectionNames.flatMap(listIndexes(_, Nil))

  def ensure(nsIndex: NSIndex): Future[Boolean] =
    onCollection(nsIndex.collectionName).ensure(nsIndex.idx)

  def create(nsIndex: NSIndex): Future[WriteResult] =
    onCollection(nsIndex.collectionName).create(nsIndex.idx)

  def drop(collectionName: String, indexName: String): Future[Int] =
    onCollection(collectionName).drop(indexName)

  def dropAll(collectionName: String): Future[Int] =
    onCollection(collectionName).dropAll()

  def onCollection(@deprecatedName(Symbol("name")) collectionName: String): CollectionIndexesManager.Aux[self.Pack] = new CollectionManager(collectionName)

  // ---

  private lazy val runner = Command.run[Pack](pack, db.failoverStrategy)

  private final class CollectionManager(
    collectionName: String) extends CollectionIndexesManager {

    import reactivemongo.api.commands.{
      CreateIndexes,
      Command,
      ListIndexes
    }

    private[api] lazy val collection = db(collectionName)

    type Pack = self.Pack

    private lazy val listCommand = new ListIndexes.Command[Pack](db.name)

    private implicit lazy val listWriter = ListIndexes.writer[Pack](pack)

    private implicit lazy val indexReader: pack.Reader[Index] =
      IndexesManager.indexReader[Pack](pack)

    private implicit lazy val listReader: pack.Reader[List[Index]] =
      ListIndexes.reader[Pack](pack)

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

    private implicit val createWriter = CreateIndexes.writer[Pack](pack)

    private implicit lazy val writeResultReader: pack.Reader[WriteResult] =
      CommandCodecs.writeResultReader[WriteResult, Pack](pack)

    def create(index: Index): Future[WriteResult] =
      runner(
        collection,
        new CreateIndexes.Command[Pack](db.name, List(index)),
        ReadPreference.primary)

    private implicit def dropWriter = DropIndexes.writer[Pack](pack)
    private implicit def dropReader = DropIndexes.reader[Pack](pack)

    def drop(indexName: String): Future[Int] = {
      runner(
        collection, DropIndexes(indexName), ReadPreference.primary).map(_.value)
    }

    @inline def dropAll(): Future[Int] = drop("*")
  }
}

/**
 * A helper class to manage the indexes on a Mongo 3.x database.
 *
 * @param db the subject database
 */
@deprecated("Internal: will be made private", "1.0.0-rc.1")
class DefaultIndexesManager(db: DB with DBMetaCommands)(
  implicit
  ec: ExecutionContext) extends AbstractIndexesManager(db) {

  type Pack = Serialization.Pack
  val pack: Pack = Serialization.internalSerializationPack
}

/**
 * @define the index to create
 * @define droppedCount The number of indexes that were dropped.
 * @define indexToCreate the index to create
 * @define createDescription Creates the given index
 */
sealed trait CollectionIndexesManager {
  type Pack <: SerializationPack

  type Index = reactivemongo.api.indexes.Index.Aux[Pack]

  /**
   * Lists the indexes for the current collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.CollectionMetaCommands
   *
   * def listIndexes(coll: CollectionMetaCommands)(
   *   implicit ec: ExecutionContext): Future[List[String]] =
   *   coll.indexesManager.list().map(_.flatMap { idx =>
   *     idx.name.toList
   *   })
   * }}}
   */
  def list(): Future[List[Index]]

  /**
   * $createDescription only if it does not exist on this collection.
   *
   * The following rules are used to check the matching index:
   * - if `nsIndex.isDefined`, it checks using the index name,
   * - otherwise it checks using the key.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.CollectionMetaCommands
   * import reactivemongo.api.indexes.Index
   *
   * def ensureIndexes(
   *   coll: CollectionMetaCommands, is: Seq[Index.Default])(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   Future.sequence(
   *     is.map(idx => coll.indexesManager.ensure(idx))).map(_ => {})
   * }}}
   *
   * _Warning_: given the options you choose, and the data to index,
   * it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param index $indexToCreate
   *
   * @return true if the index was created, false if it already exists.
   */
  def ensure(index: Index): Future[Boolean]

  /**
   * $createDescription.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.CollectionMetaCommands
   * import reactivemongo.api.indexes.Index
   *
   * def createIndexes(
   *   coll: CollectionMetaCommands, is: Seq[Index.Default])(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   Future.sequence(
   *     is.map(idx => coll.indexesManager.create(idx))).map(_ => {})
   * }}}
   *
   * _Warning_: given the options you choose, and the data to index,
   * it can be a long and blocking operation on the database.
   * You should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @param index $indexToCreate
   */
  def create(index: Index): Future[WriteResult]

  /**
   * Drops the given index on that collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.CollectionMetaCommands
   *
   * def dropIndex(coll: CollectionMetaCommands, name: String)(
   *   implicit ec: ExecutionContext): Future[Int] =
   *   coll.indexesManager.drop(name)
   * }}}
   *
   * @param indexName the name of the index to be dropped
   * @return $droppedCount
   */
  def drop(indexName: String): Future[Int]

  /**
   * Drops all the indexes on that collection.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.CollectionMetaCommands
   *
   * def dropAllIndexes(coll: CollectionMetaCommands)(
   *   implicit ec: ExecutionContext): Future[Int] =
   *   coll.indexesManager.dropAll()
   * }}}
   *
   * @return $droppedCount
   */
  def dropAll(): Future[Int]
}

// TODO: Remove
private sealed abstract class LegacyCollectionIndexesManager
  extends CollectionIndexesManager { self =>

  protected val pack: Pack

  //override type Index = reactivemongo.api.indexes.Index.Aux[Pack]

  protected def db: String

  protected def collectionName: String

  protected def legacy: LegacyIndexesManager { type Pack = self.Pack }

  implicit protected def ec: ExecutionContext

  lazy val fqName = db + "." + collectionName

  def list(): Future[List[Index]] =
    legacy.list.map(_.filter(_.namespace == fqName).map(_.idx))

  def ensure(index: Index): Future[Boolean] =
    legacy.ensure(NSIndex.at[Pack](fqName, index))

  def create(index: Index): Future[WriteResult] =
    legacy.create(NSIndex.at[Pack](fqName, index))

  def drop(indexName: String): Future[Int] =
    legacy.drop(collectionName, indexName)

  def dropAll(): Future[Int] = legacy.dropAll(collectionName)
}

private class DefaultCollectionIndexesManager(
  db: DB,
  collectionName: String)(
  implicit
  ec: ExecutionContext) extends CollectionIndexesManager {

  import reactivemongo.api.commands.{
    CreateIndexes,
    Command,
    ListIndexes
  }

  import Serialization.{ internalSerializationPack, writeResultReader }

  private[api] lazy val collection = db(collectionName)

  type Pack = Serialization.Pack
  protected val pack: Pack = internalSerializationPack

  private lazy val listCommand = new ListIndexes.Command[Pack](db.name)

  private lazy val runner = Command.run[Pack](pack, db.failoverStrategy)

  private implicit lazy val listWriter = ListIndexes.writer[Pack](pack)

  private implicit lazy val indexReader: pack.Reader[Index] =
    IndexesManager.indexReader[Pack](pack)

  private implicit lazy val listReader: pack.Reader[List[Index]] =
    ListIndexes.reader[Pack](pack)

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

  private implicit val createWriter = CreateIndexes.writer[Pack](pack)

  def create(index: Index): Future[WriteResult] =
    runner(
      collection,
      new CreateIndexes.Command[Pack](db.name, List(index)),
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
  private[api] type Aux[P <: SerializationPack] = CollectionIndexesManager { type Pack = P }

  /**
   * Returns an indexes manager for specified collection.
   *
   * @param db the database
   * @param collectionName the collection name
   */
  def apply(db: DB, collectionName: String)(implicit ec: ExecutionContext): CollectionIndexesManager.Aux[Serialization.Pack] = {
    val wireVer = db.connectionState.metadata.maxWireVersion

    if (wireVer >= MongoWireVersion.V30) {
      new DefaultCollectionIndexesManager(db, collectionName)
    } else {
      new LegacyIndexesManager(db)(ec).onCollection(collectionName)
    }
  }
}

object IndexesManager {
  type Aux[P <: SerializationPack] = IndexesManager { type Pack = P }

  /**
   * Returns an indexes manager for specified database.
   *
   * @param db the database
   */
  def apply(db: DB with DBMetaCommands)(implicit ec: ExecutionContext): IndexesManager.Aux[Serialization.Pack] = {
    val wireVer = db.connectionState.metadata.maxWireVersion

    if (wireVer >= MongoWireVersion.V30) new DefaultIndexesManager(db)
    else new LegacyIndexesManager(db)
  }

  /**
   * Returns an indexes manager for specified database.
   *
   * @param pack the serialization pack
   * @param db the database
   */
  def apply[P <: SerializationPack with Singleton](pack: P, db: DB with DBMetaCommands)(implicit ec: ExecutionContext): IndexesManager.Aux[P] = {
    val wireVer = db.connectionState.metadata.maxWireVersion
    @inline def p: P = pack

    if (wireVer >= MongoWireVersion.V30) {
      new AbstractIndexesManager(db) {
        override type Pack = P
        override val pack: P = p
      }
    } else new AbstractLegacyManager(db) {
      override type Pack = P
      override val pack: P = p
    }
  }

  @deprecated("Internal: will be made private", "0.19.0")
  object NSIndexWriter extends reactivemongo.bson.BSONDocumentWriter[NSIndex.Aux[LegacyPack.type]] {

    private val underlying = nsIndexWriter(LegacyPack)

    def write(nsIndex: NSIndex.Aux[LegacyPack.type]): reactivemongo.bson.BSONDocument = underlying.write(nsIndex)

  }

  private[reactivemongo] def nsIndexWriter[P <: SerializationPack](pack: P): pack.Writer[NSIndex.Aux[P]] = {
    val builder = pack.newBuilder
    val decoder = pack.newDecoder
    val writeIndexType = IndexType.write[P](pack)(builder)
    val writeCollation = Collation.serializeWith(pack, _: Collation)(builder)

    import builder.{ boolean, document, elementProducer => element, string }

    pack.writer[NSIndex.Aux[P]] { nsIndex =>
      //import nsIndex.{ idx => index }

      @inline def index: Index.Aux[P] = nsIndex.idx //index

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

      index.expireAfterSeconds.foreach { sec =>
        elements += element("expireAfterSeconds", builder.int(sec))
      }

      index.storageEngine.foreach {
        case pack.IsDocument(conf) =>
          elements += element("storageEngine", conf)

        case _ => ()
      }

      index.weights.foreach {
        case pack.IsDocument(w) =>
          elements += element("weights", w)

        case _ => ()
      }

      index.defaultLanguage.foreach { lang =>
        elements += element("default_language", builder.string(lang))
      }

      index.languageOverride.foreach { lang =>
        elements += element("language_override", builder.string(lang))
      }

      index.textIndexVersion.foreach { ver =>
        elements += element("textIndexVersion", builder.int(ver))
      }

      index._2dsphereIndexVersion.foreach { ver =>
        elements += element("2dsphereIndexVersion", builder.int(ver))
      }

      index.bits.foreach { bits =>
        elements += element("bits", builder.int(bits))
      }

      index.min.foreach { min =>
        elements += element("min", builder.double(min))
      }

      index.max.foreach { max =>
        elements += element("max", builder.double(max))
      }

      index.bucketSize.foreach { size =>
        elements += element("bucketSize", builder.double(size))
      }

      index.collation.foreach { collation =>
        elements += element("collation", writeCollation(collation))
      }

      index.wildcardProjection.foreach {
        case pack.IsDocument(projection) =>
          elements += element("wildcardProjection", projection)

        case _ => ()
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

  @deprecated("Internal: will be made private", "0.19.0")
  object IndexReader extends reactivemongo.bson.BSONDocumentReader[Index] {
    private val underlying =
      indexReader(reactivemongo.api.BSONSerializationPack)

    def read(doc: reactivemongo.bson.BSONDocument): Index = underlying.read(doc)
  }

  private[reactivemongo] def indexReader[P <: SerializationPack](pack: P): pack.Reader[Index.Aux[P]] = {
    val decoder = pack.newDecoder
    val builder = pack.newBuilder
    val readCollation = Collation.read[P](pack)

    import decoder.{ booleanLike, child, double, int, string }

    pack.reader[Index.Aux[P]] { doc =>
      child(doc, "key").fold[Index.Aux[P]](
        throw new Exception("the key must be defined")) { k =>
          val ks = decoder.names(k).flatMap { nme =>
            decoder.get(k, nme).map { v =>
              nme -> IndexType(pack.bsonValue(v))
            }
          }

          val key = child(doc, "weights").fold(ks) { w =>
            val fields = decoder.names(w)

            (ks, fields).zipped.map {
              case ((_, tpe), name) => name -> tpe
            }
          }.toSeq

          val name = string(doc, "name")
          val unique = booleanLike(doc, "unique").getOrElse(false)
          val background = booleanLike(doc, "background").getOrElse(false)
          val dropDups = booleanLike(doc, "dropDups").getOrElse(false)
          val sparse = booleanLike(doc, "sparse").getOrElse(false)
          val expireAfterSeconds = int(doc, "expireAfterSeconds")
          val storageEngine = child(doc, "storageEngine")
          val weights = child(doc, "weights")
          val defaultLanguage = string(doc, "default_language")
          val languageOverride = string(doc, "language_override")
          val textIndexVersion = int(doc, "textIndexVersion")
          val sphereIndexVersion = int(doc, "2dsphereIndexVersion")
          val bits = int(doc, "bits")
          val min = double(doc, "min")
          val max = double(doc, "max")
          val bucketSize = double(doc, "bucketSize")
          val wildcardProjection = child(doc, "wildcardProjection")
          val collation = child(doc, "collation").flatMap(readCollation)
          val version = int(doc, "v")

          val options = builder.document(decoder.names(doc).flatMap {
            case "ns" | "key" | "name" | "unique" | "background" |
              "dropDups" | "sparse" | "v" | "partialFilterExpression" |
              "expireAfterSeconds" | "storageEngine" | "weights" |
              "defaultLanguage" | "languageOverride" | "textIndexVersion" |
              "2dsphereIndexVersion" | "bits" | "min" | "max" |
              "bucketSize" | "collation" | "wildcardProjection" =>
              Seq.empty[pack.ElementProducer]

            case nme =>
              decoder.get(doc, nme).map { v =>
                builder.elementProducer(nme, v)
              }
          }.toSeq)

          val partialFilter =
            child(doc, "partialFilterExpression")

          Index[P](pack)(key, name, unique, background, dropDups,
            sparse, expireAfterSeconds, storageEngine, weights,
            defaultLanguage, languageOverride, textIndexVersion,
            sphereIndexVersion, bits, min, max, bucketSize, collation,
            wildcardProjection, version, partialFilter, options)
        }
    }
  }

  @deprecated("Internal: will be made private", "0.19.0")
  object NSIndexReader extends reactivemongo.bson.BSONDocumentReader[NSIndex] {
    private val underlying =
      nsIndexReader(reactivemongo.api.BSONSerializationPack)

    def read(doc: reactivemongo.bson.BSONDocument): NSIndex =
      underlying.read(doc)
  }

  private[reactivemongo] def nsIndexReader[P <: SerializationPack](pack: P): pack.Reader[NSIndex.Aux[P]] = {
    val decoder = pack.newDecoder
    val indexReader: pack.Reader[Index.Aux[P]] = this.indexReader[P](pack)

    pack.reader[NSIndex.Aux[P]] { doc =>
      decoder.string(doc, "ns").fold[NSIndex.Aux[P]](
        throw new Exception("the namespace ns must be defined")) { ns =>
          NSIndex.at[P](ns, pack.deserialize(doc, indexReader))
        }
    }
  }

  private[api] implicit lazy val nsIndexReader =
    nsIndexReader[Serialization.Pack](Serialization.internalSerializationPack)

  private[api] lazy val dropWriter = // TODO: Remove
    DropIndexes.writer(Serialization.internalSerializationPack)

  private[api] lazy val dropReader = // TODO: Remove
    DropIndexes.reader(Serialization.internalSerializationPack)

}
