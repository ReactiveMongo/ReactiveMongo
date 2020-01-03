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
package reactivemongo.api.collections

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.{ Query, QueryFlags, MongoWireVersion }
import reactivemongo.core.netty.{ BufferSequence, ChannelBufferWritableBuffer }

import reactivemongo.api.{
  Collation,
  Collection,
  Cursor,
  CursorProducer,
  DefaultCursor,
  FailoverStrategy,
  QueryOpts,
  QueryOps,
  ReadConcern,
  ReadPreference,
  SerializationPack
}

import reactivemongo.api.commands.CommandCodecs

/**
 * A builder that helps to make a fine-tuned query to MongoDB.
 *
 * When the query is ready, you can call `cursor` to get a [[Cursor]], or `one` if you want to retrieve just one document.
 *
 * {{{
 * import scala.concurrent.{ ExecutionContext, Future }
 *
 * import reactivemongo.api.bson.BSONDocument
 * import reactivemongo.api.bson.collection.BSONCollection
 *
 * def firstFirst(coll: BSONCollection)(
 *   implicit ec: ExecutionContext): Future[Option[BSONDocument]] = {
 *   val queryBuilder = coll.find(BSONDocument.empty)
 *   queryBuilder.one[BSONDocument]
 * }
 * }}}
 *
 * @define oneFunction Sends this query and gets a future `Option[T]` (alias for [[reactivemongo.api.Cursor.headOption]])
 * @define readPrefParam The [[reactivemongo.api.ReadPreference]] for this query. If the `ReadPreference` implies that this query can be run on a secondary, the slaveOk flag will be set.
 * @define readerParam the reader for the results type
 * @define resultTParam the results type
 * @define requireOneFunction Sends this query and gets a future `T` (alias for [[reactivemongo.api.Cursor.head]])
 * @define filterFunction Sets the query predicate; If unspecified, then all documents in the collection will match the predicate
 * @define projectionFunction Sets the [[https://docs.mongodb.com/manual/tutorial/project-fields-from-query-results/#projections projection specification]] to determine which fields to include in the returned documents
 */
@deprecated("Internal: will be made private", "0.16.0")
trait GenericQueryBuilder[P <: SerializationPack] extends QueryOps {
  val pack: P

  type Self <: GenericQueryBuilder[pack.type]

  def queryOption: Option[pack.Document]
  def sortOption: Option[pack.Document]
  def projectionOption: Option[pack.Document]
  def hintOption: Option[pack.Document]

  def explainFlag: Boolean
  def snapshotFlag: Boolean
  def commentString: Option[String]
  def options: QueryOpts

  @deprecatedName(Symbol("failover")) def failoverStrategy: FailoverStrategy

  def collection: Collection = ???

  def maxTimeMsOption: Option[Long]

  // TODO#1.1: Remove
  private var _readConcern: ReadConcern = ReadConcern.default
  private var _singleBatch: Boolean = false
  private var _maxScan = Option.empty[Double]
  private var _returnKey: Boolean = false
  private var _showRecordId: Boolean = false
  private var _min = Option.empty[pack.Document]
  private var _max = Option.empty[pack.Document]
  private var _collation = Option.empty[Collation]

  /**
   * The flag to determines whether to close the cursor after the first batch
   * (default: `false`)
   */
  @inline def singleBatch: Boolean = _singleBatch

  /**
   * This option specifies a maximum number of documents
   * or index keys the query plan will scan.
   */
  @inline def maxScan: Option[Double] = _maxScan

  /**
   * If this flag is true, returns only the index keys
   * in the resulting documents.
   */
  @inline def returnKey: Boolean = _returnKey

  /**
   * The flags to determines whether to return
   * the record identifier for each document.
   */
  @inline def showRecordId: Boolean = _showRecordId

  /**
   * The read concern
   * @since MongoDB 3.6
   */
  @inline def readConcern: ReadConcern = _readConcern

  /**
   * The optional exclusive [[https://docs.mongodb.com/v3.4/reference/method/cursor.max/#cursor.max upper bound]] for a specific index (default: `None`).
   */
  @inline def max: Option[pack.Document] = _max

  /**
   * The optional exclusive [[https://docs.mongodb.com/v3.4/reference/method/cursor.min/#cursor.min lower bound]] for a specific index (default: `None`).
   */
  @inline def min: Option[pack.Document] = _min

  /**
   * The optional collation to use for the find command (default: `None`).
   *
   * @since MongoDB 3.4
   */
  @inline def collation: Option[Collation] = _collation

  /**
   * Returns a [[Cursor]] for the result of this query.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.Cursor
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def findAllVisible(coll: BSONCollection)(
   *   implicit ec: ExecutionContext): Future[List[BSONDocument]] =
   *   coll.find(BSONDocument("visible" -> true)).
   *     cursor[BSONDocument]().collect[List](
   *      maxDocs = 10,
   *      err = Cursor.FailOnError[List[BSONDocument]]())
   * }}}
   *
   * @param readPreference $readPrefParam
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def cursor[T](readPreference: ReadPreference = readPreference, isMongo26WriteOp: Boolean = false)(implicit reader: pack.Reader[T], cp: CursorProducer[T]): cp.ProducedCursor = cp.produce(defaultCursor[T](readPreference, isMongo26WriteOp))

  /** The default [[ReadPreference]] */
  @deprecated("Internal: will be made private", "0.16.0")
  @inline def readPreference: ReadPreference = ReadPreference.primary

  protected def version: MongoWireVersion

  /**
   * $oneFunction (using the default [[reactivemongo.api.ReadPreference]]).
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.bson.{ BSONDocument, Macros }
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * case class User(name: String, pass: String)
   *
   * implicit val handler = Macros.reader[User]
   *
   * def findUser(coll: BSONCollection, name: String)(
   *   implicit ec: ExecutionContext): Future[Option[User]] =
   *   coll.find(BSONDocument("user" -> name)).one[User]
   * }}}
   *
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def one[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = one(this.readPreference)

  /**
   * $oneFunction.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.ReadPreference
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def findUser(coll: BSONCollection, name: String)(
   *   implicit ec: ExecutionContext): Future[Option[BSONDocument]] =
   *   coll.find(BSONDocument("user" -> name)).
   *     one[BSONDocument](ReadPreference.primaryPreferred)
   * }}}
   *
   * @param readPreference $readPrefParam
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = copy(options = options.batchSize(1)).defaultCursor(readPreference)(reader).headOption

  /**
   * $requireOneFunction
   * (using the default [[reactivemongo.api.ReadPreference]]).
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def findUser(coll: BSONCollection, name: String)(
   *   implicit ec: ExecutionContext): Future[BSONDocument] =
   *   coll.find(BSONDocument("user" -> name)).requireOne[BSONDocument]
   * }}}
   *
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def requireOne[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = requireOne(readPreference)

  /**
   * $requireOneFunction.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.ReadPreference
   * import reactivemongo.api.bson.{ BSONDocument, Macros }
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * case class User(name: String, pass: String)
   *
   * implicit val handler = Macros.handler[User]
   *
   * def findUser(coll: BSONCollection, name: String)(
   *   implicit ec: ExecutionContext): Future[User] =
   *   coll.find(BSONDocument("user" -> name)).
   *     requireOne[User](ReadPreference.primaryPreferred)
   * }}}
   *
   * @param readPreference $readPrefParam
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def requireOne[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = copy(options = options.batchSize(1)).defaultCursor(readPreference)(reader).head

  @deprecated("Use `filter`", "0.18.2")
  def query[Qry](selector: Qry)(implicit writer: pack.Writer[Qry]): Self =
    copy(queryOption = Some(pack.serialize(selector, writer)))

  @deprecated("Use `filter`", "0.18.2")
  def query(selector: pack.Document): Self = copy(queryOption = Some(selector))

  /**
   * $filterFunction.
   *
   * @tparam Qry The type of the query. An implicit `Writer[Qry]` typeclass for handling it has to be in the scope.
   */
  @deprecated(
    "Specify the filter predicate using `collection.find(..)`", "0.19.4")
  def filter[Qry](predicate: Qry)(implicit writer: pack.Writer[Qry]): Self =
    copy(queryOption = Some(pack.serialize(predicate, writer)))

  /**
   * $filterFunction.
   */
  @deprecated(
    "Specify the filter predicate using `collection.find(..)`", "0.19.4")
  def filter(predicate: pack.Document): Self =
    copy(queryOption = Some(predicate))

  /**
   * Sets the sort specification for the ordering of the results.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.Cursor
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def findSortedVisible(coll: BSONCollection)(
   *   implicit ec: ExecutionContext): Future[List[BSONDocument]] =
   *   coll.find(BSONDocument("visible" -> true)).
   *     sort(BSONDocument("age" -> 1)). // sort per age
   *     cursor[BSONDocument]().
   *     collect[List](
   *       maxDocs = 100,
   *       err = Cursor.FailOnError[List[BSONDocument]]())
   * }}}
   */
  def sort(document: pack.Document): Self = copy(sortOption = Some(document))

  /**
   * $projectionFunction.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.Cursor
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def findAllWithProjection(coll: BSONCollection)(
   *   implicit ec: ExecutionContext): Future[List[BSONDocument]] =
   *   coll.find(BSONDocument.empty).
   *     projection(BSONDocument("age" -> 1)). // only consider 'age' field
   *     cursor[BSONDocument]().
   *     collect[List](
   *       maxDocs = 100,
   *       err = Cursor.FailOnError[List[BSONDocument]]())
   * }}}
   *
   * @tparam Pjn The type of the projection. An implicit `Writer[Pjn]` typeclass for handling it has to be in the scope.
   */
  def projection[Pjn](p: Pjn)(implicit writer: pack.Writer[Pjn]): Self =
    copy(projectionOption = Some(pack.serialize(p, writer)))

  /**
   * $projectionFunction.
   */
  def projection(p: pack.Document): Self = copy(projectionOption = Some(p))

  /** Sets the [[https://docs.mongodb.com/manual/reference/operator/meta/hint/ hint document]] (a document that declares the index MongoDB should use for this query). */
  def hint(document: pack.Document): Self = copy(hintOption = Some(document))

  /** Toggles [[https://docs.mongodb.org/manual/reference/method/cursor.explain/#cursor.explain explain mode]]. */
  def explain(flag: Boolean = true): Self = copy(explainFlag = flag)

  /** Toggles [[https://docs.mongodb.org/manual/faq/developers/#faq-developers-isolate-cursors snapshot mode]]. */
  def snapshot(flag: Boolean = true): Self = copy(snapshotFlag = flag)

  /** Adds a comment to this query, that may appear in the MongoDB logs. */
  def comment(message: String): Self = copy(commentString = Some(message))

  /** Adds [[https://docs.mongodb.org/v3.0/reference/operator/meta/maxTimeMS/ maxTimeMs]] to query  */
  def maxTimeMs(p: Long): Self = copy(maxTimeMsOption = Some(p))

  def options(options: QueryOpts): Self = copy(options = options)

  /** Sets the [[ReadConcern]]. */
  def readConcern(concern: ReadConcern): Self = {
    val upd = copy()
    upd._readConcern = concern
    upd
  }

  /** Sets the `singleBatch` flag. */
  def singleBatch(flag: Boolean): Self = {
    val upd = copy()
    upd._singleBatch = flag
    upd
  }

  /** Sets the `maxScan` flag. */
  def maxScan(max: Double): Self = {
    val upd = copy()
    upd._maxScan = Some(max)
    upd
  }

  /** Sets the `returnKey` flag. */
  def returnKey(flag: Boolean): Self = {
    val upd = copy()
    upd._returnKey = flag
    upd
  }

  /** Sets the `showRecordId` flag. */
  def showRecordId(flag: Boolean): Self = {
    val upd = copy()
    upd._showRecordId = flag
    upd
  }

  /** Sets the `max` document. */
  def max(document: pack.Document): Self = {
    val upd = copy()
    upd._max = Option(document)
    upd
  }

  /** Sets the `min` document. */
  def min(document: pack.Document): Self = {
    val upd = copy()
    upd._min = Option(document)
    upd
  }

  /**
   * Sets the `collation` document.
   * @since MongoDB 3.4
   */
  def collation(collation: Collation): Self = {
    val upd = copy()
    upd._collation = Option(collation)
    upd
  }

  @deprecated("Use `options` or the separate query ops", "0.12.4")
  def updateOptions(update: QueryOpts => QueryOpts): Self =
    copy(options = update(options))

  // QueryOps
  def awaitData = options(options.awaitData)
  def batchSize(n: Int) = options(options.batchSize(n))
  def exhaust = options(options.exhaust)
  def noCursorTimeout = options(options.noCursorTimeout)
  def oplogReplay = options(options.oplogReplay)

  @deprecated("Use `allowPartialResults`", "0.19.8")
  def partial = options(options.partial)

  /**
   * For queries against a sharded collection,
   * returns partial results from the mongos if some shards are unavailable
   * instead of throwing an error.
   */
  def allowPartialResults = options(options.partial)

  /**
   * Sets the number of documents to skip at the beginning of the results.
   *
   * @see [[QueryOpts.skipN]]
   */
  def skip(n: Int) = options(options.skip(n))

  def slaveOk = options(options.slaveOk)
  def tailable = options(options.tailable)

  @deprecated("Internal: will be made private", "0.16.0")
  def copy(
    queryOption: Option[pack.Document] = queryOption,
    sortOption: Option[pack.Document] = sortOption,
    projectionOption: Option[pack.Document] = projectionOption,
    hintOption: Option[pack.Document] = hintOption,
    explainFlag: Boolean = explainFlag,
    snapshotFlag: Boolean = snapshotFlag,
    commentString: Option[String] = commentString,
    options: QueryOpts = options,
    @deprecatedName(Symbol("failover")) failoverStrategy: FailoverStrategy = failoverStrategy,
    maxTimeMsOption: Option[Long] = maxTimeMsOption): Self

  // ---

  lazy val builder = pack.newBuilder

  import builder.{
    boolean,
    document,
    elementProducer => element,
    int,
    long,
    string
  }

  private lazy val writeReadPref = QueryCodecs.writeReadPref[pack.type](builder)

  private val mergeLt32: Function2[ReadPreference, Int, pack.Document] =
    { (readPreference: ReadPreference, _: Int) =>
      val elements = Seq.newBuilder[pack.ElementProducer]

      // Primary and SecondaryPreferred are encoded as the slaveOk flag;
      // the others are encoded as $readPreference field.

      queryOption.foreach {
        elements += element(f"$$query", _)
      }

      sortOption.foreach {
        elements += element(f"$$orderby", _)
      }

      hintOption.foreach {
        elements += element(f"$$hint", _)
      }

      maxTimeMsOption.foreach { l =>
        elements += element(f"$$maxTimeMS", long(l))
      }

      commentString.foreach { c =>
        elements += element(f"$$comment", string(c))
      }

      if (explainFlag) {
        elements += element(f"$$explain", boolean(true))
      }

      if (snapshotFlag) {
        elements += element(f"$$snapshot", boolean(true))
      }

      elements += element(f"$$readPreference", writeReadPref(readPreference))

      val merged = document(elements.result())

      logger.trace(s"command: ${pack pretty merged}")

      merged
    }

  private val merge32: Function2[ReadPreference, Int, pack.Document] = {
    val writeCollation = Collation.serializeWith[pack.type](
      pack, _: Collation)(builder)

    { (readPreference, maxDocs) =>
      import QueryFlags.{
        AwaitData,
        OplogReplay,
        Partial,
        NoCursorTimeout,
        TailableCursor
      }

      def partial: Boolean = (options.flagsN & Partial) == Partial
      def awaitData: Boolean = (options.flagsN & AwaitData) == AwaitData
      def oplogReplay: Boolean = (options.flagsN & OplogReplay) == OplogReplay
      def noTimeout: Boolean =
        (options.flagsN & NoCursorTimeout) == NoCursorTimeout

      def tailable: Boolean =
        (options.flagsN & TailableCursor) == TailableCursor

      lazy val limit: Option[Int] = {
        if (maxDocs > 0 && maxDocs < Int.MaxValue) Some(maxDocs)
        else Option.empty[Int]
      }

      def batchSize: Option[Int] = {
        val sz = limit.fold(options.batchSizeN)(options.batchSizeN.min)

        if (sz > 0 && sz < Int.MaxValue) Some(sz)
        else Option.empty[Int]
      }

      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("find", string(collection.name)),
        element("skip", int(options.skipN)),
        element("tailable", boolean(tailable)),
        element("awaitData", boolean(awaitData)),
        element("oplogReplay", boolean(oplogReplay)),
        element("noCursorTimeout", boolean(noTimeout)),
        element("allowPartialResults", boolean(partial)),
        element("singleBatch", boolean(singleBatch)),
        element("returnKey", boolean(returnKey)),
        element("showRecordId", boolean(showRecordId)))

      if (version.compareTo(MongoWireVersion.V34) < 0) {
        elements += element("snapshot", boolean(snapshotFlag))
      }

      maxScan.foreach { max =>
        elements += element("maxScan", builder.double(max))
      }

      queryOption.foreach {
        elements += element("filter", _)
      }

      sortOption.foreach {
        elements += element("sort", _)
      }

      projectionOption.foreach {
        elements += element("projection", _)
      }

      hintOption.foreach {
        elements += element("hint", _)
      }

      batchSize.foreach { i =>
        elements += element("batchSize", int(i))
      }

      limit.foreach { i =>
        elements += element("limit", int(i))
      }

      commentString.foreach { c =>
        elements += element("comment", string(c))
      }

      maxTimeMsOption.foreach { l =>
        elements += element("maxTimeMS", long(l))
      }

      max.foreach { doc =>
        elements += element("max", doc)
      }

      min.foreach { doc =>
        elements += element("min", doc)
      }

      collation.foreach { c =>
        elements += element("collation", writeCollation(c))
      }

      val session = collection.db.session.filter( // TODO#1.1: Remove
        _ => (version.compareTo(MongoWireVersion.V36) >= 0))

      elements ++= CommandCodecs.writeSessionReadConcern(
        builder)(session)(readConcern)

      val readPref = element(f"$$readPreference", writeReadPref(readPreference))

      val merged = if (!explainFlag) {
        elements += readPref

        document(elements.result())
      } else {
        document(Seq[pack.ElementProducer](
          element("explain", document(elements.result())),
          readPref))
      }

      logger.trace(s"command: ${pack pretty merged}")

      merged
    }
  }

  private[reactivemongo] lazy val merge: Function2[ReadPreference, Int, pack.Document] = if (version.compareTo(MongoWireVersion.V32) < 0) mergeLt32 else merge32

  private def defaultCursor[T](
    readPreference: ReadPreference,
    isMongo26WriteOp: Boolean = false)(
    implicit
    reader: pack.Reader[T]): Cursor.WithOps[T] = {

    val body = {
      if (version.compareTo(MongoWireVersion.V32) < 0) { _: Int =>
        val buffer = write(
          merge(readPreference, Int.MaxValue),
          ChannelBufferWritableBuffer())

        BufferSequence(
          projectionOption.fold(buffer) { write(_, buffer) }.buffer)

      } else { maxDocs: Int =>
        // if MongoDB 3.2+, projection is managed in merge
        def prepared = write(
          merge(readPreference, maxDocs),
          ChannelBufferWritableBuffer())

        BufferSequence(prepared.buffer)
      }
    }

    val flags = {
      if (readPreference.slaveOk) options.flagsN | QueryFlags.SlaveOk
      else options.flagsN
    }

    val name = {
      if (version.compareTo(MongoWireVersion.V32) < 0) {
        collection.fullCollectionName
      } else {
        collection.db.name + f".$$cmd" // Command 'find' for 3.2+
      }
    }

    val op = Query(flags, name, options.skipN, options.batchSizeN)

    DefaultCursor.query(pack, op, body, readPreference,
      collection.db, failoverStrategy, isMongo26WriteOp,
      collection.fullCollectionName, maxTimeMsOption)(reader)
  }

  private def write(document: pack.Document, buffer: ChannelBufferWritableBuffer): ChannelBufferWritableBuffer = {
    pack.writeToBuffer(buffer, document)
    buffer
  }

  private lazy val logger = reactivemongo.util.LazyLogger(getClass.getName)
}
