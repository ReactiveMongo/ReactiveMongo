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
 * @define oneFunction Sends this query and gets a future `Option[T]` (alias for [[reactivemongo.api.Cursor.headOption]])
 * @define readPrefParam The [[reactivemongo.api.ReadPreference]] for this query. If the `ReadPreference` implies that this query can be run on a secondary, the slaveOk flag will be set.
 * @define readerParam the reader for the results type
 * @define resultTParam the results type
 * @define requireOneFunction Sends this query and gets a future `T` (alias for [[reactivemongo.api.Cursor.head]])
 */
@deprecated("Will be private/internal", "0.16.0")
trait GenericQueryBuilder[P <: SerializationPack] extends QueryOps {
  // TODO: Unit test?

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

  @deprecatedName('failover) def failoverStrategy: FailoverStrategy

  def collection: Collection
  def maxTimeMsOption: Option[Long]

  /** The read concern (since 3.2) */
  def readConcern: ReadConcern = ReadConcern.default // TODO: Remove body

  /* TODO: https://docs.mongodb.com/v3.2/reference/command/find/#dbcmd.find

   - singleBatch: boolean; Optional. Determines whether to close the cursor after the first batch. Defaults to false.
   - maxScan: boolean; Optional. Maximum number of documents or index keys to scan when executing the query.
   - max: document; Optional. The exclusive upper bound for a specific index; https://docs.mongodb.com/v3.4/reference/method/cursor.max/#cursor.max
   - min: document; Optional. The exclusive upper bound for a specific index; https://docs.mongodb.com/v3.4/reference/method/cursor.min/#cursor.min
   - returnKey: boolean; Optional. If true, returns only the index keys in the resulting documents.
   - showRecordId: boolean; Optional. Determines whether to return the record identifier for each document.
- noCursorTimeout: boolean; Optional. Prevents the server from timing out idle cursors after an inactivity period (10 minutes).
   - allowPartialResults: boolean; Optional. For queries against a sharded collection, returns partial results from the mongos if some shards are unavailable instead of throwing an error.
   - collation: document; Optional; Specifies the collation to use for the operation.
   */

  /**
   * Makes a [[Cursor]] of this query, which can be enumerated.
   *
   * @param readPreference $readPrefParam
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def cursor[T](readPreference: ReadPreference = readPreference, isMongo26WriteOp: Boolean = false)(implicit reader: pack.Reader[T], cp: CursorProducer[T]): cp.ProducedCursor = cp.produce(defaultCursor[T](readPreference, isMongo26WriteOp))

  /** The default [[ReadPreference]] */
  @deprecated("Will be private/internal", "0.16.0")
  @inline def readPreference: ReadPreference = ReadPreference.primary

  protected lazy val version =
    collection.db.connectionState.metadata.maxWireVersion

  /**
   * $oneFunction.
   *
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def one[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = one(readPreference)

  /**
   * $oneFunction.
   *
   * @param readPreference $readPrefParam
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = copy(options = options.batchSize(1)).defaultCursor(readPreference)(reader).headOption

  /**
   * $requireOneFunction.
   *
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def requireOne[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = requireOne(readPreference)

  /**
   * $requireOneFunction.
   *
   * @param readPreference $readPrefParam
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def requireOne[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = copy(options = options.batchSize(1)).defaultCursor(readPreference)(reader).head

  /**
   * Sets the selector document.
   *
   * @tparam Qry The type of the query. An implicit `Writer[Qry]` typeclass for handling it has to be in the scope.
   */
  def query[Qry](selector: Qry)(implicit writer: pack.Writer[Qry]): Self =
    copy(queryOption = Some(pack.serialize(selector, writer)))

  /** Sets the query (the selector document). */
  def query(selector: pack.Document): Self = copy(queryOption = Some(selector))

  /** Sets the sorting document. */
  def sort(document: pack.Document): Self = copy(sortOption = Some(document))

  /**
   * Sets the projection document (for [[http://docs.mongodb.org/manual/core/read-operations-introduction/ retrieving only a subset of fields]]).
   *
   * @tparam Pjn The type of the projection. An implicit `Writer[Pjn]` typeclass for handling it has to be in the scope.
   */
  def projection[Pjn](p: Pjn)(implicit writer: pack.Writer[Pjn]): Self =
    copy(projectionOption = Some(pack.serialize(p, writer)))

  /**
   * Sets the projection document (for [[http://docs.mongodb.org/manual/core/read-operations-introduction/ retrieving only a subset of fields]]).
   */
  def projection(p: pack.Document): Self = copy(projectionOption = Some(p))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  def hint(document: pack.Document): Self = copy(hintOption = Some(document))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  // TODO def hint(indexName: String): Self = copy(hintOption = Some(BSONDocument(indexName -> BSONInteger(1))))

  /** Toggles [[https://docs.mongodb.org/manual/reference/method/cursor.explain/#cursor.explain explain mode]]. */
  def explain(flag: Boolean = true): Self = copy(explainFlag = flag)

  /** Toggles [[https://docs.mongodb.org/manual/faq/developers/#faq-developers-isolate-cursors snapshot mode]]. */
  def snapshot(flag: Boolean = true): Self = copy(snapshotFlag = flag)

  /** Adds a comment to this query, that may appear in the MongoDB logs. */
  def comment(message: String): Self = copy(commentString = Some(message))

  /** Adds maxTimeMs to query https://docs.mongodb.org/v3.0/reference/operator/meta/maxTimeMS/ */
  def maxTimeMs(p: Long): Self = copy(maxTimeMsOption = Some(p))

  def options(options: QueryOpts): Self = copy(options = options)

  @deprecated("Use `options` or the separate query ops", "0.12.4")
  def updateOptions(update: QueryOpts => QueryOpts): Self =
    copy(options = update(options))

  // QueryOps
  def awaitData = options(options.awaitData)
  def batchSize(n: Int) = options(options.batchSize(n))
  def exhaust = options(options.exhaust)
  def noCursorTimeout = options(options.noCursorTimeout)
  def oplogReplay = options(options.oplogReplay)
  def partial = options(options.partial)
  def skip(n: Int) = options(options.skip(n))
  def slaveOk = options(options.slaveOk)
  def tailable = options(options.tailable)

  @deprecated("Will be private/internal", "0.16.0")
  def copy(
    queryOption: Option[pack.Document] = queryOption,
    sortOption: Option[pack.Document] = sortOption,
    projectionOption: Option[pack.Document] = projectionOption,
    hintOption: Option[pack.Document] = hintOption,
    explainFlag: Boolean = explainFlag,
    snapshotFlag: Boolean = snapshotFlag,
    commentString: Option[String] = commentString,
    options: QueryOpts = options,
    @deprecatedName('failover) failoverStrategy: FailoverStrategy = failoverStrategy,
    maxTimeMsOption: Option[Long] = maxTimeMsOption): Self

  // ---

  // TODO: Unit test (see merge test in ReactiveMongo-Play-Json)
  private[reactivemongo] def merge(readPreference: ReadPreference, maxDocs: Int): pack.Document = {
    val builder = pack.newBuilder

    import builder.{
      boolean,
      document,
      elementProducer => element,
      int,
      long,
      string
    }

    // Primary and SecondaryPreferred are encoded as the slaveOk flag;
    // the others are encoded as $readPreference field.

    val writeReadPref = QueryCodecs.writeReadPref[pack.type](builder)

    val merged = if (version.compareTo(MongoWireVersion.V32) < 0) {
      val elements = Seq.newBuilder[pack.ElementProducer]

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

      document(elements.result())
    } else {
      // TODO: singleBatch, maxScan, max, min, returnKey
      // showRecordId, noCursorTimeout, allowPartialResults, collation

      import QueryFlags.{ AwaitData, OplogReplay, TailableCursor }

      def awaitData: Boolean = (options.flagsN & AwaitData) == AwaitData
      def oplogReplay: Boolean = (options.flagsN & OplogReplay) == OplogReplay
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
        element("oplogReplay", boolean(oplogReplay)))

      if (version.compareTo(MongoWireVersion.V34) < 0) {
        elements += element("snapshot", boolean(snapshotFlag))
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

      val session = collection.db.session.filter( // TODO: Remove
        _ => (version.compareTo(MongoWireVersion.V36) >= 0))

      elements ++= CommandCodecs.writeSessionReadConcern(
        builder, session)(readConcern)

      val readPref = element(f"$$readPreference", writeReadPref(readPreference))

      if (!explainFlag) {
        elements += readPref

        document(elements.result())
      } else {
        document(Seq[pack.ElementProducer](
          element("explain", document(elements.result())),
          readPref))
      }
    }

    logger.debug(s"command: ${pack pretty merged}")

    merged
  }

  private def defaultCursor[T](
    readPreference: ReadPreference,
    isMongo26WriteOp: Boolean = false)(
    implicit
    reader: pack.Reader[T]): Cursor[T] = {

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
        collection.db.name + ".$cmd" // Command 'find' for 3.2+
      }
    }

    val op = Query(flags, name, options.skipN, options.batchSizeN)

    DefaultCursor.query(pack, op, body, readPreference,
      collection.db, failoverStrategy, isMongo26WriteOp,
      collection.fullCollectionName)(reader)
  }

  private def write(document: pack.Document, buffer: ChannelBufferWritableBuffer): ChannelBufferWritableBuffer = {
    pack.writeToBuffer(buffer, document)
    buffer
  }

  private lazy val logger = reactivemongo.util.LazyLogger(getClass.getName)
}
