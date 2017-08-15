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
import reactivemongo.api._
import reactivemongo.core.protocol.{ Query, QueryFlags }
import reactivemongo.core.netty.{ BufferSequence, ChannelBufferWritableBuffer }

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
  def failover: FailoverStrategy
  def collection: Collection
  def maxTimeMsOption: Option[Long]

  @deprecated("Will be removed from the public API", "0.12.0")
  def merge(readPreference: ReadPreference): pack.Document

  def copy(
    queryOption: Option[pack.Document] = queryOption,
    sortOption: Option[pack.Document] = sortOption,
    projectionOption: Option[pack.Document] = projectionOption,
    hintOption: Option[pack.Document] = hintOption,
    explainFlag: Boolean = explainFlag,
    snapshotFlag: Boolean = snapshotFlag,
    commentString: Option[String] = commentString,
    options: QueryOpts = options,
    failover: FailoverStrategy = failover,
    maxTimeMsOption: Option[Long] = maxTimeMsOption): Self

  private def write(document: pack.Document, buffer: ChannelBufferWritableBuffer): ChannelBufferWritableBuffer = {
    pack.writeToBuffer(buffer, document)
    buffer
  }

  /**
   * Sends this query and gets a [[Cursor]] of instances of `T`.
   */
  @deprecated("Use `cursor()` or `cursor(readPreference)`", "0.11.0")
  def cursor[T](implicit reader: pack.Reader[T], ec: ExecutionContext, cp: CursorProducer[T]): cp.ProducedCursor = cursor(ReadPreference.primary)

  /**
   * Makes a [[Cursor]] of this query, which can be enumerated.
   *
   * @param readPreference $readPrefParam
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def cursor[T](readPreference: ReadPreference = ReadPreference.primary, isMongo26WriteOp: Boolean = false)(implicit reader: pack.Reader[T], ec: ExecutionContext, cp: CursorProducer[T]): cp.ProducedCursor = cp.produce(defaultCursor[T](readPreference, isMongo26WriteOp))

  private def defaultCursor[T](readPreference: ReadPreference, isMongo26WriteOp: Boolean = false)(implicit reader: pack.Reader[T]): Cursor[T] = {
    val documents = BufferSequence {
      val buffer = write(merge(readPreference), ChannelBufferWritableBuffer())
      projectionOption.map { projection =>
        write(projection, buffer)
      }.getOrElse(buffer).buffer
    }

    val flags = if (readPreference.slaveOk) options.flagsN | QueryFlags.SlaveOk else options.flagsN

    val op = Query(flags, collection.fullCollectionName, options.skipN, options.batchSizeN)

    DefaultCursor.query(pack, op, documents, readPreference, collection.db.connection, failover, isMongo26WriteOp)(reader)
  }

  /**
   * $oneFunction.
   *
   * @param reader $readerParam
   *
   * @tparam T $resultTParam
   */
  def one[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = one(ReadPreference.primary)

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
  def requireOne[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = requireOne(ReadPreference.primary)

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

  @deprecated("Use [[options]] or the separate query ops", "0.12.4")
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
}
