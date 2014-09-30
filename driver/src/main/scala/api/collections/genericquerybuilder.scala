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
import reactivemongo.bson.buffer.ReadableBuffer
import reactivemongo.core.protocol.{ Query, QueryFlags }
import reactivemongo.core.netty.{ BufferSequence, ChannelBufferWritableBuffer }

/**
 * A builder that helps to make a fine-tuned query to MongoDB.
 *
 * When the query is ready, you can call `cursor` to get a [[Cursor]], or `one` if you want to retrieve just one document.
 *
 */
trait GenericQueryBuilder[P <: SerializationPack] {
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

  def merge: pack.Document

  //def structureReader: pack.Reader[pack.Document]

  def copy(
    queryOption: Option[pack.Document] = queryOption,
    sortOption: Option[pack.Document] = sortOption,
    projectionOption: Option[pack.Document] = projectionOption,
    hintOption: Option[pack.Document] = hintOption,
    explainFlag: Boolean = explainFlag,
    snapshotFlag: Boolean = snapshotFlag,
    commentString: Option[String] = commentString,
    options: QueryOpts = options,
    failover: FailoverStrategy = failover): Self

  private def write(document: pack.Document, buffer: ChannelBufferWritableBuffer = ChannelBufferWritableBuffer()): ChannelBufferWritableBuffer = {
    pack.writeToBuffer(buffer, document)
    buffer
  }

  /**
   * Sends this query and gets a [[Cursor]] of instances of `T`.
   *
   * An implicit `Reader[T]` must be present in the scope.
   */
  def cursor[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Cursor[T] = cursor(ReadPreference.primary)

  /**
   * Makes a [[Cursor]] of this query, which can be enumerated.
   *
   * An implicit `Reader[T]` must be present in the scope.
   *
   * @param readPreference The ReadPreference for this request. If the ReadPreference implies that this request might be run on a Secondary, the slaveOk flag will be set.
   */
  def cursor[T](readPreference: ReadPreference, isMongo26WriteOp: Boolean = false)(implicit reader: pack.Reader[T], ec: ExecutionContext): Cursor[T] = {
    val documents = BufferSequence {
      val buffer = write(merge, ChannelBufferWritableBuffer())
      projectionOption.map { projection =>
        write(projection, buffer)
      }.getOrElse(buffer).buffer
    }

    val flags = if (readPreference.slaveOk) options.flagsN | QueryFlags.SlaveOk else options.flagsN

    val op = Query(flags, collection.fullCollectionName, options.skipN, options.batchSizeN)

    DefaultCursor(pack, op, documents, readPreference, collection.db.connection, failover, isMongo26WriteOp)(reader)
  }

  /**
   * Sends this query and gets a future `Option[T]`.
   *
   * An implicit `Reader[T]` must be present in the scope.
   */
  def one[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = copy(options = options.batchSize(1)).cursor(reader, ec).headOption

  /**
   * Sends this query and gets a future `Option[T]`.
   *
   * An implicit `Reader[T]` must be present in the scope.
   *
   * @param readPreference The ReadPreference for this request. If the ReadPreference implies that this request might be run on a Secondary, the slaveOk flag will be set.
   */
  def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = copy(options = options.batchSize(1)).cursor(readPreference)(reader, ec).headOption

  /**
   * Sets the query (the selector document).
   *
   * @tparam Qry The type of the query. An implicit `Writer[Qry]` typeclass for handling it has to be in the scope.
   */
  def query[Qry](selector: Qry)(implicit writer: pack.Writer[Qry]): Self = copy(queryOption = Some(
    pack.serialize(selector, writer)))

  /** Sets the query (the selector document). */
  def query(selector: pack.Document): Self = copy(queryOption = Some(selector))

  /** Sets the sorting document. */
  def sort(document: pack.Document): Self = copy(sortOption = Some(document))

  def options(options: QueryOpts): Self = copy(options = options)

  /* TODO /** Sets the sorting document. */
  def sort(sorters: (String, SortOrder)*): Self = copy(sortDoc = {
    if (sorters.size == 0)
      None
    else {
      val bson = BSONDocument(
        (for (sorter <- sorters) yield sorter._1 -> BSONInteger(
          sorter._2 match {
            case SortOrder.Ascending => 1
            case SortOrder.Descending => -1
          })).toStream)
      Some(bson)
    }
  })*/

  /**
   * Sets the projection document (for [[http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields retrieving only a subset of fields]]).
   *
   * @tparam Pjn The type of the projection. An implicit `Writer][Pjn]` typeclass for handling it has to be in the scope.
   */
  def projection[Pjn](p: Pjn)(implicit writer: pack.Writer[Pjn]): Self = copy(projectionOption = Some(
    pack.serialize(p, writer)))

  def projection(p: pack.Document): Self = copy(projectionOption = Some(p))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  def hint(document: pack.Document): Self = copy(hintOption = Some(document))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  // TODO def hint(indexName: String): Self = copy(hintOption = Some(BSONDocument(indexName -> BSONInteger(1))))

  //TODO def explain(flag: Boolean = true) :QueryBuilder = copy(explainFlag=flag)

  /** Toggles [[http://www.mongodb.org/display/DOCS/How+to+do+Snapshotted+Queries+in+the+Mongo+Database snapshot mode]]. */
  def snapshot(flag: Boolean = true): Self = copy(snapshotFlag = flag)

  /** Adds a comment to this query, that may appear in the MongoDB logs. */
  def comment(message: String): Self = copy(commentString = Some(message))
}