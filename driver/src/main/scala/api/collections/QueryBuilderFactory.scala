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

import reactivemongo.api.bson.buffer.WritableBuffer

import reactivemongo.core.protocol.{ Query, QueryFlags, MongoWireVersion }
import reactivemongo.core.netty.BufferSequence

import reactivemongo.api.{
  Collation,
  Collection,
  Cursor,
  CursorProducer,
  DefaultCursor,
  FailoverStrategy,
  PackSupport,
  ReadConcern,
  ReadPreference,
  SerializationPack
}

import reactivemongo.api.commands.CommandCodecs

/** Query build factory */
private[reactivemongo] trait QueryBuilderFactory[P <: SerializationPack] extends HintFactory[P] { self: PackSupport[P] =>

  import GenericQueryBuilder.logger

  /**
   * A builder that helps to make a fine-tuned query to MongoDB.
   *
   * When the query is ready, you can call `cursor` to get a [[Cursor]], or `one` if you want to retrieve just one document.
   *
   *
   * @param skip the number of documents to skip.
   * @param batchSize the upper limit on the number of documents to retrieve per batch (0 for unspecified)
   * @param flagsN the query flags
   * @param readConcern the read concern {@since MongoDB 3.6}
   * @param readPreference the query [[ReadPreference]]
   * @param filter the query filter
   * @param projection the [[https://docs.mongodb.com/manual/reference/method/db.collection.find/index.html#projection projection specification]]
   * @param max the optional exclusive [[https://docs.mongodb.com/manual/reference/method/cursor.max/ upper bound]] for a specific index (default: `None`)
   * @param min the optional exclusive [[https://docs.mongodb.com/manual/reference/method/cursor.min/ lower bound]] for a specific index (default: `None`)
   * @param sort the optional [[https://docs.mongodb.com/manual/reference/method/cursor.sort/ sort specification]]
   * @param hint the index to [[https://docs.mongodb.com/manual/reference/method/cursor.hint/index.html “hint”]] or force MongoDB to use when performing the query.
   * @param explain the explain flag
   * @param snapshot the snapshot flag
   * @param comment the query comment
   * @param maxTimeMs the maximum execution time
   * @param singleBatch the flag to determines whether to close the cursor after the first batch (default: `false`)
   * @param maxScan this option specifies a maximum number of documents or index keys the query plan will scan.
   * @param returnKey if this flag is true, returns only the index keys in the resulting documents
   * @param showRecordId the flags to determines whether to return the record identifier for each document
   * @param collation the optional collation to use for the find command (default: `None`) {@since MongoDB 3.4}
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
   * @define projectionFunction Sets the [[https://docs.mongodb.com/manual/reference/method/db.collection.find/index.html#projection projection specification]] to determine which fields to include in the returned documents
   */
  final class QueryBuilder private[reactivemongo] (
    collection: Collection,
    failoverStrategy: FailoverStrategy = FailoverStrategy.default,
    val skip: Int = 0,
    val batchSize: Int = 0,
    val flagsN: Int = 0, // TODO: CursorOptions
    val readConcern: ReadConcern = ReadConcern.default,
    readPreference: ReadPreference = ReadPreference.primary,
    filter: Option[pack.Document] = None,
    val projection: Option[pack.Document] = None,
    val sort: Option[pack.Document] = None,
    val max: Option[pack.Document] = None,
    val min: Option[pack.Document] = None,
    val hint: Option[Hint] = None,
    val explain: Boolean = false,
    val snapshot: Boolean = false,
    val comment: Option[String] = None,
    val maxTimeMs: Option[Long] = None,
    val singleBatch: Boolean = false,
    val maxScan: Option[Double] = None,
    val returnKey: Boolean = false,
    val showRecordId: Boolean = false,
    val collation: Option[Collation] = None) {

    @inline private def version: MongoWireVersion =
      collection.db.connectionState.metadata.maxWireVersion

    /**
     * $filterFunction.
     */
    private[api] def filter(predicate: pack.Document): QueryBuilder =
      copy(filter = Some(predicate))

    /**
     * $filterFunction.
     */
    private[api] def filter[T](selector: T)(implicit w: pack.Writer[T]): QueryBuilder =
      copy(filter = Option(selector).map(pack.serialize[T](_, w)))

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
    final def sort(document: pack.Document): QueryBuilder =
      copy(sort = Some(document))

    /**
     * $projectionFunction.
     */
    final def projection(document: pack.Document): QueryBuilder =
      copy(projection = Some(document))

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
    final def projection[Pjn](p: Pjn)(implicit writer: pack.Writer[Pjn]): QueryBuilder =
      copy(projection = Some(pack.serialize(p, writer)))

    /**
     * Sets the [[https://docs.mongodb.com/manual/reference/operator/meta/hint/ hint document]] (a document that declares the index MongoDB should use for this query).
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   hint(coll.hint(BSONDocument("foo" -> 1))) // sets the hint
     * }}}
     */
    final def hint(h: Hint): QueryBuilder = copy(hint = Some(h))

    /**
     * Toggles [[https://docs.mongodb.org/manual/reference/method/cursor.explain/#cursor.explain explain mode]].
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).explain()
     * }}}
     */
    final def explain(flag: Boolean = true): QueryBuilder = copy(explain = flag)

    /**
     * Toggles [[https://docs.mongodb.org/manual/faq/developers/#faq-developers-isolate-cursors snapshot mode]].
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).snapshot()
     * }}}
     */
    final def snapshot(flag: Boolean = true): QueryBuilder =
      copy(snapshot = flag)

    /**
     * Adds a comment to this query, that may appear in the MongoDB logs.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   comment("Any comment to trace the query")
     * }}}
     */
    final def comment(message: String): QueryBuilder =
      copy(comment = Some(message))

    /**
     * Adds [[https://docs.mongodb.org/v3.0/reference/operator/meta/maxTimeMS/ maxTimeMs]] to query.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   maxTimeMs(1000L) // 1s
     * }}}
     */
    final def maxTimeMs(milliseconds: Long): QueryBuilder =
      copy(maxTimeMs = Some(milliseconds))

    /**
     * Sets the [[ReadConcern]].
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   readConcern(reactivemongo.api.ReadConcern.Local)
     * }}}
     */
    final def readConcern(concern: ReadConcern): QueryBuilder =
      copy(readConcern = concern)

    /**
     * Sets the `singleBatch` flag.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   singleBatch()
     * }}}
     */
    final def singleBatch(flag: Boolean = true): QueryBuilder =
      copy(singleBatch = flag)

    /**
     * Sets the `maxScan` flag.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   maxScan(1.23D)
     * }}}
     */
    final def maxScan(max: Double): QueryBuilder = copy(maxScan = Some(max))

    /**
     * Sets the `returnKey` flag.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).returnKey()
     * }}}
     */
    final def returnKey(flag: Boolean = true): QueryBuilder = copy(returnKey = flag)

    /**
     * Sets the `showRecordId` flag.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) =
     *   coll.find(BSONDocument.empty).showRecordId()
     * }}}
     */
    final def showRecordId(flag: Boolean = true): QueryBuilder =
      copy(showRecordId = flag)

    /**
     * Sets the `max` document.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   max(BSONDocument("field" -> "maxValue"))
     * }}}
     */
    final def max(document: pack.Document): QueryBuilder =
      copy(max = Some(document))

    /**
     * Sets the `min` document.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection) = coll.find(BSONDocument.empty).
     *   min(BSONDocument("field" -> "minValue"))
     * }}}
     */
    final def min(document: pack.Document): QueryBuilder =
      copy(min = Some(document))

    /**
     * Sets the `collation` document.
     * @since MongoDB 3.4
     *
     *
     * {{{
     * import reactivemongo.api.Collation
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def foo(coll: BSONCollection, c: Collation) =
     *   coll.find(BSONDocument.empty).collation(c)
     * }}}
     */
    final def collation(collation: Collation): QueryBuilder =
      copy(collation = Some(collation))

    // QueryOps

    /** Sets the query (raw) [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#flags flags]]. */
    private[api] def flags(n: Int): QueryBuilder = copy(flagsN = n)

    /**
     * Makes the result cursor [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.awaitData await data]].
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def waitData(c: BSONCollection) =
     *   c.find(BSONDocument.empty).awaitData
     * }}}
     */
    final def awaitData: QueryBuilder =
      copy(flagsN = flagsN | QueryFlags.AwaitData)

    /**
     * Sets the size of result batches.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def tenBatch(c: BSONCollection) =
     *   c.find(BSONDocument.empty).batchSize(10)
     * }}}
     */
    final def batchSize(n: Int): QueryBuilder = copy(batchSize = n)

    /**
     * Sets the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.exhaust flag]] to return all data returned by the query at once rather than splitting the results into batches.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def ex(c: BSONCollection) =
     *   c.find(BSONDocument.empty).exhaust
     * }}}
     */
    final def exhaust: QueryBuilder = copy(flagsN = flagsN | QueryFlags.Exhaust)

    /**
     * Sets the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.noTimeout `noTimeout`]] flag.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def acceptTimeout(c: BSONCollection) =
     *   c.find(BSONDocument.empty).noCursorTimeout
     * }}}
     */
    final def noCursorTimeout: QueryBuilder =
      copy(flagsN = flagsN | QueryFlags.NoCursorTimeout)

    private[api] def oplogReplay: QueryBuilder =
      copy(flagsN = flagsN | QueryFlags.OplogReplay)

    /**
     * Sets the [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.partial `partial`]] flag.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def allowPartial(c: BSONCollection) =
     *   c.find(BSONDocument.empty).allowPartialResults
     * }}}
     */
    final def allowPartialResults: QueryBuilder =
      copy(flagsN = flagsN | QueryFlags.Partial)

    /**
     * Sets how many documents must be skipped at the beginning of the results.
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def skipTen(c: BSONCollection) =
     *   c.find(BSONDocument.empty).skip(10)
     * }}}
     */
    final def skip(n: Int): QueryBuilder = copy(skip = n)

    /**
     * Allows querying of a replica slave ([[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.slaveOk `slaveOk`]]).
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def makeSlaveOk(c: BSONCollection) =
     *   c.find(BSONDocument.empty).slaveOk
     * }}}
     */
    final def slaveOk: QueryBuilder = copy(flagsN = flagsN | QueryFlags.SlaveOk)

    /**
     * Makes the result [[https://docs.mongodb.com/manual/reference/method/cursor.addOption/#DBQuery.Option.tailable cursor tailable]].
     *
     * {{{
     * import reactivemongo.api.bson.BSONDocument
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def ensureTailable(c: BSONCollection) =
     *   c.find(BSONDocument.empty).tailable
     * }}}
     */
    final def tailable: QueryBuilder =
      copy(flagsN = flagsN | QueryFlags.TailableCursor)

    // Cursor

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
    final def cursor[T](readPreference: ReadPreference = readPreference)(implicit reader: pack.Reader[T], cp: CursorProducer[T]): cp.ProducedCursor = cp.produce(defaultCursor[T](readPreference))

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
    final def one[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = one(this.readPreference)

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
    final def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = copy(batchSize = 1).defaultCursor(readPreference)(reader).headOption

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
    final def requireOne[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = requireOne(readPreference)

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
    final def requireOne[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[T] = copy(batchSize = 1).defaultCursor(readPreference)(reader).head

    // ---

    private[reactivemongo] def copy(
      failoverStrategy: FailoverStrategy = failoverStrategy,
      skip: Int = this.skip,
      batchSize: Int = this.batchSize,
      flagsN: Int = this.flagsN,
      readConcern: ReadConcern = this.readConcern,
      readPreference: ReadPreference = this.readPreference,
      filter: Option[pack.Document] = this.filter,
      projection: Option[pack.Document] = this.projection,
      sort: Option[pack.Document] = this.sort,
      max: Option[pack.Document] = this.max,
      min: Option[pack.Document] = this.min,
      hint: Option[Hint] = this.hint,
      explain: Boolean = this.explain,
      snapshot: Boolean = this.snapshot,
      comment: Option[String] = this.comment,
      maxTimeMs: Option[Long] = this.maxTimeMs,
      singleBatch: Boolean = this.singleBatch,
      maxScan: Option[Double] = this.maxScan,
      returnKey: Boolean = this.returnKey,
      showRecordId: Boolean = this.showRecordId,
      collation: Option[Collation] = this.collation): QueryBuilder =
      new QueryBuilder(this.collection, failoverStrategy, skip, batchSize,
        flagsN, readConcern, readPreference, filter, projection, sort, max,
        min, hint, explain, snapshot, comment, maxTimeMs, singleBatch, maxScan,
        returnKey, showRecordId, collation)

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

        elements += element(
          f"$$query",
          filter.getOrElse(builder.document(Seq.empty)))

        sort.foreach {
          elements += element(f"$$orderby", _)
        }

        hint.foreach {
          case HintString(str) =>
            elements += element(f"$$hint", builder.string(str))

          case HintDocument(doc) =>
            elements += element(f"$$hint", doc)

          case _ =>
        }

        maxTimeMs.foreach { l =>
          elements += element(f"$$maxTimeMS", long(l))
        }

        comment.foreach { c =>
          elements += element(f"$$comment", string(c))
        }

        if (explain) {
          elements += element(f"$$explain", boolean(true))
        }

        if (snapshot) {
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

        def partial: Boolean = (flagsN & Partial) == Partial
        def awaitData: Boolean = (flagsN & AwaitData) == AwaitData
        def oplogReplay: Boolean = (flagsN & OplogReplay) == OplogReplay
        def noTimeout: Boolean =
          (flagsN & NoCursorTimeout) == NoCursorTimeout

        def tailable: Boolean =
          (flagsN & TailableCursor) == TailableCursor

        lazy val limit: Option[Int] = {
          if (maxDocs > 0 && maxDocs < Int.MaxValue) Some(maxDocs)
          else Option.empty[Int]
        }

        def batchSizeN: Option[Int] = {
          val sz = limit.fold(batchSize)(batchSize.min)

          if (sz > 0 && sz < Int.MaxValue) Some(sz)
          else Option.empty[Int]
        }

        val elements = Seq.newBuilder[pack.ElementProducer]

        elements ++= Seq(
          element("find", string(collection.name)),
          element("skip", int(skip)),
          element("tailable", boolean(tailable)),
          element("awaitData", boolean(awaitData)),
          element("oplogReplay", boolean(oplogReplay)),
          element("noCursorTimeout", boolean(noTimeout)),
          element("allowPartialResults", boolean(partial)),
          element("singleBatch", boolean(singleBatch)),
          element("returnKey", boolean(returnKey)),
          element("showRecordId", boolean(showRecordId)))

        if (version.compareTo(MongoWireVersion.V34) < 0) {
          elements += element("snapshot", boolean(snapshot))
        }

        maxScan.foreach { max =>
          elements += element("maxScan", builder.double(max))
        }

        filter.foreach {
          elements += element("filter", _)
        }

        sort.foreach {
          elements += element("sort", _)
        }

        projection.foreach {
          elements += element("projection", _)
        }

        hint.foreach {
          case HintString(str) =>
            elements += element("hint", builder.string(str))

          case HintDocument(doc) =>
            elements += element("hint", doc)

        }

        batchSizeN.foreach { i =>
          elements += element("batchSize", int(i))
        }

        limit.foreach { i =>
          elements += element("limit", int(i))
        }

        comment.foreach { c =>
          elements += element("comment", string(c))
        }

        maxTimeMs.foreach { l =>
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

        val session = collection.db.session.filter(
          _ => (version.compareTo(MongoWireVersion.V36) >= 0))

        elements ++= CommandCodecs.writeSessionReadConcern(
          builder)(session)(readConcern)

        val readPref = element(f"$$readPreference", writeReadPref(readPreference))

        val merged = if (!explain) {
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
      readPreference: ReadPreference)(
      implicit
      reader: pack.Reader[T]): Cursor.WithOps[T] = {

      val body = {
        if (version.compareTo(MongoWireVersion.V32) < 0) { _: Int =>
          val buffer = pack.writeToBuffer(
            WritableBuffer.empty,
            merge(readPreference, Int.MaxValue))

          BufferSequence(
            projection.fold(buffer) { pack.writeToBuffer(buffer, _) }.
              buffer)

        } else { maxDocs: Int =>
          // if MongoDB 3.2+, projection is managed in merge
          def prepared = pack.writeToBuffer(
            WritableBuffer.empty,
            merge(readPreference, maxDocs))

          BufferSequence(prepared.buffer)
        }
      }

      val flags = {
        if (readPreference.slaveOk) flagsN | QueryFlags.SlaveOk
        else flagsN
      }

      val name = {
        if (version.compareTo(MongoWireVersion.V32) < 0) {
          collection.fullCollectionName
        } else {
          collection.db.name + f".$$cmd" // Command 'find' for 3.2+
        }
      }

      val op = Query(flags, name, skip, batchSize)

      DefaultCursor.query(pack, op, body, readPreference,
        collection.db, failoverStrategy,
        collection.fullCollectionName, maxTimeMs)(reader)
    }
  }
}

private[reactivemongo] object GenericQueryBuilder {
  lazy val logger = reactivemongo.util.LazyLogger(getClass.getName)
}
