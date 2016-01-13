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
package reactivemongo.api

import play.api.libs.iteratee._

import org.jboss.netty.buffer.ChannelBuffer

import reactivemongo.core.iteratees.{ CustomEnumeratee, CustomEnumerator }
import reactivemongo.core.netty.BufferSequence
import reactivemongo.core.protocol.{
  GetMore,
  KillCursors,
  Query,
  QueryFlags,
  RequestMaker,
  Reply,
  Response,
  ReplyDocumentIterator,
  ReplyDocumentIteratorExhaustedException
}
import reactivemongo.util.{
  ExtendedFutures,
  LazyLogger
}, ExtendedFutures.DelayedFuture
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

/**
 * @tparam T the type parsed from each result document
 */
trait Cursor[T] {
  /**
   * Produces an Enumerator of documents.
   * Given the `stopOnError` parameter, this Enumerator may stop on any non-fatal exception, or skip and continue.
   * The returned enumerator may process up to `maxDocs`. If `stopOnError` is false, then documents that cause error
   * are dropped, so the enumerator may emit a little less than `maxDocs` even if it processes `maxDocs` documents.
   *
   * @param maxDocs Enumerate up to `maxDocs` documents.
   * @param stopOnError States if the produced Enumerator may stop on non-fatal exception.
   *
   * @return an Enumerator of documents.
   */
  def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[T]

  /**
   * Produces an Enumerator of Iterator of documents.
   * Given the `stopOnError` parameter, this Enumerator may stop on any non-fatal exception, or skip and continue.
   *
   * @param maxDocs Enumerate up to `maxDocs` documents.
   * @param stopOnError States if the produced Enumerator may stop on non-fatal exception.
   *
   * @return an Enumerator of Iterators of documents.
   */
  def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[T]]

  /**
   * Produces an Enumerator of responses from the database.
   * Given the `stopOnError` parameter, this Enumerator may stop on any non-fatal exception, or skip and continue.
   *
   * @param maxDocs Enumerate up to `maxDocs` documents.
   * @param stopOnError States if the produced Enumerator may stop on non-fatal exception.
   *
   * @return an Enumerator of Responses.
   */
  def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response]

  /**
   * Collects all the documents into a collection of type `M[T]`.
   * Given the `stopOnError` parameter (which defaults to true), the resulting Future may fail if any
   * non-fatal exception occurs. If set to false, all the documents that caused exceptions are skipped.
   * Up to `maxDocs` returned by the database may be processed. If `stopOnError` is false, then documents that cause error
   * are dropped, so the result may contain a little less than `maxDocs` even if `maxDocs` documents were processed.
   *
   * @param upTo Collect up to `upTo` documents.
   * @param stopOnError States if the Future should fail if any non-fatal exception occurs.
   *
   * Example:
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]
   * // return the 3 first documents in a Vector[BSONDocument].
   * val vector = cursor.collect[Vector](3)
   * }}}
   */
  def collect[M[_]](upTo: Int = Int.MaxValue, stopOnError: Boolean = true)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]]

  /**
   * Applies a binary operator to a start value and all responses handled
   * by this cursor, going first to last.
   *
   * @tparam A the result type of the binary operator.
   * @param z the start value.
   * @param maxDocs the maximum number of documents to be read.
   * @param suc the binary operator to be applied when the next response is successfully read.
   * @param err the binary operator to be applied when failing to get the next response.
   */
  def foldResponses[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A]

  /**
   * Applies a binary operator to a start value and all bulks of documents
   * retrieved by this cursor, going first to last.
   *
   * @tparam A the result type of the binary operator.
   * @param z the start value.
   * @param maxDocs the maximum number of documents to be read.
   * @param suc the binary operator to be applied when the next response is successfully read.
   * @param err the binary operator to be applied when failing to get the next response.
   */
  def foldBulks[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A]

  /**
   * Applies a binary operator to a start value and all elements retrieved
   * by this cursor, going first to last.
   *
   * @tparam A the result type of the binary operator.
   * @param z the start value.
   * @param maxDocs the maximum number of documents to be read.
   * @param suc the binary operator to be applied when the next document is successfully read.
   * @param err the binary operator to be applied when failing to read the next document.
   *
   * {{{
   * cursor.foldWhile(Nil: Seq[Person])((s, p) => Cursor.Cont(s :+ p),
   *   { (l, e) => println("last valid value: " + l); Cursor.Fail(e) })
   * }}}
   */
  def foldWhile[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A]

  /**
   * Collects all the documents into a `List[T]`.
   * Given the `stopOnError` parameter (which defaults to true), the resulting Future may fail if any
   * non-fatal exception occurs. If set to false, all the documents that caused exceptions are skipped.
   *
   * @param upTo Collect up to `maxDocs` documents.
   * @param stopOnError States if the Future should fail if any non-fatal exception occurs.
   *
   * Example:
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]
   * // return the 3 first documents in a list.
   * val list = cursor.toList(3)
   * }}}
   */
  @deprecated("consider using collect[List] instead", "0.10.0")
  def toList(upTo: Int = Int.MaxValue, stopOnError: Boolean = true)(implicit ctx: ExecutionContext): Future[List[T]] = collect[List](upTo, stopOnError)

  /**
   * Gets the first document matching the query, if any.
   * The resulting Future may fail if any exception occurs (for example, while deserializing the document).
   *
   * Example:
   * {{{
   * val cursor = collection.find(query, filter).cursor[BSONDocument]
   * // return option of the first element.
   * val first: Future[Option[BSONDocument]] = cursor.headOption
   * }}}
   */
  def headOption(implicit ctx: ExecutionContext): Future[Option[T]] = collect[Iterable](1, true).map(_.headOption)

  /**
   * Produces an Enumerator of responses from the database.
   * An Enumeratee for error handling should be used to prevent silent failures.
   * Consider using `enumerateResponses` instead.
   *
   * @param maxDocs Enumerate up to `maxDocs` documents.
   *
   * @return an Enumerator of Responses.
   */
  def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response]
}

class FlattenedCursor[T](cursor: Future[Cursor[T]]) extends Cursor[T] {
  def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[T] =
    Enumerator.flatten(cursor.map(_.enumerate(maxDocs, stopOnError)))

  def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[T]] =
    Enumerator.flatten(cursor.map(_.enumerateBulks(maxDocs, stopOnError)))

  def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response] =
    Enumerator.flatten(cursor.map(_.enumerateResponses(maxDocs, stopOnError)))

  def collect[M[_]](upTo: Int, stopOnError: Boolean)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] =
    cursor.flatMap(_.collect[M](upTo, stopOnError))

  def foldResponses[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldResponses(z, maxDocs)(suc, err))

  def foldBulks[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulks(z, maxDocs)(suc, err))

  def foldWhile[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhile(z, maxDocs)(suc, err))

  def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
    Enumerator.flatten(cursor.map(_.rawEnumerateResponses(maxDocs)))
}

/**
 * Cursor wrapper, to help to define custom cursor classes.
 * @see CursorProducer
 */
trait WrappedCursor[T] extends Cursor[T] {
  /** The underlying cursor */
  def wrappee: Cursor[T]

  def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[T] =
    wrappee.enumerate(maxDocs, stopOnError)

  def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[T]] =
    wrappee.enumerateBulks(maxDocs, stopOnError)

  def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response] =
    wrappee.enumerateResponses(maxDocs, stopOnError)

  def collect[M[_]](upTo: Int, stopOnError: Boolean)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] =
    wrappee.collect[M](upTo, stopOnError)

  def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] = wrappee.rawEnumerateResponses(maxDocs)

  def foldResponses[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Response) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldResponses(z, maxDocs)(suc, err)

  def foldBulks[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, Iterator[T]) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldBulks(z, maxDocs)(suc, err)

  def foldWhile[A](z: => A, maxDocs: Int = Int.MaxValue)(suc: (A, T) => Cursor.State[A], err: (A, Throwable) => Cursor.State[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldWhile(z, maxDocs)(suc, err)

}

/** Cursor companion object */
object Cursor {
  private[api] val logger = LazyLogger("reactivemongo.api.Cursor")

  /**
   * Flattens the given future [[reactivemongo.api.Cursor]] to a [[reactivemongo.api.FlattenedCursor]].
   */
  def flatten[T, C[_] <: Cursor[_]](future: Future[C[T]])(implicit fs: CursorFlattener[C]): C[T] = fs.flatten(future)

  // ---

  sealed trait State[A]

  /** Continue with given value */
  case class Cont[T](value: T) extends State[T]

  /** Successfully stop processing with given value */
  case class Done[T](value: T) extends State[T]

  /** Ends processing due to failure of given `cause` */
  case class Fail[T](cause: Throwable) extends State[T]
}

object DefaultCursor {
  import Cursor.{ State, Cont, Done, Fail, logger }
  import reactivemongo.api.commands.ResultCursor

  @deprecated(message = "Use [[query]]", since = "0.11.10")
  def apply[P <: SerializationPack, A](
    pack: P,
    query: Query,
    documents: BufferSequence,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean)(implicit reader: pack.Reader[A]): Cursor[A] =
    DefaultCursor.query[P, A](pack, query, documents, readPreference,
      mongoConnection, failover, isMongo26WriteOp)

  def query[P <: SerializationPack, A](
    pack: P,
    query: Query,
    requestBuffer: BufferSequence,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean)(implicit reader: pack.Reader[A]) = new Impl[A] {
    val connection = mongoConnection
    val failoverStrategy = failover
    val mongo26WriteOp = isMongo26WriteOp
    val fullCollectionName = query.fullCollectionName
    val numberToReturn = query.numberToReturn
    val tailable = (query.flags &
      QueryFlags.TailableCursor) == QueryFlags.TailableCursor

    val documentIterator =
      ReplyDocumentIterator(pack)(_: Reply, _: ChannelBuffer)(reader)

    @inline
    def makeRequest(implicit ctx: ExecutionContext): Future[Response] =
      Failover2(connection, failoverStrategy) { () =>
        connection.sendExpectingResponse(
          RequestMaker(query, requestBuffer, readPreference), mongo26WriteOp)
      }.future
  }

  /**
   * @param channelId the ID of the current channel
   * @param toReturn the number to return
   */
  def getMore[P <: SerializationPack, A](
    pack: P,
    preload: => Response,
    result: ResultCursor,
    toReturn: Int,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failover: FailoverStrategy,
    isMongo26WriteOp: Boolean)(implicit reader: pack.Reader[A]) = new Impl[A] {
    val connection = mongoConnection
    val failoverStrategy = failover
    val mongo26WriteOp = isMongo26WriteOp
    val fullCollectionName = result.fullCollectionName
    val numberToReturn = toReturn
    val tailable = false // ??

    private lazy val response = preload

    val documentIterator =
      ReplyDocumentIterator(pack)(_: Reply, _: ChannelBuffer)(reader)

    private var req: ExecutionContext => Future[Response] = { implicit ec =>
      this.req = makeReq
      Future.successful(response)
    }

    private val op = GetMore(fullCollectionName, toReturn, result.cursorId)
    private lazy val reqMaker =
      RequestMaker(op).copy(channelIdHint = Some(response.info.channelId))

    private val makeReq = { implicit ctx: ExecutionContext =>
      logger.trace(s"[Cursor] Calling next on ${result.cursorId}, op=$op")

      Failover2(connection, failoverStrategy) { () =>
        connection.sendExpectingResponse(reqMaker, mongo26WriteOp)
      }.future
    }

    def makeRequest(implicit ctx: ExecutionContext): Future[Response] = req(ctx)
  }

  private[reactivemongo] trait Impl[A] extends Cursor[A] {
    def connection: MongoConnection

    def failoverStrategy: FailoverStrategy

    def mongo26WriteOp: Boolean

    def fullCollectionName: String

    def numberToReturn: Int

    def tailable: Boolean

    /** Sends the initial request. */
    def makeRequest(implicit ctx: ExecutionContext): Future[Response]

    def documentIterator: (Reply, ChannelBuffer) => Iterator[A]

    private def next(response: Response)(implicit ctx: ExecutionContext): Future[Option[Response]] = {
      if (response.reply.cursorID != 0) {
        val op = GetMore(fullCollectionName, numberToReturn, response.reply.cursorID)
        logger.trace("[Cursor] Calling next on " + response.reply.cursorID + ", op=" + op)
        Failover2(connection, failoverStrategy) { () =>
          connection.sendExpectingResponse(RequestMaker(op).copy(channelIdHint = Some(response.info.channelId)), mongo26WriteOp)
        }.future.map(Some(_))
      } else {
        logger.error("[Cursor] Call to next() but cursorID is 0, there is probably a bug")
        Future.successful(Option.empty[Response])
      }
    }

    @inline
    private def hasNext(response: Response): Boolean = response.reply.cursorID != 0

    @inline
    private def hasNext(response: Response, maxDocs: Int): Boolean =
      hasNext(response) && (response.reply.numberReturned + response.reply.startingFrom) < maxDocs

    @inline
    private def makeIterator(response: Response) = documentIterator(response.reply, response.documents)

    /** Returns next response using tailable mode */
    private def tailResponse(current: Response, maxDocs: Int)(implicit context: ExecutionContext): Future[Option[Response]] = connection.killed flatMap {
      case true =>
        logger.warn("[tailResponse] Connection is killed")
        Future.successful(Option.empty[Response])

      case _ =>
        if (hasNext(current)) next(current)
        else {
          logger.debug("[tailResponse] Current cursor exhausted, renewing...")
          DelayedFuture(500, connection.actorSystem).
            flatMap(_ => makeRequest.map(Some(_)))
        }
    }

    @inline
    private def killCursors(cursorID: Long, logCat: String): Unit =
      if (cursorID != 0) {
        logger.debug(s"[$logCat] Clean up ${cursorID}, sending KillCursor")
        connection.send(RequestMaker(KillCursors(Set(cursorID))))
      } else logger.trace(s"[$logCat] Cursor exhausted (${cursorID})")

    def foldResponses[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, Response) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = new FoldResponses(z, maxDocs, suc, err)(ctx)()

    def foldBulks[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, Iterator[A]) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = foldResponses(z, maxDocs)({ (s, r) =>
      Try(makeIterator(r)) match {
        case Success(it) => suc(s, it)
        case Failure(e)  => Fail(e)
      }
    }, err)

    def foldWhile[T](z: => T, maxDocs: Int = Int.MaxValue)(suc: (T, A) => State[T], err: (T, Throwable) => State[T])(implicit ctx: ExecutionContext): Future[T] = {
      def process(cur: T, st: State[T])(op: T => State[T]): State[T] =
        st match {
          case Cont(v)     => op(v)
          case f @ Fail(_) => f
          case _           => st
        }

      def go(it: Iterator[A])(v: T): State[T] =
        if (!it.hasNext) Cont(v)
        else Try(it.next) match {
          case Failure(x @ ReplyDocumentIteratorExhaustedException(_)) =>
            Fail(x)
          case Failure(e) => process(v, err(v, e))(go(it))
          case Success(a) => process(v, suc(v, a))(go(it))
        }

      foldBulks(z, maxDocs)({ (cur, it) => go(it)(cur) }, err)
    }

    def simpleCursorEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
      Enumerator.flatten(makeRequest.map(new CustomEnumerator.SEnumerator(_)(
        next = response => {
          if (hasNext(response, maxDocs)) next(response)
          else Future.successful(Option.empty[Response])
        }, cleanUp = { resp => killCursors(resp.reply.cursorID, "Cursor") })))

    def tailableCursorEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
      Enumerator.flatten(makeRequest.map { response =>
        new CustomEnumerator.SEnumerator(response -> 0)(
          next = { current =>
            val (r, c) = current
            if (c < maxDocs) {
              tailResponse(r, maxDocs).map(_.map((_, c + r.reply.numberReturned)))
            } else Future.successful(Option.empty[(Response, Int)])
          },
          cleanUp = { current =>
            val (r, _) = current
            killCursors(r.reply.cursorID, "Tailable Cursor")
          }).map(_._1)
      })

    def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
      if (tailable) tailableCursorEnumerateResponses(maxDocs) else simpleCursorEnumerateResponses(maxDocs)

    def enumerateResponses(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response] =
      rawEnumerateResponses(maxDocs) &> {
        if (stopOnError)
          CustomEnumeratee.stopOnError
        else CustomEnumeratee.recover {
          new CustomEnumeratee.RecoverFromErrorFunction {
            def apply[E, A](throwable: Throwable, input: Input[E], continue: () => Iteratee[E, A]): Iteratee[E, A] = throwable match {
              case e: ReplyDocumentIteratorExhaustedException =>
                val errstr = "ReplyDocumentIterator exhausted! " +
                  "Was this enumerator applied to many iteratees concurrently? " +
                  "Stopping to prevent infinite recovery."
                logger.error(errstr, e)
                Error(errstr, input)
              case e =>
                logger.debug("There was an exception during the stream, dropping it since stopOnError is false", e)
                continue()
            }
          }
        }
      }

    def enumerateBulks(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[A]] =
      enumerateResponses(maxDocs, stopOnError) &> Enumeratee.map(makeIterator)

    def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[A] = {
      @annotation.tailrec
      def next(it: Iterator[A], stopOnError: Boolean): Option[Try[A]] = {
        if (it.hasNext) {
          val tried = Try(it.next)
          if (tried.isFailure && !stopOnError)
            next(it, stopOnError)
          else Some(tried)
        } else None
      }
      enumerateResponses(maxDocs, stopOnError) &> Enumeratee.mapFlatten { response =>
        val iterator = documentIterator(response.reply, response.documents)
        if (!iterator.hasNext) Enumerator.empty
        else CustomEnumerator.SEnumerator(iterator.next) { _ =>
          next(iterator, stopOnError).
            fold(Future.successful(Option.empty[A])) {
              case Success(mt) => Future.successful(Some(mt))
              case Failure(e)  => Future.failed(e)
            }
        }
      }
    }

    def collect[M[_]](upTo: Int = Int.MaxValue, stopOnError: Boolean = true)(implicit cbf: CanBuildFrom[M[_], A, M[A]], ctx: ExecutionContext): Future[M[A]] = {
      (enumerateResponses(upTo, stopOnError) |>>> Iteratee.fold(cbf.apply) { (builder, response) =>

        def tried[A](it: Iterator[A]) = new Iterator[Try[A]] { def hasNext = it.hasNext; def next = Try(it.next) }

        logger.trace(s"[collect] got response $response")

        val filteredIterator =
          if (!stopOnError)
            tried(makeIterator(response)).filter(_.isSuccess).map(_.get)
          else makeIterator(response)
        val iterator =
          if (upTo < response.reply.numberReturned + response.reply.startingFrom)
            filteredIterator.take(upTo - response.reply.startingFrom)
          else filteredIterator
        builder ++= iterator
      }).map(_.result)
    }

    private class FoldResponses[T](z: => T, maxDocs: Int,
                                   suc: (T, Response) => State[T], err: (T, Throwable) => State[T])(
                                       implicit ctx: ExecutionContext) {

      val nextResponse: (Response, Int) => Future[Option[Response]] =
        if (!tailable) { (r: Response, maxDocs: Int) =>
          if (!hasNext(r, maxDocs)) Future.successful(Option.empty[Response])
          else next(r)
        } else (r: Response, maxDocs: Int) => tailResponse(r, maxDocs)

      def process(cur: T, st: State[T])(op: T => Future[T]): Future[T] =
        st match {
          case Done(v) => Future.successful(v)
          case Cont(v) => op(v)
          case Fail(x) => Future.failed[T](x)
        }

      def go(r: Response, c: Int)(v: T): Future[T] = nextResponse(r, c).flatMap(
        _.fold(Future successful v)(x =>
          procResponses(Future.successful(x), v, c)))

      def procResponses(done: Future[Response], cur: T, c: Int): Future[T] =
        done flatMap { r =>
          val st = suc(cur, r) match {
            case end @ (Done(_) | Fail(_)) => {
              // Releases cursor before ending
              killCursors(r.reply.cursorID, "FoldResponses")
              end
            }
            case Cont(v) if (c == maxDocs) => Done(v)
            case state @ _                 => state
          }

          process(cur, st)(go(r, c + r.reply.numberReturned))
        }

      def apply(): Future[T] = procResponses(makeRequest, z, 0)
    }
  }
}

/** Allows to enrich a base cursor. */
trait CursorProducer[T] {
  type ProducedCursor <: Cursor[T]

  /** Produces a custom cursor from the `base` one. */
  def produce(base: Cursor[T]): ProducedCursor
}

object CursorProducer {
  implicit def defaultCursorProducer[T]: CursorProducer[T] =
    new CursorProducer[T] {
      type ProducedCursor = Cursor[T]
      def produce(base: Cursor[T]) = base
    }
}

/** Flattening strategy for cursor. */
trait CursorFlattener[C[_] <: Cursor[_]] {
  /** Flatten a future of cursor as cursor. */
  def flatten[T](future: Future[C[T]]): C[T]
}

/** Flatteners helper */
object CursorFlattener {
  implicit object defaultCursorFlattener extends CursorFlattener[Cursor] {
    def flatten[T](future: Future[Cursor[T]]): Cursor[T] =
      new FlattenedCursor(future)
  }
}
