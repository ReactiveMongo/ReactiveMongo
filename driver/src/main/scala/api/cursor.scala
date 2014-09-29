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
import reactivemongo.api.collections.BufferReader
import reactivemongo.core.iteratees.{ CustomEnumeratee, CustomEnumerator }
import reactivemongo.core.netty.BufferSequence
import reactivemongo.core.protocol._
import reactivemongo.utils.ExtendedFutures.DelayedFuture
import reactivemongo.utils.LazyLogger
import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ ExecutionContext, Future }
import scala.util._

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
    cursor.flatMap { _.collect[M](upTo, stopOnError) }

  def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
    Enumerator.flatten(cursor.map(_.rawEnumerateResponses(maxDocs)))
}

object Cursor {
  private[api] val logger = LazyLogger("reactivemongo.api.Cursor")

  /**
   * Flattens the given future [[reactivemongo.api.Cursor]] to a [[reactivemongo.api.FlattenedCursor]].
   */
  def flatten[T](future: Future[Cursor[T]]) = new FlattenedCursor(future)
}

object DefaultCursor {
  import Cursor.logger

  def apply[P <: SerializationPack, A](
    pack: P,
    query: Query,
    documents: BufferSequence,
    readPreference: ReadPreference,
    mongoConnection: MongoConnection,
    failoverStrategy: FailoverStrategy,
    isMongo26WriteOp: Boolean)(implicit reader: pack.Reader[A]): Cursor[A] = new Cursor[A] {
      private def next(response: Response)(implicit ctx: ExecutionContext): Option[Future[Response]] = {
        if (response.reply.cursorID != 0) {
          val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
          logger.trace("[Cursor] Calling next on " + response.reply.cursorID + ", op=" + op)
          Some(Failover2(mongoConnection, failoverStrategy) { () =>
            mongoConnection.sendExpectingResponse(RequestMaker(op).copy(channelIdHint = Some(response.info.channelId)), isMongo26WriteOp)
          }.future)
        } else {
          logger.error("[Cursor] Call to next() but cursorID is 0, there is probably a bug")
          None
        }
      }

      @inline
      private def hasNext(response: Response): Boolean = response.reply.cursorID != 0

      @inline
      private def hasNext(response: Response, maxDocs: Int): Boolean = {
        response.reply.cursorID != 0 && (response.reply.numberReturned + response.reply.startingFrom) < maxDocs
      }

      @inline
      private def makeIterator(response: Response) = ReplyDocumentIterator(pack)(response.reply, response.documents)

      @inline
      private def makeRequest(implicit ctx: ExecutionContext): Future[Response] =
        Failover2(mongoConnection, failoverStrategy) { () =>
          mongoConnection.sendExpectingResponse(RequestMaker(query, documents, readPreference), isMongo26WriteOp)
        }.future

      @inline
      private def isTailable = (query.flags & QueryFlags.TailableCursor) == QueryFlags.TailableCursor

      def simpleCursorEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] = {
        Enumerator.flatten(makeRequest.map(new CustomEnumerator.SEnumerator(_)(
          next = response => if (hasNext(response, maxDocs)) next(response) else None,
          cleanUp = response =>
            if (response.reply.cursorID != 0) {
              logger.debug(s"[Cursor] Clean up ${response.reply.cursorID}, sending KillCursor")
              mongoConnection.send(RequestMaker(KillCursors(Set(response.reply.cursorID))))
            } else logger.trace(s"[Cursor] Cursor exhausted (${response.reply.cursorID})"))))
      }

      def tailableCursorEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] = {
        Enumerator.flatten(makeRequest.map { response =>
          new CustomEnumerator.SEnumerator((response, maxDocs))(
            next = current => {
              if (maxDocs - current._1.reply.numberReturned > 0) {
                val nextResponse =
                  if (hasNext(current._1)) {
                    next(current._1)
                  } else {
                    logger.debug("[Tailable Cursor] Current cursor exhausted, renewing...")
                    Some(DelayedFuture(500, mongoConnection.actorSystem).flatMap(_ => makeRequest))
                  }
                nextResponse.map(_.map((_, maxDocs - current._1.reply.numberReturned)))
              } else None
            },
            cleanUp = current =>
              if (current._1.reply.cursorID != 0) {
                logger.debug(s"[Tailable Cursor] Closing  cursor ${current._1.reply.cursorID}, cleanup")
                mongoConnection.send(RequestMaker(KillCursors(Set(current._1.reply.cursorID))))
              } else logger.trace(s"[Tailable Cursor] Cursor exhausted (${current._1.reply.cursorID})"))
        }).map(_._1)
      }

      def rawEnumerateResponses(maxDocs: Int = Int.MaxValue)(implicit ctx: ExecutionContext): Enumerator[Response] =
        if (isTailable) tailableCursorEnumerateResponses(maxDocs) else simpleCursorEnumerateResponses(maxDocs)

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
        enumerateResponses(maxDocs, stopOnError) &> Enumeratee.map(response => ReplyDocumentIterator(pack)(response.reply, response.documents))

      def enumerate(maxDocs: Int = Int.MaxValue, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[A] = {
        @tailrec
        def next(it: Iterator[A], stopOnError: Boolean): Option[Try[A]] = {
          if (it.hasNext) {
            val tried = Try(it.next)
            if (tried.isFailure && !stopOnError)
              next(it, stopOnError)
            else Some(tried)
          } else None
        }
        enumerateResponses(maxDocs, stopOnError) &> Enumeratee.mapFlatten { response =>
          val iterator = ReplyDocumentIterator(pack)(response.reply, response.documents)
          if(!iterator.hasNext)
            Enumerator.empty
          else
            CustomEnumerator.SEnumerator(iterator.next) { _ =>
              next(iterator, stopOnError).map {
                case Success(mt) => Future.successful(mt)
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
    }
}