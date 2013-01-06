package reactivemongo.api

import reactivemongo.core.actors.RequestMakerExpectingResponse
import reactivemongo.bson.handlers._
import reactivemongo.core.protocol._
import reactivemongo.utils.buffers._
import reactivemongo.utils.ExtendedFutures._

import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.LoggerFactory
import play.api.libs.iteratee._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Allows to fetch the next documents matching a query.
 *
 * Please that after invoking some Cursor methods, this Cursor instance should not be reused as it may cause unexpected behavior.
 *
 * Example:
 * {{{
import play.api.libs.iteratee.Iteratee
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.handlers.DefaultBSONHandlers._

object Samples {

  val connection = MongoConnection( List( "localhost:27016" ) )
  val db = connection("plugin")
  val collection = db("acoll")

  def listDocs() = {
    // select only the documents which field 'firstName' equals 'Jack'
    val query = BSONDocument("firstName" -> BSONString("Jack"))
    // select only the field 'lastName'
    val filter = BSONDocument(
      "lastName" -> BSONInteger(1),
      "_id" -> BSONInteger(0)
    )

    // get a Cursor[TraversableBSONDocument]
    val cursor = collection.find(query, filter)
    // let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate.apply(Iteratee.foreach { doc =>
      println("found document: " + BSONDocument.pretty(doc))
    })

    // or, the same with getting a list
    val cursor2 = collection.find(query, filter)
    val futurelist = cursor2.toList
  }
}
}}}
 *
 * It is worth diving into the [[https://github.com/playframework/Play20/wiki/Iteratees Play! 2.0 Iteratee documentation]].
 *
 * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
 *
*/
trait Cursor[T] {
  import Cursor.logger
  /** An iterator on the last fetched documents. */
  val iterator: Iterator[T]

  def cursorId: Future[Long]

  def connection: Future[MongoConnection]

  /** Gets the next instance of that cursor. */
  def next: Future[Cursor[T]]

  /** Tells if another instance of cursor can be fetched. */
  def hasNext: Boolean

  import scala.collection.generic.CanBuildFrom

  /**
   * Enumerates this cursor.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
// get a Cursor[DefaultBSONIterator]
val cursor = collection.find(query, filter)
// let's enumerate this cursor and print a readable representation of each document in the response
cursor.enumerate.apply(Iteratee.foreach { doc =>
  println("found document: " + BSONDocument.pretty(doc))
})
}
}}}
   *
   * It is worth diving into the [[https://github.com/playframework/Play20/wiki/Iteratees Play! 2.0 Iteratee documentation]].
   *
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
   *
*/
  def enumerate()(implicit ctx: ExecutionContext) :Enumerator[T] = {
    if(hasNext) {
      Enumerator.unfoldM(this) { cursor =>
        Cursor.nextElement(cursor)
      }.andThen(Enumerator.eof) &> Enumeratee.collect {
        case Some(e) => e
      } &> Enumeratee.onIterateeDone(() => {
        logger.debug("iteratee is done, closing cursor")
        close
      })
    } else {
      Enumerator.eof
    }
  }

  /**
   * Collects all the documents into a collection of type `M[T]`.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, filter)
val list = cursor2[List].collect()
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def collect[M[_]]()(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext) :Future[M[T]] = {
    enumerate |>>> Iteratee.fold(cbf.apply) { (builder, t :T) => builder += t }.map(_.result)
  }

   /**
   * Collects all the documents into a collection of type `M[T]`.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor = collection.find(query, filter)
// gather the first 3 documents
val list = cursor[List].collect(3)
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
   * @param upTo The maximum size of this collection.
   */
  def collect[M[_]](upTo: Int)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext) :Future[M[T]] = {
    enumerate &> Enumeratee.take(upTo) |>>> Iteratee.fold(cbf.apply) { (builder, t :T) => builder += t }.map(_.result)
  }

  /**
   * Collects all the documents into a collection of type `M[T]`.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, filter)
val list = cursor2.toList
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def toList()(implicit ctx: ExecutionContext) :Future[List[T]] = collect[List]()

  /**
   * Collects all the documents into a collection of type `M[T]`.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, filter)
// return the 3 first documents in a list.
val list = cursor2.toList(3)
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def toList(upTo: Int)(implicit ctx: ExecutionContext) :Future[List[T]] = collect[List](upTo)

  /**
   * Gets the first returned document, if any.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, filter)
val list = cursor2[List].collect()
}}}
   *
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def headOption()(implicit ec: ExecutionContext) :Future[Option[T]] = {
    collect[Iterable](1).map(_.headOption)
  }

  /** Explicitly closes that cursor. It cannot be used again. */
  def close() :Unit
}

class DefaultCursor[T](response: Response, private[api] val mongoConnection: MongoConnection, private[api] val query: Query, private[api] val originalRequest: BufferSequence, private[api] val failoverStrategy: FailoverStrategy)(implicit handler: BSONReaderHandler, reader: RawBSONReader[T], ctx: ExecutionContext) extends Cursor[T] {
  import Cursor.logger
  logger.debug("making default cursor instance from response " + response + ", returned=" + response.reply.numberReturned)

  lazy val iterator :Iterator[T] = handler.handle(response.reply, response.documents)

  def cursorId = Future(response.reply.cursorID)
  def connection = Future(mongoConnection)

  def next :Future[DefaultCursor[T]] = {
    if(response.reply.cursorID != 0) {
      val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
      logger.debug("cursor: calling next on " + response.reply.cursorID + ", op=" + op)
      Failover(RequestMaker(op).copy(channelIdHint=Some(response.info.channelId)), mongoConnection.mongosystem, failoverStrategy).future.map { r => logger.debug("from " + response + " to " + r); new DefaultCursor(r, mongoConnection, query, originalRequest, failoverStrategy) }
    } else {
      logger.debug("throwing no such element exception")
      Future.failed(new NoSuchElementException())
    }
  }

  def hasNext :Boolean = response.reply.cursorID != 0

  def regenerate = {
    logger.debug("regenerating")
    val requestMaker = RequestMaker(query, originalRequest)
    Failover(requestMaker, mongoConnection.mongosystem, failoverStrategy).future.map { response =>
            new DefaultCursor(response, mongoConnection, query, originalRequest, failoverStrategy)}
  }

  def close() = {
    Cursor.logger.debug("sending killcursor on id = " + response.reply.cursorID)
    connection.map(_.send(RequestMaker(KillCursors(Set(response.reply.cursorID)))))
  }
}


/**
 * A [[reactivemongo.api.Cursor]] that holds no document, and which the next cursor is given in the constructor.
 */
class FlattenedCursor[T](futureCursor: Future[Cursor[T]])(implicit ctx: ExecutionContext) extends Cursor[T] {
  import Cursor.logger

  logger.debug("making flattened cursor instance")

  val iterator :Iterator[T] = Iterator.empty

  def cursorId = futureCursor.flatMap(_.cursorId)

  def connection = futureCursor.flatMap(_.connection)

  def next = futureCursor

  def hasNext = true

  def close() = {
    logger.debug("FlattenedCursor closing")
    futureCursor.map(_.close)
  }
}

private[api] class TailableController() {
  private var isStopped = false

  def stopped = isStopped

  def stop() = isStopped = true

  override def toString = {
    "TailableController(" + isStopped + ")"
  }
}

class TailableCursor[T](cursor: DefaultCursor[T], private val controller: TailableController = new TailableController())(implicit ctx: ExecutionContext) extends Cursor[T] {
  import Cursor.logger

  logger.debug("making tailable cursor instance")

  val iterator :Iterator[T] = cursor.iterator

  def connection = cursor.connection

  def cursorId = cursor.cursorId

  def next = {
    logger.debug("calling next on tailable cursor, controller=" + controller)
    if(controller.stopped)
      Future.failed(new NoSuchElementException())
    else {
      val fut = cursor.next.recoverWith {
        case _ =>
          logger.debug("regenerating cursor")
          val f = DelayedFuture(500, MongoConnection.system).flatMap(_ => cursor.regenerate)
          f.onComplete {
            case Failure(e) => e.printStackTrace
            case Success(t) => logger.debug("regenerate is ok")
          }
          f
      }.map(new TailableCursor(_, controller))
      fut.onComplete {
        case Failure(e) => e.printStackTrace
        case Success(t) => logger.debug("next is ok")
      }
      fut
    }
  }

  def hasNext = {
    logger.debug("calling hasNext on tailable cursor");
    !controller.stopped
  }

  def close() = {
    logger.debug("TailableCursor closing")
    cursor.cursorId.map { id =>
      controller.stop
      if(id > 0) {
        Cursor.logger.debug("sending killcursor on id = " + id)
        connection.map(_.send(RequestMaker(KillCursors(Set(id)))))
      }
    }
  }
}

object Cursor {
  import play.api.libs.iteratee._

  private[api] val logger = LoggerFactory.getLogger("reactivemongo.api.Cursor")
  
  /**
   * Flattens the given future [[reactivemongo.api.Cursor]] to a [[reactivemongo.api.FlattenedCursor]].
   */
  def flatten[T](futureCursor: Future[Cursor[T]])(implicit ctx: ExecutionContext) = new FlattenedCursor(futureCursor)

  private def nextElement[T](cursor: Cursor[T])(implicit ec: ExecutionContext) :Future[Option[(Cursor[T], Option[T])]] = {
    if(cursor.iterator.hasNext)
      Future(Some((cursor,Some(cursor.iterator.next))))
    else if (cursor.hasNext)
      cursor.next.map(c => Some((c,None)))
    else Future(None)
  }
}