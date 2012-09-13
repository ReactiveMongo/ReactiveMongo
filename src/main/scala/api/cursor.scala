package reactivemongo.api

import reactivemongo.bson.handlers._
import reactivemongo.core.protocol._
import reactivemongo.utils.ExtendedFutures._

import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.LoggerFactory
import play.api.libs.iteratee._
import scala.concurrent.{ExecutionContext, Future}

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
      println("found document: " + DefaultBSONIterator.pretty(doc.bsonIterator))
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
  /** An iterator on the last fetched documents. */
  val iterator: Iterator[T]

  val cursorId: Option[Long]
  val connection: Option[MongoConnection]

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
  println("found document: " + DefaultBSONIterator.pretty(doc))
})
}
}}}
   *
   * It is worth diving into the [[https://github.com/playframework/Play20/wiki/Iteratees Play! 2.0 Iteratee documentation]].
   *
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][T] typeclass for handling it has to be in the scope.
   *
*/
  def enumerate()(implicit ctx: ExecutionContext) :Enumerator[T] =
    if(hasNext)
      Enumerator.unfoldM(this) { cursor =>
        Cursor.nextElement(cursor)
      } &> Enumeratee.collect { case Some(e) => e }
    else Enumerator.eof

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

  /** Explicitly closes that cursor. */
  def close = if(hasNext && cursorId.isDefined && connection.isDefined) {
      connection.get.send(RequestMaker(KillCursors(Set(cursorId.get))))
  }

  /*def nextElement()(implicit ec: ExecutionContext) :Future[Option[(Cursor[T], T)]] = {
    Cursor.nextElement(this).flatMap {
      case result @ Some((cursor, Some(e))) => Future(Some(cursor -> e))
      case Some((cursor, None)) => Cursor.nextElement(cursor).flatMap { // we just got a new cursor. If this cursor has no element, then the Mongo cursor is done.
        case result @ Some((cursor, Some(e))) => Future(Some(cursor -> e))
        case _ => Future(None)
      }
      case _ => Future(None)
    }
  }*/
}

class DefaultCursor[T](response: Response, private[api] val mongoConnection: MongoConnection, private[api] val query: Query, private[api] val originalRequest: ChannelBuffer, private[api] val failoverStrategy: FailoverStrategy)(implicit handler: BSONReaderHandler, reader: RawBSONReader[T], ctx: ExecutionContext) extends Cursor[T] {
  import Cursor.logger

  logger.trace("response is " + response + response.reply)
  lazy val iterator :Iterator[T] = handler.handle(response.reply, response.documents)

  val cursorId = Some(response.reply.cursorID)
  override val connection = Some(mongoConnection)

  def next :Future[DefaultCursor[T]] = {
    if(response.reply.cursorID != 0) {
      logger.debug("cursor: calling next on " + response.reply.cursorID)
      val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
      Failover(RequestMaker(op).copy(channelIdHint=Some(response.info.channelId)), mongoConnection.mongosystem, failoverStrategy).future.map { response => new DefaultCursor(response, mongoConnection, query, originalRequest, failoverStrategy) }
    } else throw new NoSuchElementException()
  }

  def hasNext :Boolean = response.reply.cursorID != 0
}


/**
 * A [[reactivemongo.api.Cursor]] that holds no document, and which the next cursor is given in the constructor.
 */
class FlattenedCursor[T](futureCursor: Future[Cursor[T]]) extends Cursor[T] {
  val iterator :Iterator[T] = Iterator.empty

  val cursorId = None

  val connection = None

  def next = futureCursor

  def hasNext = true
}

object Cursor {
  import play.api.libs.iteratee._
  import play.api.libs.concurrent.{Promise => PlayPromise, _}

  private[api] val logger = LoggerFactory.getLogger("Cursor")

  /**
   * Flattens the given future [[reactivemongo.api.Cursor]] to a [[reactivemongo.api.FlattenedCursor]].
   */
  def flatten[T](futureCursor: Future[Cursor[T]]) = new FlattenedCursor(futureCursor)

  private def nextElement[T](cursor: Cursor[T])(implicit ec: ExecutionContext) :Future[Option[(Cursor[T], Option[T])]] = {
    if(cursor.iterator.hasNext)
      Future(Some((cursor,Some(cursor.iterator.next))))
    else if (cursor.hasNext)
      cursor.next.map(c => Some((c,None)))
    else
      Future(None)
  }
}