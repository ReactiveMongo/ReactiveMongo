package foo

import scala.concurrent.Await
import scala.concurrent.util._
import scala.concurrent.util.duration._
import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.protocol.commands._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {
  import scala.concurrent.ExecutionContext.Implicits.global // TODO create own ExecutionContext

  val connection = MongoConnection( List( "localhost:27016" ) )
  val db = DB("plugin", connection)
  val collection = db("acoll")

  // just for running examples - avoid this in production since it blocks the current thread
  Await.result(connection.waitForPrimary(5 seconds), 5 seconds)

  def listDocs() = {
    // select only the documents which field 'name' equals 'Jack'
    val query = BSONDocument("name" -> BSONString("Jack"))
    // select only the field 'name'
    val filter = BSONDocument(
      "name" -> BSONInteger(1),
      "_id" -> BSONInteger(0)
    )

    // get a Cursor[DefaultBSONIterator]
    val cursor = collection.find(query, Some(filter))
    // let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate.apply(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc.bsonIterator))
    })

    // or, the same with getting a list
    val cursor2 = collection.find(query, Some(filter))
    val futurelist = cursor2.toList
    futurelist.onSuccess {
      case list => println(list)
    }
  }

  def insert() = {
    val document = BSONDocument(
      "firstName" -> BSONString("Stephane"),
      "lastName" -> BSONString("Godbillon"),
      "age" -> BSONInteger(28),
      "company" -> BSONDocument(
        "name" -> BSONString("Zenexity"),
        "address" -> BSONString("56 rue Saint Lazare 75009 Paris")
      )
    )

    val future = collection.insert(document, GetLastError())
    future.onComplete {
      case Left(e) => throw e
      case Right(lastError) => {
        println("successfully inserted document: "  + lastError)
      }
    }
  }

  def insertThenCount() = {
    val document = BSONDocument(
      "firstName" -> BSONString("Stephane"),
      "lastName" -> BSONString("Godbillon"),
      "age" -> BSONInteger(28),
      "company" -> BSONDocument(
        "name" -> BSONString("Zenexity"),
        "address" -> BSONString("56 rue Saint Lazare 75009 Paris")
      )
    )

    // get the future result of the insertion
    val futureInsert = collection.insert(document, GetLastError())

    // when the insertion is successfully done, we send the count command
    val futureInsertThenCount = futureInsert.flatMap(lasterror => {
      println("successfully inserted document (lasterror is " + lasterror + ")")
      val count = Count(
        collection.collectionName,
        Some(BSONDocument(
          "company.name" -> BSONString("Zenexity")
        )))
      // get the future count
      collection.command(count)
    })

    futureInsertThenCount.onComplete {
      case Left(e) => throw e
      case Right(count) => {
        println("successfully inserted document, now there are " + count + " zenexity guys here")
      }
    }
  }

  def update() = {
    val selector = BSONDocument("name" -> BSONString("Jack"))

    val modifier = BSONDocument(
      "$set" -> BSONDocument(
        "lastName" ->BSONString("London"),
        "firstName" -> BSONString("Jack")),
      "$unset" -> BSONDocument(
        "name" -> BSONInteger(1))
    )

    // get a future update
    val futureUpdate = collection.update(selector, modifier, GetLastError())

    // get a cursor of documents that have lastName: "London" and firstName: "Jack". Our updated document should be included.
    val futureCursor = futureUpdate.map { lastError =>
      val updatedDocumentSelector = BSONDocument(
        "lastName" -> BSONString("London"),
        "firstName" -> BSONString("Jack"))

      collection.find(updatedDocumentSelector)
    }

    // let's enumerate this cursor and print a readable representation of each document in the response
    val enumerator = Cursor.flatten(futureCursor).enumerate
    enumerator(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc.bsonIterator))
    })
  }

  def remove() = {
    // let's remove all documents that have company.name = Zenexity
    val selector = BSONDocument(
      "company.name" -> BSONString("Zenexity"))

    val futureRemove = collection.remove(selector, GetLastError(), false)

    futureRemove.onComplete {
      case Left(e) => throw e
      case Right(lasterror) => {
        println("successfully removed document")
      }
    }
  }

  // finds all documents with lastName = Godbillon and replace lastName with GODBILLON
  def findAndModify() = {
    val selector = BSONDocument(
      "lastName" -> BSONString("Godbillon"))

    val modifier = BSONDocument(
      "$set" -> BSONDocument("lastName" -> BSONString("GODBILLON")))

    val command = FindAndModify(
      collection.collectionName,
      selector,
      Update(modifier, false))

    collection.command(command).onComplete {
      case Left(error) => throw error
      case Right(maybeDocument) => println("findAndModify successfully done with original document = " +
        // if there is an original document returned, print it in a pretty format
        maybeDocument.map(doc => {
          // get a BSONIterator (lazy BSON parser) of this document 
          // stringify it with DefaultBSONIterator.pretty
          DefaultBSONIterator.pretty(doc.bsonIterator)
        })
      )
    }
  }
}
