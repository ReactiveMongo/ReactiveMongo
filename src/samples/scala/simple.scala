package foo

import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Await
import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.protocol.commands._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {
  val connection = MongoConnection( List( "localhost:27016" ) )
  val db = DB("plugin", connection)
  val collection = db("acoll")

  // just for running examples - avoid this in production since it blocks the current thread
  Await.result(connection.waitForPrimary(Timeout(5 seconds)), 5 seconds)

  def listDocs() = {
    // get a Future[Cursor[DefaultBSONIterator]]
    val futureCursor = collection.find(
      Bson("name" -> BSONString("Jack")),
      // select only the field 'name'
      Some(Bson(
        "name" -> BSONInteger(1),
        "_id" -> BSONInteger(0)
      ))
    )

    // let's enumerate this cursor and print a readable representation of each document in the response
    Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })
  }

  def insert() = {
    val document = Bson(
      "firstName" -> BSONString("Stephane"),
      "lastName" -> BSONString("Godbillon"),
      "age" -> BSONInteger(28),
      "company" -> BSONDocument(Bson(
        "name" -> BSONString("Zenexity"),
        "address" -> BSONString("56 rue Saint Lazare 75009 Paris")
      ).makeBuffer)
    )

    val future = collection.insert(document, GetLastError())
    future.onComplete {
      case Left(e) => throw e
      case Right(lastError) => {
        println("successfully inserted document")
      }
    }
  }

  def insertThenCount() = {
    val document = Bson(
      "firstName" -> BSONString("Stephane"),
      "lastName" -> BSONString("Godbillon"),
      "age" -> BSONInteger(28),
      "company" -> BSONDocument(Bson(
        "name" -> BSONString("Zenexity"),
        "address" -> BSONString("56 rue Saint Lazare 75009 Paris")
      ).makeBuffer)
    )

    // get the future result of the insertion
    val futureInsert = collection.insert(document, GetLastError())

    // when the insertion is successfully done, we send the count command
    val futureInsertThenCount = futureInsert.flatMap(lasterror => {
      println("successfully inserted document (lasterror is " + lasterror + ")")
      val count = Count(
        collection.collectionName,
        Some(Bson(
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
    val selector = Bson("name" -> BSONString("Jack"))

    val modifier = Bson(
      "$set" -> BSONDocument(Bson(
        "lastName" ->BSONString("London"),
        "firstName" -> BSONString("Jack")).makeBuffer),
      "$unset" -> BSONDocument(Bson(
        "name" -> BSONInteger(1)).makeBuffer)
    )

    // get a future update
    val futureUpdate = collection.update(selector, modifier, GetLastError())

    // get a future cursor of documents that have lastName: "London" and firstName: "Jack". Our updated document should be included.
    val futureCursor = futureUpdate.flatMap { lastError =>
      val updatedDocumentSelector = Bson(
        "lastName" -> BSONString("London"),
        "firstName" -> BSONString("Jack"))

      collection.find(updatedDocumentSelector)
    }

    // let's enumerate this cursor and print a readable representation of each document in the response
    Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })
  }

  def remove() = {
    // let's remove all documents that have company.name = Zenexity
    val selector = Bson(
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
    val selector = Bson(
      "lastName" -> BSONString("Godbillon"))

    val modifier = Bson(
      "$set" -> BSONDocument(
        Bson("lastName" -> BSONString("GODBILLON")).makeBuffer))

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
          val bsonIterator = DefaultBSONReader.read(doc.value)
          // stringify it with DefaultBSONIterator.pretty
          DefaultBSONIterator.pretty(bsonIterator)
        })
      )
    }
  }
}
