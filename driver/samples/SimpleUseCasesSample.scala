package samples

import play.api.libs.iteratee.Iteratee
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands._
import reactivemongo.api.collections.default.BSONCollection
import scala.util.{ Failure, Success }

object SimpleUseCases {
  import scala.concurrent.ExecutionContext.Implicits.global

  // gets an instance of the driver
  // (creates an actor system)
  val driver = new MongoDriver
  /* Gets a connection using this MongoDriver instance.
   * The connection creates a pool of connections.
   * *** USE WITH CAUTION ***
   * Both ReactiveMongo class and MongoConnection class should be instantiated one time in your application
   * (each time you make a ReactiveMongo instance, you create an actor system; each time you create a MongoConnection
   * instance, you create a pool of connections).
   */
  val connection = driver.connection(List("localhost:27017"))

  // Gets a reference to the database "plugin"
  val db = connection("plugin")

  // Gets a reference to the collection "acoll"
  // By default, you get a BSONCollection.
  val collection = db[BSONCollection]("acoll")

  def listDocs() = {
    // select only the documents which field 'firstName' equals 'Jack'
    val query = BSONDocument("firstName" -> "Jack")
    // select only the field 'lastName'
    val filter = BSONDocument(
      "lastName" -> 1,
      "_id" -> 0)

    // get a Cursor[BSONDocument]
    val cursor = collection.find(query, filter).cursor[BSONDocument]
    // let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate().apply(Iteratee.foreach { doc =>
      println("found document: " + BSONDocument.pretty(doc))
    })

    // or, the same with getting a list
    val cursor2 = collection.find(query, filter).cursor[BSONDocument]
    val futurelist = cursor2.collect[List]()
    futurelist.onSuccess {
      case list =>
        val names = list.map(_.getAs[String]("lastName").get)
        println("got names: " + names)
    }
  }

  def count() = {
    // select only the documents which company name equals 'Zenexity'
    val query = BSONDocument("company.name" -> "Zenexity")
    val futureCount = db.command(
      Count(
        // run this command on the given collection
        collection.name,
        // ... with the query we wrote above
        Some(query)))
    futureCount.map { count =>
      println("found " + count + " documents which company name is 'Zenexity'")
    }
  }

  def querybuilder() = {
    val cursor =
      collection.
        // select only the documents which company name equals 'Zenexity'
        find(BSONDocument("company.name" -> "Zenexity")).
        // sort by lastName
        sort(BSONDocument("lastName" -> 1)).
        // retrieve only lastName and firstName
        projection(BSONDocument(
          "lastName" -> 1,
          "firstName" -> 1,
          "_id" -> 0)).
        // get a Cursor[BSONDocument]
        cursor[BSONDocument]

    // get a future list of BSONDocuments
    val futurelist = cursor.collect[List]()
    futurelist.onSuccess {
      case list => println(list.map(doc => BSONDocument.pretty(doc)))
    }
  }

  def insert() = {
    val document = BSONDocument(
      "firstName" -> "Stephane",
      "lastName" -> "Godbillon",
      "age" -> 29,
      "company" -> BSONDocument(
        "name" -> "Zenexity",
        "address" -> "56 rue Saint Lazare 75009 Paris"))

    val future = collection.insert(document)
    future.onComplete {
      case Failure(e) => throw e
      case Success(lastError) => {
        println("successfully inserted document: " + lastError)
      }
    }
  }

  def insertThenCount() = {
    val document = BSONDocument(
      "firstName" -> "Stephane",
      "lastName" -> "Godbillon",
      "age" -> 28,
      "company" -> BSONDocument(
        "name" -> "Zenexity",
        "address" -> "56 rue Saint Lazare 75009 Paris"))

    // get the future result of the insertion
    val futureInsert = collection.insert(document)

    // when the insertion is successfully done, we send the count command
    val futureInsertThenCount = futureInsert.flatMap(lasterror => {
      println("successfully inserted document (lasterror is " + lasterror + ")")
      val count = Count(
        collection.name,
        Some(BSONDocument(
          "company.name" -> "Zenexity")))
      // get the future count
      db.command(count)
    })

    futureInsertThenCount.onComplete {
      case Failure(e) => throw e
      case Success(count) => {
        println("successfully inserted document, now there are " + count + " zenexity guys here")
      }
    }
  }

  def update() = {
    val selector = BSONDocument("name" -> "Jack")

    val modifier = BSONDocument(
      "$set" -> BSONDocument(
        "lastName" -> "London",
        "firstName" -> "Jack"),
      "$unset" -> BSONDocument(
        "name" -> 1))

    // get a future update
    val futureUpdate = collection.update(selector, modifier, GetLastError())

    // get a cursor of documents that have lastName: "London" and firstName: "Jack". Our updated document should be included.
    val futureCursor = futureUpdate.map { lastError =>
      val updatedDocumentSelector = BSONDocument(
        "lastName" -> "London",
        "firstName" -> "Jack")

      collection.find(updatedDocumentSelector).cursor[BSONDocument]
    }

    // let's enumerate this cursor and print a readable representation of each document in the response
    val enumerator = Cursor.flatten(futureCursor).enumerate()
    enumerator(Iteratee.foreach { doc =>
      println("found document: " + BSONDocument.pretty(doc))
    })
  }

  def remove() = {
    // let's remove all documents that have company.name = Zenexity
    val selector = BSONDocument(
      "company.name" -> "Zenexity")

    val futureRemove = collection.remove(selector, GetLastError(), false)

    futureRemove.onComplete {
      case Failure(e) => throw e
      case Success(lasterror) => {
        println("successfully removed document")
      }
    }
  }

  // finds all documents with lastName = Godbillon and replace lastName with GODBILLON
  def findAndModify() = {
    val selector = BSONDocument(
      "lastName" -> "Godbillon")

    val modifier = BSONDocument(
      "$set" -> BSONDocument("lastName" -> "GODBILLON"))

    val command = FindAndModify(
      collection.name,
      selector,
      Update(modifier, false))

    db.command(command).onComplete {
      case Failure(error) => {
        throw new RuntimeException("got an error while performing findAndModify", error)
      }
      case Success(maybeDocument) => println("findAndModify successfully done with original document = " +
        // if there is an original document returned, print it in a pretty format
        maybeDocument.map(doc => {
          // get a BSONIterator (lazy BSON parser) of this document
          // stringify it with DefaultBSONIterator.pretty
          BSONDocument.pretty(doc)
        }))
    }
  }
}
