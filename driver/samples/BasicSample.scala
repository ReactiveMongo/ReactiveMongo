package samples

import play.api.libs.iteratee.Iteratee

import reactivemongo.api.MongoDriver
import reactivemongo.bson._

import scala.concurrent.{Future}
import scala.concurrent.ExecutionContext.Implicits.global

object Samples {

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
  val connection = driver.connection(List("localhost"))

  // Gets a reference to the database "plugin"
  val db = connection("plugin")

  // Gets a reference to the collection "acoll"
  // By default, you get a BSONCollection.
  val collection = db("acoll")

  def listDocs() = {
    // Select only the documents which field 'firstName' equals 'Jack'
    val query = BSONDocument("firstName" -> "Jack")
    // select only the field 'lastName'
    val filter = BSONDocument(
      "lastName" -> 1,
      "_id" -> 0)

    // Get a cursor of BSONDocuments
    val cursor = collection.find(query, filter).cursor[BSONDocument]
    // Let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate().apply(Iteratee.foreach { doc =>
      println("found document: " + BSONDocument.pretty(doc))
    })

    // Or, the same with getting a list
    val cursor2 = collection.find(query, filter).cursor[BSONDocument]
    val futureList: Future[List[BSONDocument]] = cursor.collect[List]()
    futureList.map { list =>
      list.foreach { doc =>
        println("found document: " + BSONDocument.pretty(doc))
      }
    }
  }
}
