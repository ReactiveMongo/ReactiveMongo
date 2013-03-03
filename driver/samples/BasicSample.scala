package samples

import play.api.libs.iteratee.Iteratee

import reactivemongo.api.MongoConnection
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global

object Samples {

  val connection = MongoConnection(List("localhost"))

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
    val cursor = collection.find(query, filter).cursor
    // Let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate.apply(Iteratee.foreach { doc =>
      println("found document: " + BSONDocument.pretty(doc))
    })

    // Or, the same with getting a list
    val cursor2 = collection.find(query, filter).cursor
    val futureList = cursor.toList
    futureList.map { list =>
      list.foreach { doc =>
        println("found document: " + BSONDocument.pretty(doc))
      }
    }
  }
}
