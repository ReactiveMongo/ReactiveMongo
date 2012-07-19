package foo

import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  def listDocs() = {
    val connection = MongoConnection( List( "localhost:27016" ) )
    val db = DB("plugin", connection)
    val collection = db("acoll")

    // get a Future[Cursor[DefaultBSONIterator]]
    val futureCursor = collection.find(Bson("name" -> BSONString("Jack")))

    // let's enumerate this cursor and print a readable representation of each document in the response
    Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })
  }
}
