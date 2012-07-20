= MongoDB Async Driver =

[[https://bitbucket.org/sgodbillon/mongodb-async-driver|MongoAsync]] is a scala driver that provides fully non-blocking and asynchronous I/O operations.
----
\\
== Scale better, use less threads ==

With a classic synchronous database driver, each operation blocks the current thread until a response is received. This model is simple but has a major flaw - it can't scale that much.

Imagine that you have a web application with 10 concurrent accesses to the database. That means you eventually end up with 10 frozen threads at the same time, doing nothing but waiting for a response. A common solution is to rise the number of running threads to handle more requests. Such a waste of resources is not really a problem if your application is not heavily loaded, but what happens if you have 100 or even 1000 more requests to handle, performing each several db queries? The multiplication grows really fast...

The problem is getting more and more obvious while using the new generation of web frameworks. What's the point of using a nifty, powerful, fully asynchronous web framework if all your database accesses are blocking?

MongoAsync is designed to avoid any kind of blocking request. Every operation returns immediately, freeing the running thread and resuming execution when it is over. Accessing the database is not a bottleneck anymore.

== Let the stream flow! ==

The future of the web is streaming data to a very large number clients at the same time. Twitter Stream API is a good example of this paradigm shift that is radically altering the way data is consumed all over the web.

MongoAsync enables you to build such a web application right now. It allows you to stream data both into and from your MongoDB servers.

One scenario could be consuming progressively your collection of documents as needed without filling memory unnecessarily.

But if what you're interested in is live feeds then you can stream a MongoDB capped collection through a websocket, comet or any other streaming protocol. Each time a document is stored into this collection, the webapp broadcasts it to all the interested clients, in a complete non-blocking way.

Moreover, you can now use GridFS as a non-blocking, streaming datastore. MongoAsync retrieves the file, chunk by chunk, and streams it until the client is done or there's no more data. Neither huge memory consumption, nor blocked thread during the process!

== Set up your project dependencies ==

There is a Maven repository at {{{https://bitbucket.org/sgodbillon/repository/raw/master/snapshots/}}}.

If you use SBT, you just have to edit your build.properties and add the following:

{{{
resolvers += "sgodbillon" at "https://bitbucket.org/sgodbillon/repository/raw/master/snapshots/"

libraryDependencies ++= Seq(
  "org.asyncmongo" %% "mongo-async-driver" % "0.1-SNAPSHOT"
)
}}}

== Connect to a database ==

You can get a connection to a server (or a replica set) like this:

{{{
#!scala
def test() {
  import org.asyncmongo.api._

  val connection = MongoConnection( List( "localhost:27017" ) )
  val db = DB("plugin", connection)
  val collection = db("acoll")
}
}}}

The {{{connection}}} reference manages a pool of connections. You can provide a list of one ore more servers; the driver will guess if it's a standalone server or a replica set configuration. Even with one replica node, the driver will probe for other nodes and add them automatically.

== Run a simple query ==

{{{
#!scala
package foo

import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  def listDocs() = {
    val connection = MongoConnection( List( "localhost:27017" ) )
    val db = DB("plugin", connection)
    val collection = db("acoll")
    
    val futureCursor = collection.find(Bson("name" -> BSONString("Jack")))
    
    Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })
  }
}

}}}

The above code deserves some explanations.
First, let's take a look to the {{{collection.find}}} signature:

{{{
#!scala
def find[T, U, V](query: T, fields: Option[U] = None, skip: Int = 0, limit: Int = 0, flags: Int = 0)(implicit writer: BSONWriter[T], writer2: BSONWriter[U], handler: BSONReaderHandler, reader: BSONReader[V]) :Future[Cursor[V]]
}}}

The find method allows you to pass any query object of type {{{T}}}, provided that there is an implicit {{{BSONWriter[T]}}} in the scope. {{{BSONWriter[T]}}} is a typeclass which instances implement a {{{write(document: T)}}} method that returns a {{{ChannelBuffer}}}:

{{{
#!scala
trait BSONWriter[DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}
}}}

{{{BSONReader[V]}}} is the opposite typeclass. It's typically a deserializer that takes a {{{ChannelBuffer}}} and returns an instance of {{{V}}}:

{{{
#!scala
trait BSONReader[DocumentType] {
  def read(buffer: ChannelBuffer) :DocumentType
}
}}}

These two typeclasses allow you to provide different de/serializers for different types.
For this example, we don't need to write specific handlers, so we use the default ones by importing {{{org.asyncmongo.handlers.DefaultBSONHandlers._}}}.

Among {{{DefaultBSONHandlers}}} is a {{{BSONWriter[Bson]}}} that handles the shipped-in BSON library.

You may have noticed that {{{collection.find}}} returns a {{{Future[Cursor[V]]}}}. In fact, //everything in MongoAsync is both non-blocking and asynchronous//. That means each time you make a query, the only immediate result you get is a future of result, so the current thread is not blocked waiting for its completion. You don't need to have //n// threads to process //n// database operations at the same time anymore.

When a query matches too much documents, Mongo sends just a part of them and creates a Cursor  in order to get the next documents. The problem is, how to handle it in a non-blocking, asynchronous, yet elegant way?

That's where the Enumerator/Iteratee pattern (or immutable Producer/Consumer pattern) comes to the rescue!

Let's consider the next statement:

{{{
#!scala
Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
  println("found document: " + DefaultBSONIterator.pretty(doc))
})
}}}

The method {{{Cursor.enumerate[T](Future[Cursor[T]])}}} returns an {{{Enumerator[T]}}}. Enumerators can be seen as //producers// of data: their job is to give chunks of data when data is available. In this case, we get a producer of documents, which source is a future cursor.

Now that we have the producer, we need to define how the documents are processed: that is the {{{Iteratee}}}'s job. Iteratees, as the opposite of Enumerators, are consumers: they are fed in by enumerators and do some computation with the chunks they get.

Here, we write a very simple Iteratee: each time it gets a document, it makes a readable, JSON-like description of the document and prints it on the console. Note that none of these operations are blocking: when the running thread is not processing the callback of our iteratee, it can be used to compute other things.

When this snippet is run, we get the following:

{{{
found document: {
	_id: BSONObjectID["4f899e7eaf527324ab25c56b"],
	name: BSONString(Jack)
}
found document: {
	_id: BSONObjectID["4f899f9baf527324ab25c56c"],
	name: BSONString(Jack)
}
found document: {
	_id: BSONObjectID["4f899f9baf527324ab25c56d"],
	name: BSONString(Jack)
}
found document: {
	_id: BSONObjectID["4f8a269aaf527324ab25c56e"],
	name: BSONString(Jack)
}
found document: {
	_id: BSONObjectID["4f8a269aaf527324ab25c56f"],
	name: BSONString(Jack)
}
found document: {
	_id: BSONObjectID["4fa15559af527324ab25c570"],
	name: BSONString(Jack)
}
}}}

== Go further! ==

MongoAsync makes a heavy usage of the Iteratee library provided by the [[http://www.playframework.org/|Play! Framework 2.0]]. You can dive into [[http://www.playframework.org/documentation/2.0.2/Iteratees|Play's Iteratee documentation]] to learn about this cool piece of software, and make your own Iteratees and Enumerators.

Used in conjonction with stream-aware frameworks, like Play!, you can easily stream the data stored in MongoDB. See the examples and get convinced!

=== Samples ===

* [[https://github.com/sgodbillon/demo-mongo-async|MongoAsync Tailable Cursor, WebSocket and Play 2]]

