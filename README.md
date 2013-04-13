# ReactiveMongo - Asynchronous & Non-Blocking Scala Driver for MongoDB

![Build Status](https://travis-ci.org/zenexity/ReactiveMongo.png)

[ReactiveMongo](https://github.com/zenexity/ReactiveMongo/) is a scala driver that provides fully non-blocking and asynchronous I/O operations.

## Scale better, use less threads

With a classic synchronous database driver, each operation blocks the current thread until a response is received. This model is simple but has a major flaw - it can't scale that much.

Imagine that you have a web application with 10 concurrent accesses to the database. That means you eventually end up with 10 frozen threads at the same time, doing nothing but waiting for a response. A common solution is to rise the number of running threads to handle more requests. Such a waste of resources is not really a problem if your application is not heavily loaded, but what happens if you have 100 or even 1000 more requests to handle, performing each several db queries? The multiplication grows really fast...

The problem is getting more and more obvious while using the new generation of web frameworks. What's the point of using a nifty, powerful, fully asynchronous web framework if all your database accesses are blocking?

ReactiveMongo is designed to avoid any kind of blocking request. Every operation returns immediately, freeing the running thread and resuming execution when it is over. Accessing the database is not a bottleneck anymore.

## Let the stream flow!

The future of the web is in streaming data to a very large number of clients simultaneously. Twitter Stream API is a good example of this paradigm shift that is radically altering the way data is consumed all over the web.

ReactiveMongo enables you to build such a web application right now. It allows you to stream data both into and from your MongoDB servers.

One scenario could be consuming progressively your collection of documents as needed without filling memory unnecessarily.

But if what you're interested in is live feeds then you can stream a MongoDB capped collection through a websocket, comet or any other streaming protocol. Each time a document is stored into this collection, the webapp broadcasts it to all the interested clients, in a complete non-blocking way.

Moreover, you can now use GridFS as a non-blocking, streaming datastore. ReactiveMongo retrieves the file, chunk by chunk, and streams it until the client is done or there's no more data. Neither huge memory consumption, nor blocked thread during the process!

## Step By Step Example 

Let's show a simple use case: print the documents of a capped collection.

### Prerequisites

We assume that you got a running MongoDB instance. If not, get [the latest MongoDB binaries](http://www.mongodb.org/downloads) and unzip the archive. Then you can launch the database:

```
$ mkdir data
$ ./bin/mongod --dbpath data
```

This will start a standalone MongoDB instance that stores its data in the ```data``` directory and listens on the TCP port 27017.

### Set up your project dependencies

If you use SBT, you just have to edit your build.sbt and add the following:

```scala
libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.8"
)
```

If you want to use the latest snapshot, add the following instead:

```scala
resolvers += "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.9-SNAPSHOT"
)
```

### Connect to the database

You can get a connection to a server (or a replica set) like this:

```scala
def test() {
  import reactivemongo.api._
  import scala.concurrent.ExecutionContext.Implicits.global
  
  val connection = MongoConnection( List( "localhost:27017" ) )
  val db = connection("plugin")
  val collection = db("acoll")
}
```

The `connection` reference manages a pool of connections. You can provide a list of one ore more servers; the driver will guess if it's a standalone server or a replica set configuration. Even with one replica node, the driver will probe for other nodes and add them automatically.

### Run a simple query

```scala
package foo

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {
  import scala.concurrent.ExecutionContext.Implicits.global

  def listDocs() = {
    // select only the documents which field 'firstName' equals 'Jack'
    val query = BSONDocument("firstName" -> BSONString("Jack"))

    // get a Cursor[BSONDocument]
    val cursor = collection.find(query)
    // let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate.apply(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc.bsonIterator))
    })

    // or, the same with getting a list
    val cursor2 = collection.find(query)
    val futurelist = cursor2.toList
    futurelist.onSuccess {
      case list =>
        val names = list.map(_.getAs[BSONString]("lastName").get.value)
        println("got names: " + names)
    }
  }
}
```

The above code deserves some explanations.
First, let's take a look to the `collection.find` signature:

```scala
def find[Qry, Rst](query: Qry)(implicit writer: RawBSONWriter[Qry], handler: BSONReaderHandler, reader: RawBSONReader[Rst]) :FlattenedCursor[Rst]
```

The find method allows you to pass any query object of type `Qry`, provided that there is an implicit `RawBSONWriter[Qry]` in the scope. `RawBSONWriter[Qry]` is a typeclass which instances implement a `write(document: Qry)` method that returns a `ChannelBuffer`:

```scala
trait RawBSONWriter[-DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}
```

`RawBSONReader[Rst]` is the opposite typeclass. It's typically a deserializer that takes a `ChannelBuffer` and returns an instance of `Rst`:

```scala
trait RawBSONReader[+DocumentType] {
  def read(buffer: ChannelBuffer) :DocumentType
}
```

Of course, you can rely on the shipped-in BSON library. There are two subtraits that enable to de/serialize your models using `BSONDocuments`:

```scala
trait BSONWriter[-DocumentType] extends RawBSONWriter[DocumentType] {
  def toBSON(document: DocumentType) :BSONDocument
}
```
```scala
trait BSONReader[+DocumentType] extends RawBSONReader[DocumentType] {
  def fromBSON(doc: BSONDocument) :DocumentType
}
```

These two typeclasses allow you to provide different de/serializers for different types.
For this example, we don't need to write specific handlers, so we use the default ones by importing `reactivemongo.bson.handlers.DefaultBSONHandlers._`.

Among `DefaultBSONHandlers` is a `BSONWriter[BSONDocument]` that handles the shipped-in BSON library.

You may have noticed that `collection.find` returns a `FlattenedCursor[Rst]`. This cursor is actually a future cursor. In fact, *everything in ReactiveMongo is both non-blocking and asynchronous*. That means each time you make a query, the only immediate result you get is a future of result, so the current thread is not blocked waiting for its completion. You don't need to have *n* threads to process *n* database operations at the same time anymore.

When a query matches too much documents, Mongo sends just a part of them and creates a Cursor in order to get the next documents. The problem is, how to handle it in a non-blocking, asynchronous, yet elegant way?

Obviously ReactiveMongo's cursor provides helpful methods to build a collection (like a list) from it, so we could write:

```scala
val futureList :Future[List] = cursor.toList
futureList.map { list =>
  println("ok, got the list: " + list)
}
```

As always, this is perfectly non-blocking... but what if we want to process the returned documents on the fly, without creating a potentially huge list in memory?

That's where the Enumerator/Iteratee pattern (or immutable Producer/Consumer pattern) comes to the rescue!

Let's consider the next statement:

```scala
cursor.enumerate.apply(Iteratee.foreach { doc =>
  println("found document: " + DefaultBSONIterator.pretty(doc))
})
```

The method `cursor.enumerate` returns an `Enumerator[T]`. Enumerators can be seen as //producers// of data: their job is to give chunks of data when data is available. In this case, we get a producer of documents, which source is a future cursor.

Now that we have the producer, we need to define how the documents are processed: that is the `Iteratee`'s job. Iteratees, as the opposite of Enumerators, are consumers: they are fed in by enumerators and do some computation with the chunks they get.

Here, we write a very simple Iteratee: each time it gets a document, it makes a readable, JSON-like description of the document and prints it on the console. Note that none of these operations are blocking: when the running thread is not processing the callback of our iteratee, it can be used to compute other things.

When this snippet is run, we get the following:

```
found document: {
	_id: BSONObjectID["4f899e7eaf527324ab25c56b"],
  firstName: BSONString(Jack),
	lastName: BSONString(London)
}
found document: {
	_id: BSONObjectID["4f899f9baf527324ab25c56c"],
  firstName: BSONString(Jack),
	lastName: BSONString(Kerouac)
}
found document: {
	_id: BSONObjectID["4f899f9baf527324ab25c56d"],
  firstName: BSONString(Jack),
	lastName: BSONString(Nicholson)
}
```

## Go further!

ReactiveMongo makes a heavy usage of the Iteratee library provided by the [Play! Framework 2.1](http://www.playframework.org/). You can dive into [Play's Iteratee documentation](http://www.playframework.org/documentation/2.0.2/Iteratees) to learn about this cool piece of software, and make your own Iteratees and Enumerators.

Used in conjonction with stream-aware frameworks, like Play!, you can easily stream the data stored in MongoDB. See the examples and get convinced!

### Samples

* [ReactiveMongo Tailable Cursor, WebSocket and Play 2](https://github.com/sgodbillon/reactivemongo-tailablecursor-demo)
* [Full Web Application featuring basic CRUD operations and GridFS streaming](https://github.com/sgodbillon/reactivemongo-demo-app)
