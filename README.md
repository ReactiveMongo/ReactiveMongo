# ReactiveMongo - Asynchronous & Non-Blocking Scala Driver for MongoDB

## Query DSL Fork

This is a fork of the [original ReactiveMongo project](https://github.com/zenexity/ReactiveMongo/) that adds a _Criteria DSL_ option to [ReactiveMongo](https://github.com/zenexity/ReactiveMongo/).

### Original Query Syntax

The `reactivemongo.api.collections.GenericCollection` type provides the `find` method used to find documents matching a criteria.  It is this interaction which the DSL targets.  Originally, providing a selector to `find` had an interaction similar to:

```scala
  val cursor = collection.find(BSONDocument("firstName" -> "Jack")).cursor[BSONDocument]
```

This is, of course, still supported as the DSL does not preclude this usage.

### Criteria DSL

What the DSL *does* provide is the ablity to formulate queries thusly:

```scala
  // Using an Untyped.criteria
  {
  import Untyped._

  val untyped = criteria.firstName === "Jack" && criteria.age >= 18;
  val anotherCursor = collection.find(untyped).cursor[BSONDocument]
  }
```

I plan on doing a type-safe version using Scala macros at some point if requested or the `Untyped` version proves unsastifactory.

What follows is the original README.md from [ReactiveMongo](https://github.com/zenexity/ReactiveMongo/) as it was originally authored.


[![Build Status](https://travis-ci.org/zenexity/ReactiveMongo.png?branch=master)](https://travis-ci.org/zenexity/ReactiveMongo)

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

Let's show a simple use case: print the documents of a collection.

### Prerequisites

We assume that you got a running MongoDB instance. If not, get [the latest MongoDB binaries](http://www.mongodb.org/downloads) and unzip the archive. Then you can launch the database:

```
$ mkdir data
$ ./bin/mongod --dbpath data
```

This will start a standalone MongoDB instance that stores its data in the ```data``` directory and listens on the TCP port 27017.

### Set up your project dependencies

ReactiveMongo is available on [Maven Central](http://search.maven.org/#browse%7C1306790).

If you use SBT, you just have to edit `build.sbt` and add the following:

```scala
libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.9"
)
```

Or if you want to be on the bleeding edge using snapshots:

```scala
resolvers += "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "0.10-SNAPSHOT"
)
```

### Connect to the database

You can get a connection to a server (or a replica set) like this:

```scala
def connect() {
  import reactivemongo.api._
  import scala.concurrent.ExecutionContext.Implicits.global

  // gets an instance of the driver
  // (creates an actor system)
  val driver = new MongoDriver
  val connection = driver.connection(List("localhost"))

  // Gets a reference to the database "plugin"
  val db = connection("plugin")

  // Gets a reference to the collection "acoll"
  // By default, you get a BSONCollection.
  val collection = db("acoll")
}
```

A `MongoDriver` instance manages an actor system; a `connection` manages a pool of connections. In general, MongoDriver or create a MongoConnection are never instantiated more than once. You can provide a list of one ore more servers; the driver will guess if it's a standalone server or a replica set configuration. Even with one replica node, the driver will probe for other nodes and add them automatically.
### Run a simple query

```scala
import reactivemongo.api._
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global

def listDocs() = {
  // Select only the documents which field 'firstName' equals 'Jack'
  val query = BSONDocument("firstName" -> "Jack")
  // select only the fields 'lastName' and '_id'
  val filter = BSONDocument(
    "lastName" -> 1,
    "_id" -> 1)

  // Get a cursor of BSONDocuments
  val cursor = collection.find(query, filter).cursor[BSONDocument]
  /* Let's enumerate this cursor and print a readable
   * representation of each document in the response */
  cursor.enumerate.apply(Iteratee.foreach { doc =>
    println("found document: " + BSONDocument.pretty(doc))
  })

  // Or, the same with getting a list
  val cursor2 = collection.find(query, filter).cursor[BSONDocument]
  val futureList: Future[List[BSONDocument]] = cursor.toList
  futureList.map { list =>
    list.foreach { doc =>
      println("found document: " + BSONDocument.pretty(doc))
    }
  }
}
```

The above code deserves some explanations. First, let's take a look to the `collection.find` signature:

```scala
def find[S](selector: S)(implicit wrt: BSONDocumentWriter[S]): BSONQueryBuilder
```

The find method allows you to pass any query object of type `S`, provided that there is an implicit `BSONDocumentWriter[S]` in the scope. `BSONDocumentWriter[S]` is a typeclass which instances implement a `write(document: S)` method that returns a `BSONDocument`. It can be described as follows:

```scala
trait BSONDocumentWriter[DocumentType] {
  def write(document: DocumentType): BSONDocument
}
```

Obviously, there is a default writer for `BSONDocuments` so you can give a `BSONDocument` as an argument for the `find` method.

The find method returns a QueryBuilder – the query is therefore not performed yet. It gives you the opportunity to add options to the query, like a sort order, projection, flags... When your query is ready to be sent to MongoDB, you may just call the `cursor` method on it. This method is parametrized with the type which the response documents will be deserialized to. A `BSONDocumentReader[T]` must be implicitly available in the scope for that type. As opposed to `BSONDocumentWriter[T]`, a reader is typically a deserializer that takes a `BSONDocument` and returns an instance of `T`:

```scala
trait BSONDocumentReader[DocumentType] {
  def read(buffer: BSONDocument): DocumentType
}
```

Like for `BSONDocumentWriter[T]`, there is a default reader for `BSONDocument` in the package `reactivemongo.bson`.

You may have noticed that the `cursor[T]` metho returns a `FlattenedCursor[T]`. This cursor is actually a future cursor. In fact, _everything in ReactiveMongo is both non-blocking and asynchronous_. That means each time you make a query, the only immediate result you get is a future of result, so the current thread is not blocked waiting for its completion. You don't need to have _n_ threads to process _n_ database operations at the same time anymore.

When a query matches too much documents, Mongo sends just a part of them and creates a Cursor in order to get the next documents. The problem is, how to handle it in a non-blocking, asynchronous, yet elegant way?

Obviously ReactiveMongo's cursor provides helpful methods to build a collection (like a list) from it, so we could write:

```scala
val futureList: Future[List[BSONDocument]] = cursor.toList
futureList.map { list =>
  println("ok, got the list: " + list)
}
```

As always, this is perfectly non-blocking... but what if we want to process the returned documents on the fly, without creating a potentially huge list in memory?

That's where the Enumerator/Iteratee pattern (or immutable Producer/Consumer pattern) comes to the rescue!

Let's consider the following statement:

```scala
cursor.enumerate.apply(Iteratee.foreach { doc =>
  println("found document: " + BSONDocument.pretty(doc))
})
```

The method `cursor.enumerate` returns an `Enumerator[T]`. Enumerators can be seen as _producers_ of data: their job is to give chunks of data when data is available. In this case, we get a producer of documents, which source is a future cursor.

Now that we have the producer, we need to define how the documents are processed: that is the `Iteratee`'s job. Iteratees, as the opposite of Enumerators, are consumers: they are fed in by enumerators and do some computation with the chunks they get.

Here, we write a very simple Iteratee: each time it gets a document, it makes a readable, JSON-like description of the document and prints it on the console. Note that none of these operations are blocking: when the running thread is not processing the callback of our iteratee, it can be used to compute other things.

When this snippet is run, we get the following:

    found document: {
      _id: BSONObjectID("4f899e7eaf527324ab25c56b"),
      lastName: BSONString(London)
    }
    found document: {
      _id: BSONObjectID("4f899f9baf527324ab25c56c"),
      lastName: BSONString(Kerouac)
    }
    found document: {
      _id: BSONObjectID("4f899f9baf527324ab25c56d"),
      lastName: BSONString(Nicholson)
    }

## Go further!

There is a pretty complete [Scaladoc](http://reactivemongo.org/releases/0.9/api/index.html) available. The code is accessible from the [Github repository](https://github.com/zenexity/ReactiveMongo). And obviously, don't hesitate to ask questions in the [ReactiveMongo Google Group](https://groups.google.com/forum/?fromgroups#!forum/reactivemongo)!

ReactiveMongo makes a heavy usage of the [Iteratee](http://www.playframework.com/documentation/2.1.1/Iteratees) library. Although it is developped by the [Play! Framework](http://www.playframework.com) team, it does _not_ depend on any other part of the framework. You can dive into [Play's Iteratee documentation](http://www.playframework.com/documentation/2.1.1/Iteratees) to learn about this cool piece of software, and make your own Iteratees and Enumerators.

Used in conjonction with stream-aware frameworks, like Play!, you can easily stream the data stored in MongoDB. For Play, there is a [ReactiveMongo Plugin](https://github.com/zenexity/Play-ReactiveMongo) that brings some cool stuff, like JSON-specialized collection and helpers for GridFS. See the examples and get convinced!

### Samples

These sample applications are kept up to date with the latest driver version. They are built upon Play 2.1.

* [ReactiveMongo Tailable Cursor, WebSocket and Play 2](https://github.com/sgodbillon/reactivemongo-tailablecursor-demo)
* [Full Web Application featuring basic CRUD operations and GridFS streaming](https://github.com/sgodbillon/reactivemongo-demo-app)
