package reactivemongo.api

/**
 * A MongoDB Collection. You should consider the default implementation, [[reactivemongo.api.collections.default.BSONCollection]].
 *
 * Example using the default implementation (BSONCollection)
 *
 * {{{
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
}}}
 */
trait Collection {
  /** The database which this collection belong to. */
  def db: DB
  /** The name of the collection. */
  def name: String
  /** The default failover strategy for the methods of this collection. */
  def failoverStrategy: FailoverStrategy

  /** Gets the full qualified name of this collection. */
  def fullCollectionName = db.name + "." + name

  /**
   * Gets another implementation of this collection.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.default.BSONCollection]]).
   *
   * @param failoverStrategy Overrides the default strategy.
   */
  def as[C <: Collection](failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.default.BSONCollectionProducer): C = {
    producer.apply(db, name, failoverStrategy)
  }

  /**
   * Gets another collection in the current database.
   * An implicit CollectionProducer[C] must be present in the scope, or it will be the default implementation ([[reactivemongo.api.collections.default.BSONCollection]]).
   *
   * @param name The other collection name.
   * @param failoverStrategy Overrides the default strategy.
   */
  def sister[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.default.BSONCollectionProducer): C = {
    producer.apply(db, name, failoverStrategy)
  }
}

/**
 * A Producer of [[Collection]] implementation.
 *
 * This is used to get an implementation implicitly when getting a reference of a [[Collection]].
 */
trait CollectionProducer[+C <: Collection] {
  def apply(db: DB, name: String, failoverStrategy: FailoverStrategy = FailoverStrategy()): C
}