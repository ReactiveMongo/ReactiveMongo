package reactivemongo.api

/**
 * The [[https://docs.mongodb.com/manual/reference/read-concern/ Read Concern]]
 * allows to control the consistency and isolation used to read data
 * from replica sets.
 *
 * {{{
 * import reactivemongo.api.ReadConcern
 * import reactivemongo.api.bson.BSONDocument
 * import reactivemongo.api.bson.collection.BSONCollection
 *
 * def queryBuilderWithReadConcern(coll: BSONCollection) =
 *   coll.find(BSONDocument.empty).
 *   readConcern(ReadConcern.Available)
 *
 * }}}
 */
sealed trait ReadConcern {
  /** The read concern level */
  def level: String

  final override def equals(that: Any): Boolean = that match {
    case other: ReadConcern => other.level == level
    case _                  => false
  }

  final override def hashCode: Int = level.hashCode

  final override def toString = s"ReadConcern($level)"
}

object ReadConcern {
  /**
   * [[https://docs.mongodb.com/manual/reference/read-concern-local/#readconcern.%22local%22 Read Concern 'local']]
   */
  object Local extends ReadConcern { val level = "local" }

  /**
   * [[https://docs.mongodb.com/manual/reference/read-concern-available/#readconcern.%22available%22 Read Concern 'available']]
   * @since MongoDB 3.6
   */
  object Available extends ReadConcern { val level = "available" }

  /**
   * [[https://docs.mongodb.com/manual/reference/read-concern-majority/#readconcern.%22majority%22 Read Concern 'majority']]
   * @since MongoDB 3.6
   */
  object Majority extends ReadConcern { val level = "majority" }

  /**
   * [[https://docs.mongodb.com/manual/reference/read-concern-linearizable/#readconcern.%22linearizable%22 Read Concern 'linearizable']]
   * @since MongoDB 3.4
   */
  object Linearizable extends ReadConcern { val level = "linearizable" }

  /**
   * [[https://docs.mongodb.com/manual/reference/read-concern-snapshot/#readconcern.%22snapshot%22 Read Concern 'snapshot']]
   * @since MongoDB 3.4
   */
  object Snapshot extends ReadConcern { val level = "snapshot" }

  /** Local */
  @inline private[api] def default: ReadConcern = Local

  private[api] def unapply(level: String): Option[ReadConcern] = level match {
    case Available.level    => Some(Available)
    case Majority.level     => Some(Majority)
    case Linearizable.level => Some(Linearizable)
    case Snapshot.level     => Some(Snapshot)
    case _                  => Some(Local)
  }
}
