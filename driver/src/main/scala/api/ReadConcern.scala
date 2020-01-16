package reactivemongo.api

/**
 * The [[https://docs.mongodb.com/manual/reference/read-concern/ Read Concern]] allows to control the consistency and isolation used to read data from replica sets.
 */
sealed trait ReadConcern {
  /** The read concern level */
  def level: String

  override def equals(that: Any): Boolean = that match {
    case other: ReadConcern => other.level == level
    case _                  => false
  }

  override def hashCode: Int = level.hashCode

  override def toString = s"ReadConcern($level)"
}

object ReadConcern {
  object Available extends ReadConcern { val level = "available" }

  /** Requires `--enableMajorityReadConcern` */
  object Majority extends ReadConcern { val level = "majority" }

  object Local extends ReadConcern { val level = "local" }

  object Linearizable extends ReadConcern { val level = "linearizable" }

  /** Local */
  @inline private[api] def default: ReadConcern = Local

  private[api] def unapply(level: String): Option[ReadConcern] = level match {
    case Available.level    => Some(Available)
    case Majority.level     => Some(Majority)
    case Linearizable.level => Some(Linearizable)
    case _                  => Some(Local)
  }
}
