package reactivemongo.api

/**
 * MongoDB Read Preferences enable to read from primary or secondaries
 * with a predefined strategy.
 */
sealed trait ReadPreference {
  /** Indicates whether a slave member is ok. */
  def slaveOk: Boolean = true

  //def filterTag: Option[BSONDocument => Boolean]
}

object ReadPreference {
  /** Reads only from the primary. This is the default choice. */
  object Primary extends ReadPreference {
    override val slaveOk = false
    val filterTag = None

    override val toString = "Primary"
  }

  private[reactivemongo] def TagFilter(
    tagSet: Seq[Map[String, String]]): Option[Map[String, String] => Boolean] = {
    if (tagSet.isEmpty) None else Some { tags: Map[String, String] =>
      val matching = tagSet.find(_.foldLeft(Map.empty[String, String]) {
        case (ms, (k, v)) =>
          if (tags.get(k).exists(_ == v)) {
            ms + (k -> v)
          } else ms
      }.isEmpty)

      matching.isDefined
    }
  }

  private[api] sealed trait Taggable { self: ReadPreference =>
    /** Returns the tags to be used. */
    def tags: List[Map[String, String]]
  }

  /** Extractor for taggable read preference. */
  object Taggable {
    def unapply(pref: ReadPreference): Option[List[Map[String, String]]] =
      pref match {
        case p: Taggable => p.tags.headOption.map(_ :: p.tags.tail)
        case _           => None
      }
  }

  /** Reads from the primary if it is available, or secondaries if it is not. */
  case class PrimaryPreferred(val tags: List[Map[String, String]])
    extends ReadPreference with Taggable {

    override val toString = s"""PrimaryPreferred(${tags mkString ", "})"""
  }

  /** Reads only from any secondary. */
  case class Secondary(val tags: List[Map[String, String]])
    extends ReadPreference with Taggable {

    override val toString = s"""Secondary(${tags mkString ", "})"""
  }

  /**
   * Reads from any secondary,
   * or from the primary if they are not available.
   */
  case class SecondaryPreferred(val tags: List[Map[String, String]])
    extends ReadPreference with Taggable {

    override val toString = s"""SecondaryPreferred(${tags mkString ", "})"""
  }

  /**
   * Reads from the faster node (e.g. the node which replies faster than
   * all others), regardless its status (primary or secondary).
   */
  case class Nearest(val tags: List[Map[String, String]])
    extends ReadPreference with Taggable {

    override val toString = s"""Nearest(${tags mkString ", "})"""
  }

  /** Reads only from the primary. This is the default choice. */
  def primary: Primary.type = Primary

  /** Reads from the primary if it is available, or secondaries if it is not. */
  val primaryPreferred: PrimaryPreferred = new PrimaryPreferred(List.empty)

  /** Reads from any node that has the given `tagSet` in the replica set (preferably the primary). */
  def primaryPreferred(tagSet: List[Map[String, String]]): PrimaryPreferred = new PrimaryPreferred(tagSet)

  /** Reads only from any secondary. */
  val secondary: Secondary = new Secondary(List.empty)

  /** Reads from a secondary that has the given `tagSet` in the replica set. */
  def secondary(tagSet: List[Map[String, String]]): Secondary = new Secondary(tagSet)

  /** Reads from any secondary, or from the primary if they are not available. */
  val secondaryPreferred: SecondaryPreferred =
    new SecondaryPreferred(List.empty)

  /** Reads from any node that has the given `tagSet` in the replica set (preferably a secondary). */
  def secondaryPreferred(tagSet: List[Map[String, String]]): SecondaryPreferred = new SecondaryPreferred(tagSet)

  /**
   * Reads from the fastest node (ie the node which replies faster than all others), regardless its status
   * (primary or secondary).
   */
  val nearest: Nearest = new Nearest(List.empty)

  /**
   * Reads from the fastest node (e.g. the node which replies faster than all others) that has the given `tagSet`, regardless its status (primary or secondary).
   */
  def nearest[T](tagSet: List[Map[String, String]]): Nearest = new Nearest(tagSet)
}
