package reactivemongo.api

import reactivemongo.bson.{
  BSONArray,
  BSONDocument,
  BSONElement,
  BSONDocumentWriter
}

/**
 * MongoDB Read Preferences enable to read from primary or secondaries
 * with a predefined strategy.
 */
sealed trait ReadPreference {
  /** Indicates whether a slave member is ok. */
  def slaveOk: Boolean = true

  def filterTag: Option[BSONDocument => Boolean]
}

object ReadPreference {
  /** Reads only from the primary. This is the default choice. */
  object Primary extends ReadPreference {
    override val slaveOk = false
    val filterTag = None

    override val toString = "Primary"
  }

  private def TagFilter(tagSet: Seq[Map[String, String]]): Option[BSONDocument => Boolean] = if (tagSet.isEmpty) None else Some { doc: BSONDocument =>
    val matching = tagSet.find(_.foldLeft(Map.empty[String, String]) {
      case (ms, (k, v)) =>
        doc.getAs[String](k).filter(_ == v) match {
          case None => ms + (k -> v)
          case _    => ms
        }
    }.isEmpty)

    matching.isDefined
  }

  private def TagFilter(tagSet: Seq[BSONDocument]): Option[BSONDocument => Boolean] = if (tagSet.isEmpty) None else Some { doc =>
    tagSet.exists(containsAll(doc, _))
  }

  @deprecated("For legacy purpose only", "0.11.12")
  sealed trait Taggable extends scala.Serializable with java.io.Serializable
    with Product with Equals { self: ReadPreference =>

    /** Returns the tags to be used. */
    def tags: List[Map[String, String]]

    def canEqual(that: Any): Boolean = that match {
      case _: PrimaryPreferred => true
      case _                   => false
    }

    val productArity = 1
    def productElement(n: Int): Any =
      if (n == 0) filterTag else throw new IndexOutOfBoundsException()

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
  class PrimaryPreferred @deprecated("Use `primaryPreferred(List)`", "0.11.12") (val filterTag: Option[BSONDocument => Boolean])
    extends ReadPreference with Taggable {

    private var _tags = List.empty[Map[String, String]]
    def tags = _tags

    private[api] def this(tags: List[Map[String, String]]) = {
      this(TagFilter(tags))
      _tags = tags
    }

    def copy(filter: Option[BSONDocument => Boolean]): PrimaryPreferred = {
      val pref = new PrimaryPreferred(filter)
      pref._tags = this._tags
      pref
    }

    override val toString = s"PrimaryPreferred(${_tags})"
  }

  /** Factory for the [[PrimaryPreferred]] read preference. */
  object PrimaryPreferred extends scala.runtime.AbstractFunction1[Option[BSONDocument => Boolean], PrimaryPreferred] {
    private[api] def apply(tags: List[Map[String, String]]): PrimaryPreferred =
      new PrimaryPreferred(tags)

    def apply(filterTag: Option[BSONDocument => Boolean]): PrimaryPreferred =
      new PrimaryPreferred(filterTag)

    def unapply(that: PrimaryPreferred): Option[Option[BSONDocument => Boolean]] = Some(that.filterTag)

  }

  /** Reads only from any secondary. */
  class Secondary @deprecated("Use `secondary(List)`", "0.11.12") (val filterTag: Option[BSONDocument => Boolean])
    extends ReadPreference with Taggable {

    private var _tags = List.empty[Map[String, String]]
    def tags = _tags

    private[api] def this(tags: List[Map[String, String]]) = {
      this(TagFilter(tags))
      _tags = tags
    }

    def copy(filter: Option[BSONDocument => Boolean]): Secondary = {
      val pref = new Secondary(filter)
      pref._tags = this._tags
      pref
    }

    override val toString = s"Secondary(${_tags})"
  }

  /** Factory for the [[Secondary]] read preference. */
  object Secondary extends scala.runtime.AbstractFunction1[Option[BSONDocument => Boolean], Secondary] {
    private[api] def apply(tags: List[Map[String, String]]): Secondary =
      new Secondary(tags)

    def apply(filterTag: Option[BSONDocument => Boolean]): Secondary =
      new Secondary(filterTag)

    def unapply(that: Secondary): Option[Option[BSONDocument => Boolean]] = Some(that.filterTag)

  }

  /** Reads from any secondary, or from the primary if they are not available. */
  class SecondaryPreferred @deprecated("Use `secondaryPreferred(List)`", "0.11.12") (val filterTag: Option[BSONDocument => Boolean])
    extends ReadPreference with Taggable {

    private var _tags = List.empty[Map[String, String]]
    def tags = _tags

    private[api] def this(tags: List[Map[String, String]]) = {
      this(TagFilter(tags))
      _tags = tags
    }

    def copy(filter: Option[BSONDocument => Boolean]): SecondaryPreferred = {
      val pref = new SecondaryPreferred(filter)
      pref._tags = this._tags
      pref
    }

    override val toString = s"SecondaryPreferred(${_tags})"
  }

  /** Factory for the [[SecondaryPreferred]] read preference. */
  object SecondaryPreferred extends scala.runtime.AbstractFunction1[Option[BSONDocument => Boolean], SecondaryPreferred] {
    private[api] def apply(tags: List[Map[String, String]]): SecondaryPreferred =
      new SecondaryPreferred(tags)

    def apply(filterTag: Option[BSONDocument => Boolean]): SecondaryPreferred =
      new SecondaryPreferred(filterTag)

    def unapply(that: SecondaryPreferred): Option[Option[BSONDocument => Boolean]] = Some(that.filterTag)

  }

  /**
   * Reads from the faster node (e.g. the node which replies faster than
   * all others), regardless its status (primary or secondary).
   */
  class Nearest @deprecated("Use `nearest(List)`", "0.11.12") (val filterTag: Option[BSONDocument => Boolean])
    extends ReadPreference with Taggable {

    private var _tags = List.empty[Map[String, String]]
    def tags = _tags

    private[api] def this(tags: List[Map[String, String]]) = {
      this(TagFilter(tags))
      _tags = tags
    }

    def copy(filter: Option[BSONDocument => Boolean]): Nearest = {
      val pref = new Nearest(filter)
      pref._tags = this._tags
      pref
    }

    override val toString = s"Nearest(${_tags})"
  }

  /** Factory for the [[Nearest]] read preference. */
  object Nearest extends scala.runtime.AbstractFunction1[Option[BSONDocument => Boolean], Nearest] {
    private[api] def apply(tags: List[Map[String, String]]): Nearest =
      new Nearest(tags)

    def apply(filterTag: Option[BSONDocument => Boolean]): Nearest =
      new Nearest(filterTag)

    def unapply(that: Nearest): Option[Option[BSONDocument => Boolean]] = Some(that.filterTag)

  }

  // TODO: Move to BSON module
  private def containsAll(target: BSONDocument, other: BSONDocument): Boolean = {
    val els = target.elements

    other.elements.forall { element =>
      els.find {
        case BSONElement(name, value) =>
          element.name == name && ((element.value, value) match {
            case (d1: BSONDocument, d2: BSONDocument) =>
              d1.elements == d2.elements

            case (a1: BSONArray, a2: BSONArray) =>
              a1.values == a2.values

            case (v1, v2) => v1 == v2
          })
      }.isDefined
    }
  }

  /** Reads only from the primary. This is the default choice. */
  def primary: Primary.type = Primary

  /** Reads from the primary if it is available, or secondaries if it is not. */
  val primaryPreferred: PrimaryPreferred = new PrimaryPreferred(None)

  /** Reads from any node that has the given `tag` in the replica set (preferably the primary). */
  @deprecated("Use `primaryPreferred(T*)`", "0.11.12")
  def primaryPreferred[T](tag: T)(implicit writer: BSONDocumentWriter[T]): PrimaryPreferred = new PrimaryPreferred(TagFilter(List(writer.write(tag))))

  /** Reads from any node that has the given `tagSet` in the replica set (preferably the primary). */
  def primaryPreferred[T](tagSet: T*)(implicit writer: BSONDocumentWriter[T]): PrimaryPreferred = new PrimaryPreferred(TagFilter(tagSet.map(writer.write(_))))

  /** Reads from any node that has the given `tagSet` in the replica set (preferably the primary). */
  def primaryPreferred(tagSet: List[Map[String, String]]): PrimaryPreferred = new PrimaryPreferred(tagSet)

  /** Reads only from any secondary. */
  val secondary: Secondary = new Secondary(None)

  /** Reads from a secondary that has the given `tag` in the replica set. */
  @deprecated("Use `secondary(T*)`", "0.11.12")
  def secondary[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Secondary =
    new Secondary(TagFilter(List(writer.write(tag))))

  /** Reads from a secondary that has the given `tagSet` in the replica set. */
  def secondary[T](tagSet: T*)(implicit writer: BSONDocumentWriter[T]): Secondary = new Secondary(TagFilter(tagSet.map(writer.write(_))))

  /** Reads from a secondary that has the given `tagSet` in the replica set. */
  def secondary(tagSet: List[Map[String, String]]): Secondary = new Secondary(tagSet)

  /** Reads from any secondary, or from the primary if they are not available. */
  val secondaryPreferred: SecondaryPreferred = new SecondaryPreferred(None)

  /** Reads from any node that has the given `tag` in the replica set (preferably a secondary). */
  @deprecated("Use `secondaryPreferred(T*)`", "0.11.12")
  def secondaryPreferred[T](tag: T)(implicit writer: BSONDocumentWriter[T]): SecondaryPreferred = new SecondaryPreferred(TagFilter(List(writer.write(tag))))

  /** Reads from any node that has the given `tagSet` in the replica set (preferably a secondary). */
  def secondaryPreferred[T](tagSet: T*)(implicit writer: BSONDocumentWriter[T]): SecondaryPreferred = new SecondaryPreferred(TagFilter(tagSet.map(writer.write(_))))

  /** Reads from any node that has the given `tagSet` in the replica set (preferably a secondary). */
  def secondaryPreferred(tagSet: List[Map[String, String]]): SecondaryPreferred = new SecondaryPreferred(tagSet)

  /**
   * Reads from the fastest node (ie the node which replies faster than all others), regardless its status
   * (primary or secondary).
   */
  val nearest: Nearest = new Nearest(None)

  /**
   * Reads from the fastest node (e.g. the node which replies faster than all others) that has the given `tag`, regardless its status (primary or secondary).
   */
  @deprecated("Use `secondaryPreferred(T*)`", "0.11.12")
  def nearest[T](tag: T)(implicit writer: BSONDocumentWriter[T]): Nearest =
    new Nearest(TagFilter(List(writer.write(tag))))

  /**
   * Reads from the fastest node (e.g. the node which replies faster than all others) that has the given `tagSet`, regardless its status (primary or secondary).
   */
  def nearest[T](tagSet: T*)(implicit writer: BSONDocumentWriter[T]): Nearest = new Nearest(TagFilter(tagSet.map(writer.write(_))))

  /**
   * Reads from the fastest node (e.g. the node which replies faster than all others) that has the given `tagSet`, regardless its status (primary or secondary).
   */
  def nearest[T](tagSet: List[Map[String, String]]): Nearest = new Nearest(tagSet)
}

sealed trait ReadConcern {
  /** The read concern level */
  def level: String

  override def toString = s"ReadConcern($level)"
}

object ReadConcern {
  object Majority extends ReadConcern { val level = "majority" }
  object Local extends ReadConcern { val level = "local" }
}
