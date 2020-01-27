package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

private[commands] trait SortAggregation[P <: SerializationPack] {
  aggregation: AggregationFramework[P] =>

  /**
   * [[https://docs.mongodb.org/v3.0/reference/operator/aggregation/meta/#exp._S_meta Keyword of metadata]].
   */
  sealed trait MetadataKeyword {
    /** Keyword name */
    def name: String
  }

  /** References the score associated with the corresponding [[https://docs.mongodb.org/v3.0/reference/operator/query/text/#op._S_text `\$text`]] query for each matching document. */
  case object TextScore extends MetadataKeyword {
    val name = "textScore"
  }

  /**
   * Represents that a field should be sorted on, as well as whether it
   * should be ascending or descending.
   */
  sealed trait SortOrder {
    /** The name of the field to be used to sort. */
    def field: String
  }

  /** Ascending sort order */
  class Ascending private[api] (val field: String)
    extends SortOrder with Product1[String] with Serializable {

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Ascending => true
      case _            => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Ascending =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"Ascending(${field})"
  }

  object Ascending extends scala.runtime.AbstractFunction1[String, Ascending] {
    def apply(field: String): Ascending = new Ascending(field)

    def unapply(ascending: Ascending): Option[String] =
      Option(ascending).map(_.field)
  }

  /** Descending sort order */
  class Descending private[api] (val field: String)
    extends SortOrder with Product1[String] with Serializable {

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Descending => true
      case _             => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Descending =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"Descending(${field})"
  }

  object Descending
    extends scala.runtime.AbstractFunction1[String, Descending] {

    def apply(field: String): Descending = new Descending(field)

    def unapply(descending: Descending): Option[String] =
      Option(descending).map(_.field)
  }

  /**
   * [[https://docs.mongodb.org/v3.0/reference/operator/aggregation/sort/#sort-pipeline-metadata Metadata sort]] order.
   *
   * @param keyword the metadata keyword to sort by
   */
  class MetadataSort private[api] (
    val field: String,
    val keyword: MetadataKeyword) extends SortOrder
    with Product2[String, MetadataKeyword] with Serializable {

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = keyword

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: MetadataSort => true
      case _               => false
    }

    private[api] lazy val tupled = field -> keyword

    override def equals(that: Any): Boolean = that match {
      case other: MetadataSort =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"MetadataSort${tupled.toString}"
  }

  object MetadataSort
    extends scala.runtime.AbstractFunction2[String, MetadataKeyword, MetadataSort] {

    def apply(field: String, keyword: MetadataKeyword): MetadataSort =
      new MetadataSort(field, keyword)

    def unapply(other: MetadataSort): Option[(String, MetadataKeyword)] =
      Option(other).map { i => i.field -> i.keyword }

  }
}
