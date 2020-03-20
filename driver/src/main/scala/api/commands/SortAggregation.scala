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
  final class Ascending private[api] (val field: String) extends SortOrder {
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

  object Ascending {
    def apply(field: String): Ascending = new Ascending(field)

    def unapply(ascending: Ascending): Option[String] =
      Option(ascending).map(_.field)
  }

  /** Descending sort order */
  final class Descending private[api] (val field: String) extends SortOrder {
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

  object Descending {
    def apply(field: String): Descending = new Descending(field)

    def unapply(descending: Descending): Option[String] =
      Option(descending).map(_.field)
  }

  /**
   * [[https://docs.mongodb.org/v3.0/reference/operator/aggregation/sort/#sort-pipeline-metadata Metadata sort]] order.
   *
   * @param keyword the metadata keyword to sort by
   */
  final class MetadataSort private[api] (
    val field: String,
    val keyword: MetadataKeyword) extends SortOrder {

    private[api] lazy val tupled = field -> keyword

    override def equals(that: Any): Boolean = that match {
      case other: MetadataSort =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"MetadataSort${tupled.toString}"
  }

  object MetadataSort {
    def apply(field: String, keyword: MetadataKeyword): MetadataSort =
      new MetadataSort(field, keyword)

    def unapply(other: MetadataSort): Option[(String, MetadataKeyword)] =
      Option(other).map { i => i.field -> i.keyword }

  }
}
