package reactivemongo.api

/**
 * Represents a [[https://docs.mongodb.com/manual/reference/collation/ collection, view, index or operation]] specific collation.
 *
 * {{{
 * import reactivemongo.api.Collation
 *
 * val collation = Collation(
 *   locale = "en-US",
 *   caseLevel = None,
 *   caseFirst = None,
 *   strength = Some(Collation.PrimaryStrength),
 *   numericOrdering = None,
 *   alternate = None,
 *   maxVariable = None,
 *   backwards = None)
 * }}}
 */
final class Collation private[api] (
  val locale: String,
  val caseLevel: Option[Boolean],
  val caseFirst: Option[Collation.CaseFirst],
  val strength: Option[Collation.Strength],
  val numericOrdering: Option[Boolean],
  val alternate: Option[Collation.Alternate],
  val maxVariable: Option[Collation.MaxVariable],
  val backwards: Option[Boolean],
  val normalization: Option[Boolean]) {

  override def equals(that: Any): Boolean = that match {
    case other: Collation => other.tupled == tupled
    case _                => false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"Collation${tupled.toString}"

  // ---

  private[api] def tupled =
    (locale, caseLevel, caseFirst, strength, numericOrdering, alternate,
      maxVariable, backwards, normalization)

}

/**
 * [[Collation]] utilities.
 */
object Collation {
  /**
   * @param locale the [[https://docs.mongodb.com/manual/reference/collation-locales-defaults/#collation-languages-locales ICU locale]]
   * @param caseLevel the flag that determines whether to include case comparison at strength level 1 or 2 (see [[http://userguide.icu-project.org/collation/concepts#TOC-CaseLevel ICU case level]])
   * @param caseFirst the field that determines sort order of case differences during tertiary level comparisons
   * @param numericOrdering the flag that determines whether to compare numeric strings as numbers or as strings
   * @param alternate the field that determines whether collation should consider whitespace and punctuation as base characters
   * @param maxVariable the field that determines up to which characters are considered ignorable
   * @param backwards the flag that determines whether strings with diacritics sort from back of the string
   * @param normalization the flag that determines whether to check if text require normalization and to perform normalization
   */
  def apply(
    locale: String,
    caseLevel: Option[Boolean] = None,
    caseFirst: Option[CaseFirst] = None,
    strength: Option[Strength] = None,
    numericOrdering: Option[Boolean] = None,
    alternate: Option[Alternate] = None,
    maxVariable: Option[MaxVariable] = None,
    backwards: Option[Boolean] = None,
    normalization: Option[Boolean] = None): Collation =
    new Collation(locale, caseLevel, caseFirst, strength, numericOrdering,
      alternate, maxVariable, backwards, normalization)

  // ---

  final class Strength private[api] (val value: Int) extends AnyVal
  val PrimaryStrength = new Strength(1)
  val SecondaryStrength = new Strength(2)
  val TertiaryStrength = new Strength(3)
  val QuaternaryStrength = new Strength(4)
  val IdentityStrength = new Strength(5)

  final class CaseFirst private[api] (val value: String) extends AnyVal
  val UpperCaseFirst = new CaseFirst("upper")
  val LowerCaseFirst = new CaseFirst("lower")
  val OffCaseFirst = new CaseFirst("off")

  final class Alternate private[api] (val value: String) extends AnyVal
  val NonIgnorable = new Alternate("non-ignorable")
  val Shifted = new Alternate("shifted")

  final class MaxVariable private[api] (val value: String) extends AnyVal
  val Punct = new MaxVariable("punct")
  val Space = new MaxVariable("space")

  // ---

  @inline private[api] def serialize[P <: SerializationPack](
    pack: P,
    collation: Collation): pack.Document =
    serializeWith(pack, collation)(pack.newBuilder)

  @SuppressWarnings(Array("UnusedMethodParameter"))
  private[api] def serializeWith[P <: SerializationPack](
    pack: P,
    collation: Collation)(
    builder: SerializationPack.Builder[pack.type]): pack.Document = {

    import builder.{ elementProducer => element, int, string, boolean }

    val elements = Seq.newBuilder[pack.ElementProducer]

    elements += element("locale", string(collation.locale))

    collation.caseLevel.foreach { level =>
      elements += element("caseLevel", boolean(level))
    }

    collation.caseFirst.foreach { first =>
      elements += element("caseFirst", string(first.value))
    }

    collation.strength.foreach { strength =>
      elements += element("strength", int(strength.value))
    }

    collation.numericOrdering.foreach { ordering =>
      elements += element("numericOrdering", boolean(ordering))
    }

    collation.alternate.foreach { alt =>
      elements += element("alternate", string(alt.value))
    }

    collation.maxVariable.foreach { max =>
      elements += element("maxVariable", string(max.value))
    }

    collation.backwards.foreach { back =>
      elements += element("backwards", boolean(back))
    }

    collation.normalization.foreach { norm =>
      elements += element("normalization", boolean(norm))
    }

    builder.document(elements.result())
  }

  private[api] def read[P <: SerializationPack](pack: P): pack.Document => Option[Collation] = {
    val decoder = pack.newDecoder

    import decoder.{ booleanLike, string }

    { doc =>
      string(doc, "locale").map { locale =>
        new Collation(
          locale,
          caseLevel = booleanLike(doc, "caseLevel"),
          caseFirst = string(doc, "caseFirst").map(new Collation.CaseFirst(_)),
          strength = decoder.int(
            doc, "strength").map(new Collation.Strength(_)),
          numericOrdering = booleanLike(doc, "numericOrdering"),
          alternate = string(doc, "alternate").map(new Collation.Alternate(_)),
          maxVariable = string(
            doc, "maxVariable").map(new Collation.MaxVariable(_)),
          backwards = booleanLike(doc, "backwards"),
          normalization = booleanLike(doc, "normalization"))

      }
    }
  }
}
