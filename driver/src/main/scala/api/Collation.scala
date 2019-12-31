package reactivemongo.api

/**
 * Represents a [[https://docs.mongodb.com/manual/reference/collation/ collection, view, index or operation]] specific collation.
 *
 * {{{
 * import reactivemongo.api.Collation
 *
 * val collation = new Collation(
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
final class Collation(
  val locale: String,
  val caseLevel: Option[Boolean],
  val caseFirst: Option[Collation.CaseFirst],
  val strength: Option[Collation.Strength],
  val numericOrdering: Option[Boolean],
  val alternate: Option[Collation.Alternate],
  val maxVariable: Option[Collation.MaxVariable],
  val backwards: Option[Boolean]) {

  override def equals(that: Any): Boolean = that match {
    case other: Collation => other.tupled == tupled
    case _                => false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"Collation${tupled.toString}"

  // ---

  private[api] def tupled =
    (locale, caseLevel, caseFirst, strength, numericOrdering, alternate,
      maxVariable, backwards)

}

/**
 * [[Collation]] utilities.
 */
object Collation {
  final class Strength(val value: Int) extends AnyVal
  val PrimaryStrength = new Strength(1)
  val SecondaryStrength = new Strength(2)
  val TertiaryStrength = new Strength(3)
  val QuaternaryStrength = new Strength(4)
  val IdentityStrength = new Strength(5)

  final class CaseFirst(val value: String) extends AnyVal
  val UpperCaseFirst = new CaseFirst("upper")
  val LowerCaseFirst = new CaseFirst("lower")
  val OffCaseFirst = new CaseFirst("off")

  final class Alternate(val value: String) extends AnyVal
  val NonIgnorable = new Alternate("non-ignorable")
  val Shifted = new Alternate("shifted")

  final class MaxVariable(val value: String) extends AnyVal
  val Punct = new MaxVariable("punct")
  val Space = new MaxVariable("space")

  // ---

  @inline private[api] def serialize[P <: SerializationPack](
    pack: P,
    collation: Collation): pack.Document =
    serializeWith(pack, collation)(pack.newBuilder)

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
          backwards = booleanLike(doc, "backwards"))

      }
    }
  }
}
