package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Represents a [[https://docs.mongodb.com/manual/reference/collation/ collection, view, index or operation]] specific collation.
 */
final class Collation(
  val locale: String,
  val caseLevel: Option[Boolean],
  val caseFirst: Option[Collation.CaseFirst],
  val strength: Option[Collation.Strength],
  val numericOrdering: Option[Boolean],
  val alternate: Option[Collation.Alternate],
  val maxVariable: Option[Collation.MaxVariable],
  val backwards: Option[Boolean])

object Collation {
  class Strength(val value: Int) extends AnyVal
  val PrimaryStrength = new Strength(1)
  val SecondaryStrength = new Strength(2)
  val TertiaryStrength = new Strength(3)
  val QuaternaryStrength = new Strength(4)
  val IdentityStrength = new Strength(5)

  class CaseFirst(val value: String) extends AnyVal
  val UpperCaseFirst = new CaseFirst("upper")
  val LowerCaseFirst = new CaseFirst("lower")
  val OffCaseFirst = new CaseFirst("off")

  class Alternate(val value: String) extends AnyVal
  val NonIgnorable = new Alternate("non-ignorable")
  val Shifted = new Alternate("shifted")

  class MaxVariable(val value: String) extends AnyVal
  val Punct = new MaxVariable("punct")
  val Space = new MaxVariable("space")

  // ---

  private[api] def serialize[P <: SerializationPack](
    pack: P,
    collation: Collation): pack.Document = {
    val builder = pack.newBuilder
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
}
