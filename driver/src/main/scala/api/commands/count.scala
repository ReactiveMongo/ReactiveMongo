package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

import scala.language.implicitConversions

@deprecated("Use new `reactivemongo.api.collections.CountOp`", "0.16.0")
trait CountCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Count(
    //{ count: <collection>, query: <query>, limit: <limit>, skip: <skip>, hint: <hint> }
    query: Option[pack.Document],
    limit: Int,
    skip: Int,
    hint: Option[Hint]) extends CollectionCommand with CommandWithPack[pack.type] with CommandWithResult[CountResult]

  object Count {
    def apply(doc: ImplicitlyDocumentProducer, limit: Int = 0, skip: Int = 0, hint: Option[Hint] = None): Count = Count(Some(doc.produce), limit, skip, hint)
  }

  sealed trait Hint

  class HintString private[api] (val s: String)
    extends Hint with Product1[String] with Serializable {

    @deprecated("No longer a case class", "1.0.0-rc.1")
    @inline def _1 = s

    def canEqual(that: Any): Boolean = that match {
      case _: HintString => true
      case _             => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: HintString =>
        (this.s == null && other.s == null) || (
          this.s != null && this.s.equals(other.s))

      case _ =>
        false
    }

    override def hashCode: Int = if (s == null) -1 else s.hashCode

    override def toString = s"Hint['$s']"
  }

  @deprecated("No longer a case class", "1.0.0-rc.1")
  object HintString
    extends scala.runtime.AbstractFunction1[String, HintString] {

    def apply(s: String): HintString = new HintString(s)

    def unapply(hint: HintString): Option[String] = Option(hint).map(_.s)
  }

  class HintDocument private[api] (val doc: pack.Document)
    extends Hint with Product1[pack.Document] with Serializable {

    @deprecated("No longer a case class", "1.0.0-rc.1")
    @inline def _1 = doc

    def canEqual(that: Any): Boolean = that match {
      case _: HintDocument => true
      case _               => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: HintDocument =>
        (this.doc == null && other.doc == null) || (
          this.doc != null && this.doc.equals(other.doc))

      case _ =>
        false
    }

    override def hashCode: Int = if (doc == null) -1 else doc.hashCode

    override def toString = s"Hint${pack pretty doc}"
  }

  @deprecated("No longer a case class", "1.0.0-rc.1")
  object HintDocument
    extends scala.runtime.AbstractFunction1[pack.Document, HintDocument] {

    def apply(doc: pack.Document): HintDocument = new HintDocument(doc)

    def unapply(hint: HintDocument): Option[pack.Document] =
      Option(hint).map(_.doc)
  }

  object Hint {
    implicit def strToHint(s: String): Hint = new HintString(s)

    implicit def docToHint(doc: ImplicitlyDocumentProducer): Hint =
      new HintDocument(doc.produce)
  }

  class CountResult(@deprecatedName(Symbol("count")) val value: Int) extends BoxedAnyVal[Int] with Product1[Int] with Serializable {

    @deprecated("No longer a case class", "1.0.0-rc.1")
    @inline def _1 = value

    @deprecated("No longer a case class", "1.0.0-rc.1")
    def canEqual(that: Any): Boolean = that match {
      case _: CountResult => true
      case _              => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: CountResult => this.value == other.value
      case _                  => false
    }

    @inline override def hashCode: Int = value

    override def toString = s"CountResult($value)"
  }

  @deprecated("No longer a case class", "1.0.0-rc.1")
  object CountResult
    extends scala.runtime.AbstractFunction1[Int, CountResult] {

    def apply(value: Int): CountResult = new CountResult(value)

    def unapply(hint: CountResult): Option[Int] = Option(hint).map(_.value)
  }
}
