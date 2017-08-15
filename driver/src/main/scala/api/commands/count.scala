package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

import scala.language.implicitConversions

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
  case class HintString(s: String) extends Hint
  case class HintDocument(doc: pack.Document) extends Hint
  object Hint {
    implicit def strToHint(s: String): Hint = HintString(s)
    implicit def docToHint(doc: ImplicitlyDocumentProducer): Hint = HintDocument(doc.produce)
  }

  case class CountResult(count: Int) extends BoxedAnyVal[Int] {
    def value = count
  }
}
