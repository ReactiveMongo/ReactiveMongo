package reactivemongo.api

object ChangeStreams {
  sealed abstract class FullDocument(val name: String)

  object FullDocument {
    case object Default extends FullDocument("default")
    case object UpdateLookup extends FullDocument("updateLookup")
  }
}
