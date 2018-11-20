package reactivemongo.api

object ChangeStreams {

  /**
   * Defines the lookup strategy of a change stream.
   */
  sealed abstract class FullDocumentStrategy(val name: String)

  object FullDocumentStrategy {

    /**
     * Default lookup strategy. Insert and Replace events contain the full document at the time of the event.
     */
    case object Default extends FullDocumentStrategy("default")

    /**
     * In this strategy, in addition to the default behavior, Update change events will be joined with the *current*
     * version of the related document (which is thus not necessarily the value at the time of the event).
     */
    case object UpdateLookup extends FullDocumentStrategy("updateLookup")
  }
}
