package reactivemongo.api.collections

import reactivemongo.api.{ PackSupport, SerializationPack }

private[reactivemongo] trait HintFactory[P <: SerializationPack] {
  collection: PackSupport[P] =>

  /**
   * Returns a hint for the given index `name`.
   *
   * @param name the index name
   */
  def hint(name: String): Hint = new HintString(name)

  /**
   * Returns a hint for the given index `specification` document.
   *
   * @param specification the index specification document
   */
  def hint(specification: pack.Document): Hint =
    new HintDocument(specification)

  // ---

  /** An index hint */
  sealed trait Hint

  private[api] case class HintString(
      indexName: String)
      extends Hint

  private[api] case class HintDocument(
      indexSpec: pack.Document)
      extends Hint
}
