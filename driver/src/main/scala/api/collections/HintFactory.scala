package reactivemongo.api.collections

import reactivemongo.api.SerializationPack

sealed trait Hint[P <: SerializationPack with Singleton]

private[api] trait HintFactory[P <: SerializationPack with Singleton] {
  collection: GenericCollection[P] =>

  /**
   * Returns a hint for the given index `name`.
   *
   * @param name the index name
   */
  def hint(name: String): Hint[pack.type] = new HintString(name)

  /**
   * Returns a hint for the given index `specification` document.
   *
   * @param specification the index specification document
   */
  def hint(specification: pack.Document): Hint[pack.type] =
    new HintDocument(specification)

  // ---

  private[api] case class HintString(
    indexName: String) extends Hint[pack.type]

  private[api] case class HintDocument(
    indexSpec: pack.Document) extends Hint[pack.type]
}
