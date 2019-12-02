package reactivemongo.bson

/**
 * {{{
 * import reactivemongo.bson._
 *
 * // { "name": "Johny", "surname": "Doe", "age": 28, "months": [1, 2, 3] }
 * document ++ ("name" -> "Johny") ++ ("surname" -> "Doe") ++
 * ("age" -> 28) ++ ("months" -> array(1, 2, 3))
 *
 * // { "_id": generatedId, "name": "Jane", "surname": "Doe", "age": 28,
 * //   "months": [1, 2, 3], "details": { "salary": 12345,
 * //   "inventory": ["foo", 7.8, 0, false] } }
 * document ++ ("_id" -> generateId, "name" -> "Jane", "surname" -> "Doe",
 *   "age" -> 28, "months" -> (array ++ (1, 2) ++ 3),
 *   "details" -> document(
 *     "salary" -> 12345L, "inventory" -> array("foo", 7.8, 0L, false)))
 * }}}
 */
object `package` extends DefaultBSONHandlers {
  // DSL helpers:

  /** Returns an empty document. */
  def document = BSONDocument.empty

  /** Returns a document with given elements. */
  def document(elements: Producer[BSONElement]*) = BSONDocument(elements: _*)

  /** Returns an empty array. */
  def array = BSONArray.empty

  /** Returns an array with given values. */
  def array(values: Producer[BSONValue]*) = BSONArray(values: _*)

  /** Returns a newly generated object ID. */
  def generateId = BSONObjectID.generate()

  def element(name: String, value: BSONValue) = BSONElement(name, value)

  /** Convenient type alias for document handlers */
  type BSONDocumentHandler[T] = BSONDocumentReader[T] with BSONDocumentWriter[T] with BSONHandler[BSONDocument, T]

  /** Handler factory */
  def BSONDocumentHandler[T](
    read: BSONDocument => T,
    write: T => BSONDocument): BSONDocumentHandler[T] =
    new BSONDocumentHandlerImpl[T](read, write)
}
