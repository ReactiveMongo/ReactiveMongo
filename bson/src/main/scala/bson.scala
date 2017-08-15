/*
 * Copyright 2013 Stephane Godbillon (@sgodbillon)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.bson

/**
 * {{{
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
    write: T => BSONDocument): BSONDocumentHandler[T] = new BSONDocumentHandlerImpl[T](read, write)
}

object BSON {
  /**
   * Produces a `T` instance of the given BSON value, if there is an implicit `BSONReader[B, T]` in the scope.
   *
   * Prefer `readDocument` over this one if you want to deserialize `BSONDocuments`.
   */
  def read[B <: BSONValue, T](bson: B)(implicit reader: BSONReader[B, T]): T = reader.read(bson)
  /**
   * Produces a `BSONValue` instance of the given `T` value, if there is an implicit `BSONWriter[T, B]` in the scope.
   *
   * Prefer `writeDocument` over this one if you want to serialize `T` instances.
   */
  def write[T, B <: BSONValue](t: T)(implicit writer: BSONWriter[T, B]): B = writer.write(t)

  /** Produces a `T` instance of the given `BSONDocument`, if there is an implicit `BSONReader[BSONDocument, T]` in the scope. */
  def readDocument[T](doc: BSONDocument)(implicit reader: BSONReader[BSONDocument, T]): T = reader.read(doc)

  /** Produces a `BSONDocument` of the given `T` instance, if there is an implicit `BSONWriter[T, BSONDocument]` in the scope. */
  def writeDocument[T](t: T)(implicit writer: BSONWriter[T, BSONDocument]): BSONDocument = writer.write(t)
}
