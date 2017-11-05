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
