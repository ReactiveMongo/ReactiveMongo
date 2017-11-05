package reactivemongo.bson.buffer

import reactivemongo.bson.{ BSONArray, BSONDocument, BSONValue }
import scala.util.{ Success, Try }

trait BufferHandler {
  def serialize(bson: BSONValue, buffer: WritableBuffer): WritableBuffer
  def deserialize(buffer: ReadableBuffer): Try[(String, BSONValue)]

  def write(buffer: WritableBuffer, document: BSONDocument) = {
    serialize(document, buffer)
  }

  def write(buffer: WritableBuffer, arr: BSONArray) = serialize(arr, buffer)

  def readDocument(buffer: ReadableBuffer): Try[BSONDocument]

  def writeDocument(
    document: BSONDocument,
    buffer: WritableBuffer): WritableBuffer

  def stream(buffer: ReadableBuffer): Stream[(String, BSONValue)] =
    deserialize(buffer) match {
      case Success(elem) => elem #:: stream(buffer)
      case _             => Stream.empty
    }
}
