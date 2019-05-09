package reactivemongo.bson.buffer

import reactivemongo.bson.{ BSONArray, BSONDocument, BSONElement, BSONValue }
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

@deprecated("Use [[reactivemongo.bson.BSONIterator]]", "0.12.8")
sealed trait BSONIterator extends Iterator[(String, BSONValue)] {
  val buffer: ReadableBuffer

  val startIndex = buffer.index
  val documentSize = buffer.readInt

  @SuppressWarnings(Array("OptionGet")) // TODO: Review
  def next: (String, BSONValue) = {
    val code = buffer.readByte
    buffer.readString -> DefaultBufferHandler.handlersByCode.get(code).map(_.read(buffer)).get
  }

  def hasNext = buffer.index - startIndex + 1 < documentSize

  def mapped: Map[String, BSONElement] =
    (for (el <- this) yield (el._1, BSONElement(el._1, el._2))).toMap
}

@deprecated("Use [[reactivemongo.bson.BSONIterator]]", "0.12.8")
object BSONIterator {
  import scala.util.Failure
  import reactivemongo.bson._

  private[bson] def pretty(i: Int, it: Iterator[Try[BSONElement]], f: String => String = { name => s""""${name}": """ }): String = {
    val indent = (0 to i).map { _ => "  " }.mkString("")

    it.map {
      case Success(BSONElement(name, value)) => {
        val prefix = s"${indent}${f(name)}"

        value match {
          case array: BSONArray => s"${prefix}[\n" + pretty(i + 1, array.elements.map(Success(_)).iterator, _ => "") + s"\n${indent}]"

          case BSONBoolean(b) =>
            s"${indent}${name}: $b"

          case BSONDocument(elements) =>
            s"${prefix}{\n" + pretty(i + 1, elements.iterator) + s"\n$indent}"

          case BSONDouble(d) =>
            s"""${prefix}$d"""

          case BSONInteger(i) =>
            s"${prefix}$i"

          case BSONLong(l) =>
            s"${prefix}NumberLong($l)"

          case d @ BSONDecimal(_, _) =>
            s"${prefix}NumberDecimal($d)"

          case BSONString(s) =>
            prefix + '"' + s.replaceAll("\"", "\\\"") + '"'

          case oid @ BSONObjectID(_) =>
            s"${prefix}Object(${oid.stringify})"

          case ts @ BSONTimestamp(_) =>
            s"${prefix}Timestamp(${ts.time}, ${ts.ordinal})"

          case BSONUndefined => s"${prefix}undefined"
          case BSONMinKey    => s"${prefix}MinKey"
          case BSONMaxKey    => s"${prefix}MaxKey"

          case _ =>
            s"${prefix}$value"
        }
      }

      case Failure(e) => s"${indent}ERROR[${e.getMessage()}]"
    }.mkString(",\n")
  }

  /** Makes a pretty String representation of the given iterator of BSON elements. */
  def pretty(it: Iterator[Try[BSONElement]]): String = "{\n" + pretty(0, it) + "\n}"
}
