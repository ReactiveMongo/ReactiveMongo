package reactivemongo.api.bson
package compat

import scala.language.implicitConversions

import scala.util.{ Failure, Success }

import scala.reflect.ClassTag

import reactivemongo.bson.{
  BSONDocument => LegacyDoc,
  BSONDocumentHandler => LegacyDocHandler,
  BSONDocumentReader => LegacyDocReader,
  BSONDocumentWriter => LegacyDocWriter,
  BSONHandler => LegacyHandler,
  BSONReader => LegacyReader,
  BSONValue => LegacyValue,
  BSONWriter => LegacyWriter
}

/**
 * See [[compat$]] and [[HandlerConverters]]
 */
object HandlerConverters extends HandlerConverters

/**
 * Implicit conversions for handler types
 * between `reactivemongo.bson` and `reactivemongo.api.bson` .
 *
 * {{{
 * import reactivemongo.api.bson.compat.HandlerConverters._
 *
 * def foo[T](lw: reactivemongo.bson.BSONDocumentWriter[T]) = {
 *   val w: reactivemongo.api.bson.BSONDocumentWriter[T] = lw
 *   w
 * }
 *
 * import reactivemongo.bson.BSONValue
 *
 * def bar[T](lr: reactivemongo.api.bson.BSONReader[T]) = {
 *   val r: reactivemongo.bson.BSONReader[BSONValue, T] = lr
 *   r
 * }
 * }}}
 */
trait HandlerConverters extends LowPriorityHandlerConverters1 {

  /**
   * Implicit conversion from legacy `BSONDocumentHandler` to the new API.
   *
   * {{{
   * import reactivemongo.api.bson.compat.HandlerConverters.toDocumentHandler
   *
   * def foo[T](lh: reactivemongo.bson.BSONDocumentHandler[T]) = {
   *   val h: reactivemongo.api.bson.BSONDocumentHandler[T] = lh
   *   h
   * }
   * }}}
   */
  implicit final def toDocumentHandler[T](h: LegacyDocHandler[T]): BSONDocumentHandler[T] = providedDocumentHandler[T](toDocumentReader(h), toDocumentWriter(h))

  /**
   * Implicit conversion from new `BSONDocumentHandler` to the legacy API.
   *
   * {{{
   * import reactivemongo.api.bson.compat.HandlerConverters.fromDocumentHandler
   *
   * def bar[T](lh: reactivemongo.api.bson.BSONDocumentHandler[T]) = {
   *   val h: reactivemongo.bson.BSONDocumentHandler[T] = lh
   *   h
   * }
   * }}}
   */
  implicit final def fromDocumentHandler[T](h: BSONDocumentHandler[T]): LegacyDocHandler[T] = legacyDocumentHandler[T](fromDocumentReader(h), fromDocumentWriter(h))

  /**
   * Based on the compatibility conversions,
   * provides instances of legacy `BSONDocumentWriter`
   * for the new BSON value API.
   */
  implicit def legacyWriterNewValue[B <: BSONValue, L](implicit w: BSONDocumentWriter[B], conv: L => B): LegacyDocWriter[L] = w.beforeWrite[L](conv)

  /**
   * Based on the compatibility conversions,
   * provides instances of legacy `BSONDocumentReader`
   * for the new BSON value API.
   */
  implicit def legacyReaderNewValue[B <: BSONValue, L](implicit r: BSONDocumentReader[B], conv: B => L): LegacyDocReader[L] = r.afterRead[L](conv)

}

private[bson] sealed trait LowPriorityHandlerConverters1
  extends LowPriorityHandlerConverters2 { _: HandlerConverters =>

  implicit final def toHandler[T, B <: LegacyValue](h: LegacyHandler[B, T])(implicit bc: ClassTag[B]): BSONHandler[T] = BSONHandler.provided[T](toReader(h), toWriter(h))

  /**
   * Provided there are both a `BSONDocumentReader`
   * and a `BSONDocumentWriter` for the given type `T`,
   * a `BSONDocumentHandler` is materialized.
   */
  final def providedDocumentHandler[T](implicit r: BSONDocumentReader[T], w: BSONDocumentWriter[T]): BSONDocumentHandler[T] = new DefaultDocumentHandler[T](r, w)

  implicit final def fromHandler[T](h: BSONHandler[T]): LegacyHandler[LegacyValue, T] = LegacyHandler.provided[LegacyValue, T](fromWriter(h), fromReader(h))

  final def legacyDocumentHandler[T](implicit r: LegacyDocReader[T], w: LegacyDocWriter[T]): LegacyDocHandler[T] = new LegacyHandler[LegacyDoc, T] with LegacyDocReader[T] with LegacyDocWriter[T] {
    def read(doc: LegacyDoc): T = r.read(doc)
    def write(value: T): LegacyDoc = w.write(value)
  }

}

private[bson] sealed trait LowPriorityHandlerConverters2
  extends LowPriorityHandlerConverters3 { _: LowPriorityHandlerConverters1 =>

  /**
   * Provided there is a legacy document writer, resolve a new one.
   */
  implicit final def toDocumentWriter[T](implicit w: LegacyDocWriter[T]): BSONDocumentWriter[T] = toDocumentWriterConv[T](w)

  /**
   * Provided there is a legacy document reader, resolve a new one.
   */
  implicit final def toDocumentReader[T](implicit r: LegacyDocReader[T]): BSONDocumentReader[T] = toDocumentReaderConv[T](r)

  /**
   * {{{
   * import reactivemongo.api.bson.compat.HandlerConverters.toDocumentWriterConv
   *
   * def foo[T](lw: reactivemongo.bson.BSONDocumentWriter[T]) = {
   *   val w: reactivemongo.api.bson.BSONDocumentWriter[T] = lw
   *   w
   * }
   * }}}
   */
  implicit final def toDocumentWriterConv[T](w: LegacyDocWriter[T]): BSONDocumentWriter[T] = BSONDocumentWriter[T] { t => ValueConverters.toDocument(w write t) }

  /**
   * {{{
   * import reactivemongo.api.bson.compat.HandlerConverters.toDocumentReaderConv
   *
   * def lorem[T](lw: reactivemongo.bson.BSONDocumentReader[T]) = {
   *   val w: reactivemongo.api.bson.BSONDocumentReader[T] = lw
   *   w
   * }
   * }}}
   */
  implicit final def toDocumentReaderConv[T](r: LegacyDocReader[T]): BSONDocumentReader[T] = BSONDocumentReader[T] { bson =>
    r.read(ValueConverters fromDocument bson)
  }

  /**
   * {{{
   * import reactivemongo.api.bson.compat.HandlerConverters.fromDocumentWriter
   *
   * def bar[T](lw: reactivemongo.api.bson.BSONDocumentWriter[T]) = {
   *   val w: reactivemongo.bson.BSONDocumentWriter[T] = lw
   *   w
   * }
   * }}}
   */
  implicit final def fromDocumentWriter[T](w: BSONDocumentWriter[T]): LegacyDocWriter[T] = LegacyDocWriter[T] { t =>
    w.writeTry(t) match {
      case Success(d) => ValueConverters.fromDocument(d)
      case Failure(e) => throw e
    }
  }

  /**
   * {{{
   * import reactivemongo.api.bson.compat.HandlerConverters.fromDocumentReader
   *
   * def foo[T](r: reactivemongo.api.bson.BSONDocumentReader[T]) = {
   *   val lr: reactivemongo.bson.BSONDocumentReader[T] = r
   *   lr
   * }
   * }}}
   */
  implicit final def fromDocumentReader[T](r: BSONDocumentReader[T]): LegacyDocReader[T] = LegacyDocReader[T] { lv =>
    r.readTry(ValueConverters toDocument lv) match {
      case Success(t) => t
      case Failure(e) => throw e
    }
  }
}

private[bson] sealed trait LowPriorityHandlerConverters3 {
  _: LowPriorityHandlerConverters2 =>

  implicit final def toWriter[T](w: LegacyWriter[T, _ <: LegacyValue]): BSONWriter[T] = BSONWriter[T] { t => ValueConverters.toValue(w write t) }

  implicit final def toReader[T, B <: LegacyValue](r: LegacyReader[B, T])(implicit bc: ClassTag[B]): BSONReader[T] = BSONReader.from[T] { bson =>
    bc.unapply(ValueConverters fromValue bson).map(r.read) match {
      case Some(result) => Success(result)

      case _ => Failure(exceptions.TypeDoesNotMatchException(
        bc.runtimeClass.getSimpleName,
        bson.getClass.getSimpleName))
    }
  }

  implicit final def fromWriter[T](w: BSONWriter[T]): LegacyWriter[T, LegacyValue] = SafeBSONWriter.unapply(w) match {
    case Some(sw) => LegacyWriter { t =>
      ValueConverters.fromValue(sw safeWrite t)
    }

    case _ => LegacyWriter {
      w.writeTry(_) match {
        case Success(v) => ValueConverters.fromValue(v)
        case Failure(e) => throw e
      }
    }
  }

  implicit final def fromReader[T](r: BSONReader[T]): LegacyReader[LegacyValue, T] = LegacyReader[LegacyValue, T] { lv =>
    r.readTry(ValueConverters toValue lv) match {
      case Success(t) => t

      case Failure(e) =>
        throw e
    }
  }
}
