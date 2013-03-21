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

object `package` extends DefaultBSONHandlers {
  type BSONElement = (String, BSONValue)
}

sealed trait Producer[T] {
  private[bson] def produce: Option[T]
}

object Producer {
  case class NameOptionValueProducer(private val element: (String, Option[BSONValue])) extends Producer[(String, BSONValue)] {
    private[bson] def produce = element._2.map(value => element._1 -> value)
  }

  case class OptionValueProducer(private val element: Option[BSONValue]) extends Producer[BSONValue] {
    private[bson] def produce = element
  }

  implicit def nameValue2Producer[T](element: (String, T))(implicit writer: BSONWriter[T, _ <: BSONValue]) =
    NameOptionValueProducer(element._1, Some(writer.write(element._2)))

  implicit def nameOptionValue2Producer[T](element: (String, Option[T]))(implicit writer: BSONWriter[T, _ <: BSONValue]) =
    NameOptionValueProducer(element._1, element._2.map(value => writer.write(value)))

  implicit def noneOptionValue2Producer(element: (String, None.type)) =
    NameOptionValueProducer(element._1, None)

  implicit def valueProducer[T](element: T)(implicit writer: BSONWriter[T, _ <: BSONValue]) =
    OptionValueProducer(Some(writer.write(element)))

  implicit def optionValueProducer[T](element: Option[T])(implicit writer: BSONWriter[T, _ <: BSONValue]) =
    OptionValueProducer(element.map(writer.write(_)))

  implicit def noneOptionValueProducer(element: None.type) =
    OptionValueProducer(None)
}

trait BSONValue {
  val code: Byte
}

object BSONValue {
  import scala.util.Try
  implicit class ExtendedBSONValue[B <: BSONValue](val bson: B) extends AnyVal {
    def asTry[T](implicit reader: BSONReader[B, T]): Try[T] = {
      reader.readTry(bson)
    }
    def asOpt[T](implicit reader: BSONReader[B, T]): Option[T] = asTry(reader).toOption
    def as[T](implicit reader: BSONReader[B, T]): T = asTry(reader).get

    def seeAsTry[T](implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] =
      Try { reader.asInstanceOf[BSONReader[BSONValue, T]].readTry(bson) }.flatten

    def seeAsOpt[T](implicit reader: BSONReader[_ <: BSONValue, T]): Option[T] =
      seeAsTry[T].toOption
  }
}

object BSON {
  def read[T](doc: BSONDocument)(implicit reader: BSONReader[BSONDocument, T]): T = reader.read(doc)
  def write[T](t: T)(implicit writer: BSONWriter[T, BSONDocument]): BSONDocument = writer.write(t)
}

object Macros {
  import language.experimental.macros

  def reader[A]: BSONReader[BSONDocument, A] = macro MacroImpl.reader[A, Options.Default]
  def readerOpts[A, Opts <: Options.Default]: BSONReader[BSONDocument, A] = macro MacroImpl.reader[A, Opts]

  def writer[A]: BSONWriter[A, BSONDocument] = macro MacroImpl.writer[A, Options.Default]
  def writerOpts[A, Opts  <: Options.Default]: BSONWriter[A, BSONDocument] = macro MacroImpl.writer[A,Opts]

  def handler[A]: BSONReader[BSONDocument, A] with BSONWriter[A, BSONDocument] = macro MacroImpl.handler[A, Options.Default]
  def handlerOpts[A, Opts  <: Options.Default]: BSONReader[BSONDocument, A] with BSONWriter[A, BSONDocument] = macro MacroImpl.handler[A, Opts]

  object Options {
    trait Default
    trait Verbose extends Default
    trait SaveClassName extends Default
    trait UnionType[Types <: \/[_,_]] extends SaveClassName with Default

    trait \/[A,B]
  }
}
