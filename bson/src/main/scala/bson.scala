package reactivemongo.bson

object `package` {
  type BSONElement = (String, BSONValue)
}

sealed trait Producer[T] {
  private[bson] def produce: Option[T]
}

object Producer {
  case class NameOptionValueProducer(element: (String, Option[BSONValue])) extends Producer[(String, BSONValue)] {
    def produce = element._2.map(value => element._1 -> value)
  }

  case class OptionValueProducer(element: Option[BSONValue]) extends Producer[BSONValue] {
    def produce = element
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
  val code: Int
}

object BSONValue {
  import scala.util.Try
  implicit class ExtendedBSONValue[B <: BSONValue](bson: B) {
    def asTry[T](implicit reader: BSONReader[B, T]): Try[T] = {
      reader.readTry(bson)
    }
    def asOpt[T](implicit reader: BSONReader[B, T]): Option[T] = asTry.toOption
    def as[T](implicit reader: BSONReader[B, T]): T = asTry.get
  }
}