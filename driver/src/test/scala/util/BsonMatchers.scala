package util

import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._
import org.specs2.matcher.MustExpectations._
import reactivemongo.bson.{ BSONDocument, BSONReader, BSONValue }

object BsonMatchers {

  def haveField[T](key: String)(implicit reader: BSONReader[_ <: BSONValue, T]): HaveField[T] = {
    new HaveField(key)
  }

  class HaveField[T](key: String)(implicit reader: BSONReader[_ <: BSONValue, T]) {
    def that(matcher: Matcher[T]): Matcher[BSONDocument] = {
      doc: BSONDocument =>
        doc.getAs[T](key) must beSome(matcher)
    }
  }
}
