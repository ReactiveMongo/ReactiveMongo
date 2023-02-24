package util

import reactivemongo.api.bson.{ BSONDocument, BSONReader }

import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._
import org.specs2.matcher.MustExpectations._

object BsonMatchers {

  def haveField[T](
      key: String
    )(implicit
      reader: BSONReader[T]
    ): HaveField[T] =
    new HaveField(key)

  class HaveField[T](
      key: String
    )(implicit
      reader: BSONReader[T]) {

    def that(matcher: Matcher[T]): Matcher[BSONDocument] = {
      (_: BSONDocument).getAsOpt[T](key) must beSome(matcher)
    }
  }
}
