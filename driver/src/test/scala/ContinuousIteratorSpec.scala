package reactivemongo

import reactivemongo.core.nodeset.ContinuousIterator

final class ContinuousIteratorSpec extends org.specs2.mutable.Specification {
  "Continuous Iterator" title

  section("unit")

  "Iterator" should {
    "be empty" in {
      val it = ContinuousIterator(List.empty[String])

      it.isEmpty must beTrue and (it.size must_=== 0) and {
        it.toList must_=== List.empty[String]
      }
    }

    "continuously return a single element" in {
      val it = ContinuousIterator(List("foo"))

      it.take(3).toList must_=== List("foo", "foo", "foo")
    }

    "continuously return the 2 underlying elements" in {
      val it = ContinuousIterator(List("foo", "bar"))

      it.take(3).toList must_=== List("foo", "bar", "foo") and {
        it.take(1).toList must_=== List("bar")
      } and {
        it.hasNext must beTrue
      } and {
        it.take(4).toSeq must_=== Seq("foo", "bar", "foo", "bar")
      }
    }
  }

  section("unit")
}
