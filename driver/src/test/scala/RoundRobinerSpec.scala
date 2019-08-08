package reactivemongo

import reactivemongo.core.nodeset.RoundRobiner

final class RoundRobinerSpec extends org.specs2.mutable.Specification {
  "Round robiner" title

  section("unit")

  "Selection" should {
    "pick no element" in {
      val r = RoundRobiner(List.empty[String])

      r.size must_=== 0 and {
        r.isEmpty must beTrue
      } and {
        r.pick must beNone
      } and {
        r.pickWithFilter(_ => true) must beNone
      } and {
        r.pickWithFilter(_ => false) must beNone
      } and {
        r.pickWithFilterAndPriority(_ => true, 2) must beNone
      } and {
        r.pickWithFilterAndPriority(_ => false, 2) must beNone
      }
    }

    "from non empty source" >> {
      val elements = List("foo", "bar", "lorem")

      "not be empty" in {
        val r = RoundRobiner(elements)

        r.size must_=== 3 and {
          r.isEmpty must beFalse
        }
      }

      "pick element without filter" in {
        val r = RoundRobiner(elements)

        (elements ::: elements).foldLeft(ok) { (res, e) =>
          res and {
            r.pick must beSome(e)
          }
        }
      }

      "pick element with filter" in {
        val r = RoundRobiner(elements)

        r.pickWithFilter(_ startsWith "b") must beSome("bar") and {
          r.pickWithFilter(_.size > 3) must beSome("lorem")
        } and {
          // loop again (skip 'foo' and 'bar')
          r.pickWithFilter(_.size > 3) must beSome("lorem")
        } and {
          r.pickWithFilter(_.size == 3) must beSome("foo")
        } and {
          r.pickWithFilter(_.size == 3) must beSome("bar")
        }
      }

      "pick element with filter and ordering" in {
        val r = RoundRobiner("ipsum" :: elements)
        // Note: Ordering[String] is used on the 2 unpriorised picked elements

        val revOrdering = implicitly[math.Ordering[String]].reverse

        r.pickWithFilterAndPriority(
          _ startsWith "b", 2) must beSome("bar") and {
            r.pickWithFilterAndPriority(_.size > 3, 2) must beSome("ipsum")
          } and {
            // loop again (skip 'foo' and 'bar')
            r.pickWithFilterAndPriority(_.size > 3, 2) must beSome("ipsum")
          } and {
            r.pickWithFilterAndPriority(_.size > 3, 2)(
              revOrdering) must beSome("lorem")
          } and {
            r.pickWithFilterAndPriority(_.size == 3, 2) must beSome("bar")
          } and {
            r.pickWithFilterAndPriority(_.size == 3, 2) must beSome("bar")
          } and {
            r.pickWithFilterAndPriority(_.size == 3, 2)(
              revOrdering) must beSome("foo")
          }
      }
    }
  }

  section("unit")
}
