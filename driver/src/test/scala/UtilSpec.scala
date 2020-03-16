import org.specs2.concurrent.ExecutionEnv

import scala.concurrent.Future

final class UtilSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Utilities" title

  section("unit")
  "URI" should {
    import scala.io.Source

    "be loaded from a local file" in {
      val resource = new java.io.File("/etc/hosts")

      reactivemongo.api.tests.withContent(resource.toURI) { in =>
        Source.fromInputStream(in).mkString must beTypedEqualTo(
          Source.fromFile(resource).mkString)
      }
    }

    "be loaded from classpath" in {
      val resource = getClass.getResource("/reference.conf")

      reactivemongo.api.tests.withContent(resource.toURI) { in =>
        Source.fromInputStream(in).mkString must beTypedEqualTo(
          Source.fromURL(resource).mkString)
      }
    }
  }

  "Simple ring" should {
    "have a fixed-size & be circular" in {
      val ring = reactivemongo.api.tests.SimpleRing[Int](3)

      ring.enqueue(1) must_=== 1 and {
        ring.enqueue(2) must_=== 2
      } and {
        ring.toArray() must_=== Array(1, 2)
      } and {
        ring.enqueue(3) must_=== 3
      } and {
        ring.enqueue(4) must_=== 3
      } and {
        ring.toArray() must_=== Array(2, 3, 4)
      } and {
        ring.dequeue() must beSome(2)
      } and {
        ring.dequeue() must beSome(3)
      } and {
        ring.enqueue(5) must_=== 2
      } and {
        ring.dequeue() must beSome(4)
      } and {
        ring.toArray() must_=== Array(5)
      } and {
        ring.enqueue(6) must_=== 2
      } and {
        ring.enqueue(7) must_=== 3
      } and {
        ring.enqueue(8) must_=== 3
      } and {
        ring.toArray() must_=== Array(6, 7, 8)
      } and {
        ring.dequeue() must beSome(6)
      } and {
        ring.dequeue() must beSome(7)
      }
    }
  }
  section("unit")

  "DNS resolver" should {
    "handle empty/null record" in {
      reactivemongo.api.tests.
        srvRecords("gmail.com") { _ => _ => Future.successful(null)
        } must beTypedEqualTo(List.empty[(String, Int)]).await

    }

    "resolve SRV record for _imaps._tcp at gmail.com" in {
      reactivemongo.api.tests.srvRecords(
        name = "gmail.com",
        srvPrefix = "_imaps._tcp") must beTypedEqualTo(
        List("imap.gmail.com" -> 993)).await

    }

    "resolve TXT record for gmail.com" in {
      reactivemongo.util.txtRecords().apply("gmail.com") must contain(
        ===("v=spf1 redirect=_spf.google.com")).await

    }
  }
}
