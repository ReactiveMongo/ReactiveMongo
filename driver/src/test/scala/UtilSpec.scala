class UtilSpec extends org.specs2.mutable.Specification {
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
  section("unit")

  "DNS resolver" should {
    import Common.timeout

    // ---

    import org.xbill.DNS.{ Lookup, Resolver, Type }

    def defaultResolver: Resolver = {
      val r = Lookup.getDefaultResolver
      r.setTimeout(timeout.toSeconds.toInt)
      r
    }

    def txtRecords(
      name: String,
      resolver: Resolver = defaultResolver): List[String] = {

      val lookup = new Lookup(name, Type.TXT)

      lookup.setResolver(resolver)

      lookup.run().map { rec =>
        val data = rec.rdataToString
        val stripped = data.stripPrefix("\"")

        if (stripped == data) {
          data
        } else {
          stripped.stripSuffix("\"")
        }
      }.toList
    }

    // ---

    "resolve SRV record for _imaps._tcp at gmail.com" in {
      reactivemongo.util.srvRecords(
        name = "gmail.com",
        srvPrefix = "_imaps._tcp") must_=== List("imap.gmail.com")

    } tag "wip"

    "resolve TXT record for gmail.com" in {
      txtRecords("gmail.com") must_=== List("v=spf1 redirect=_spf.google.com")
    } tag "wip"
  }
}
