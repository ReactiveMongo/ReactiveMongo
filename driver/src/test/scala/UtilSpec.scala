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

    import org.xbill.DNS.{ Lookup, Name, Resolver, Type }

    def defaultResolver: Resolver = {
      val r = Lookup.getDefaultResolver
      r.setTimeout(timeout.toSeconds.toInt)
      r
    }

    def srvRecords(
      name: String,
      srvPrefix: String = "_mongodb._tcp",
      resolver: Resolver = defaultResolver): List[String] = {
      val service = Name.fromConstantString(name + '.')
      // assert service.label >= 3

      val baseName = Name.fromString(
        name.dropWhile(_ != '.').drop(1), Name.root)

      val srvName = Name.concatenate(
        Name.fromConstantString(srvPrefix), service)

      val lookup = new Lookup(srvName, Type.SRV)

      lookup.setResolver(resolver)

      lookup.run().map { rec =>
        val nme = rec.getAdditionalName

        // if nme.isAbsolute then assert nme.subdomain(baseName)

        if (nme.isAbsolute) {
          nme.toString(true)
        } else {
          Name.concatenate(nme, baseName).toString(true)
        }
      }.toList
    }

    // ---

    "resolve SRV record for _imaps._tcp at gmail.com" in {
      srvRecords("gmail.com", "_imaps._tcp") must_=== List("imap.gmail.com")
    } tag "wip"
  }
}
