import reactivemongo.api.{
  MongoConnection,
  MongoConnectionOptions,
  ScramSha1Authentication
}, MongoConnection.{ ParsedURI, parseURI }

import reactivemongo.core.nodeset.Authenticate
import reactivemongo.api.commands.WriteConcern

class MongoURISpec extends org.specs2.mutable.Specification {
  "Mongo URI" title

  section("unit")
  "MongoConnection URI parser" should {
    val simplest = "mongodb://host1"

    s"parse $simplest with success" in {
      parseURI(simplest) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withOpts = "mongodb://host1?foo=bar"

    s"parse $withOpts with success" in {
      val expected = ParsedURI(
        hosts = List("host1" -> 27017),
        db = None,
        authenticate = None,
        options = MongoConnectionOptions(),
        ignoredOptions = List("foo"))

      parseURI(withOpts) must beSuccessfulTry(expected) and {
        Common.driver.connection(expected, true) must beFailedTry.
          withThrowable[IllegalArgumentException](
            "The connection URI contains unsupported options: foo")
      }
    }

    val withPort = "mongodb://host1:27018"
    s"parse $withPort with success" in {
      parseURI(withPort) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27018),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withWrongPort = "mongodb://host1:68903"
    s"parse $withWrongPort with failure" in {
      parseURI(withWrongPort).isFailure must beTrue
    }

    val withWrongPort2 = "mongodb://host1:kqjbce"
    s"parse $withWrongPort2 with failure" in {
      parseURI(withWrongPort2).isFailure must beTrue
    }

    val withDb = "mongodb://host1/somedb"
    s"parse $withDb with success" in {
      parseURI(withDb) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = Some("somedb"),
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withAuth = "mongodb://user123:passwd123@host1/somedb"
    s"parse $withAuth with success" in {
      parseURI(withAuth) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val wrongWithAuth = "mongodb://user123:passwd123@host1"
    s"parse $wrongWithAuth with failure" in {
      parseURI(wrongWithAuth).isFailure must beTrue
    }

    val fullFeatured = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authMode=scram-sha1"

    s"parse $fullFeatured with success" in {
      parseURI(fullFeatured) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(authMode = ScramSha1Authentication),
          ignoredOptions = List("foo")))
    }

    val withAuthParamAndSource = "mongodb://user123:;qGu:je/LX}nN\\8@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authSource=authdb"

    s"parse $withAuthParamAndSource with success" in {
      parseURI(withAuthParamAndSource) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate(
            "authdb", "user123", ";qGu:je/LX}nN\\8")),
          options = MongoConnectionOptions(authSource = Some("authdb")),
          ignoredOptions = List("foo")))
    }

    val withWriteConcern = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcern=journaled"

    s"parse $withWriteConcern with success" in {
      parseURI(withWriteConcern) must beSuccessfulTry(ParsedURI(
        hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
        options = MongoConnectionOptions(
          writeConcern = WriteConcern.Journaled),
        ignoredOptions = Nil))
    }

    val withWriteConcernWMaj = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernW=majority"

    s"parse $withWriteConcernWMaj with success" in {
      parseURI(withWriteConcernWMaj) must beSuccessfulTry(ParsedURI(
        hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
        options = MongoConnectionOptions(
          writeConcern = WriteConcern.Default.copy(w = WriteConcern.Majority)),
        ignoredOptions = Nil))
    }

    val withWriteConcernWTag = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernW=anyTag"

    s"parse $withWriteConcernWTag with success" in {
      parseURI(withWriteConcernWTag) must beSuccessfulTry(ParsedURI(
        hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
        options = MongoConnectionOptions(
          writeConcern = WriteConcern.Default.copy(
            w = WriteConcern.TagSet("anyTag"))),
        ignoredOptions = Nil))
    }

    val withWriteConcernWAck = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernW=5"

    s"parse $withWriteConcernWAck with success" in {
      parseURI(withWriteConcernWAck) must beSuccessfulTry(ParsedURI(
        hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
        options = MongoConnectionOptions(
          writeConcern = WriteConcern.Default.copy(
            w = WriteConcern.WaitForAcknowledgments(5))),
        ignoredOptions = Nil))
    }

    val withWriteConcernJournaled = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernJ=true"

    s"parse $withWriteConcernJournaled with success" in {
      parseURI(withWriteConcernJournaled) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(
            writeConcern = WriteConcern.Default.copy(j = true)),
          ignoredOptions = Nil))
    }

    val withWriteConcernNJ = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernJ=false&writeConcern=journaled"

    s"parse $withWriteConcernNJ with success" in {
      parseURI(withWriteConcernNJ) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(
            writeConcern = WriteConcern.Journaled.copy(j = false)),
          ignoredOptions = Nil))
    }

    val withWriteConcernTmout = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernTimeout=1543"

    s"parse $withWriteConcernTmout with success" in {
      parseURI(withWriteConcernTmout) must beSuccessfulTry(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(
            writeConcern = WriteConcern.Default.copy(wtimeout = Some(1543))),
          ignoredOptions = Nil))
    }

    val defaultFo = "mongodb://host1?rm.failover=default"

    s"parse $defaultFo with success" in {
      parseURI(defaultFo) must beSuccessfulTry[ParsedURI].like {
        case uri =>
          strategyStr(uri) must_== "100 milliseconds100 milliseconds200 milliseconds300 milliseconds500 milliseconds600 milliseconds700 milliseconds800 milliseconds1000 milliseconds1100 milliseconds1200 milliseconds"
      }
    }

    val remoteFo = "mongodb://host1?rm.failover=remote&writeConcernJ=true"

    s"parse $remoteFo with success" in {
      parseURI(remoteFo) must beSuccessfulTry[ParsedURI].like {
        case uri =>
          strategyStr(uri) must_== "100 milliseconds100 milliseconds200 milliseconds300 milliseconds500 milliseconds600 milliseconds700 milliseconds800 milliseconds1000 milliseconds1100 milliseconds1200 milliseconds1300 milliseconds1500 milliseconds1600 milliseconds1700 milliseconds1800 milliseconds2000 milliseconds"
      }
    }

    val strictFo = "mongodb://host1?rm.failover=strict&writeConcernJ=true"

    s"parse $strictFo with success" in {
      parseURI(strictFo) must beSuccessfulTry[ParsedURI].like {
        case uri =>
          strategyStr(uri) must_== "100 milliseconds100 milliseconds200 milliseconds300 milliseconds500 milliseconds600 milliseconds"
      }
    }

    val customFo = "mongodb://host1?rm.failover=123ms:4x5&writeConcernJ=true"

    s"parse $customFo with success" in {
      parseURI(customFo) must beSuccessfulTry[ParsedURI].like {
        case uri =>
          strategyStr(uri) must_== "123 milliseconds615 milliseconds1230 milliseconds1845 milliseconds2460 milliseconds"
      }
    }

    val foInvalidDelay = "mongodb://host1?rm.failover=123ko:4x5"

    s"fail to parse $foInvalidDelay" in {
      parseURI(foInvalidDelay) must beSuccessfulTry[ParsedURI].like {
        case uri => uri.ignoredOptions.headOption must beSome("rm.failover")
      }
    }

    val foInvalidRetry = "mongodb://host1?rm.failover=123ms:Ax5"

    s"fail to parse $foInvalidRetry" in {
      parseURI(foInvalidRetry) must beSuccessfulTry[ParsedURI].like {
        case uri => uri.ignoredOptions.headOption must beSome("rm.failover")
      }
    }

    val foInvalidFactor = "mongodb://host1?rm.failover=123ms:2xO"

    s"fail to parse $foInvalidFactor" in {
      parseURI(foInvalidFactor) must beSuccessfulTry[ParsedURI].like {
        case uri => uri.ignoredOptions.headOption must beSome("rm.failover")
      }
    }

    val monRefMS = "mongodb://host1?rm.monitorRefreshMS=456&rm.failover=123ms:4x5"

    s"parse $monRefMS with success" in {
      parseURI(monRefMS) must beSuccessfulTry[ParsedURI].like {
        case uri => uri.options.monitorRefreshMS must_== 456
      }
    }

    val invalidMonRef1 = "mongodb://host1?rm.monitorRefreshMS=A"

    s"fail to parse $invalidMonRef1" in {
      parseURI(invalidMonRef1) must beSuccessfulTry[ParsedURI].like {
        case uri =>
          uri.ignoredOptions.headOption must beSome("rm.monitorRefreshMS")
      }
    }

    val invalidMonRef2 = "mongodb://host1?rm.monitorRefreshMS=50"

    s"fail to parse $invalidMonRef2 (monitorRefreshMS < 100)" in {
      parseURI(invalidMonRef2) must beSuccessfulTry[ParsedURI].like {
        case uri =>
          uri.ignoredOptions.headOption must beSome("rm.monitorRefreshMS")
      }
    }

    val invalidIdle = "mongodb://host1?maxIdleTimeMS=99&rm.monitorRefreshMS=100"

    s"fail to parse $invalidIdle (with maxIdleTimeMS < monitorRefreshMS)" in {
      parseURI(invalidIdle) must beFailedTry[ParsedURI].withThrowable[MongoConnection.URIParsingException]("Invalid URI options: maxIdleTimeMS\\(99\\) < monitorRefreshMS\\(100\\)")
    }
  }
  section("unit")

  // ---

  def strategyStr(uri: ParsedURI): String = {
    val fos = uri.options.failoverStrategy

    (1 to fos.retries).foldLeft(
      StringBuilder.newBuilder ++= fos.initialDelay.toString) { (d, i) =>
        d ++= (fos.initialDelay * (fos.delayFactor(i).toLong)).toString
      }.result()
  }
}
