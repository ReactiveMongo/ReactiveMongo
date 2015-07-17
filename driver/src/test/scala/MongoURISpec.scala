import scala.util.{ Failure, Success, Try }

import reactivemongo.api.{
  MongoConnection,
  MongoConnectionOptions,
  ScramSha1Authentication
}, MongoConnection.ParsedURI
import reactivemongo.core.nodeset.Authenticate

class MongoURISpec extends org.specs2.mutable.Specification {
  "Mongo URI" title

  "MongoConnection URI parser" should {
    val simplest = "mongodb://host1"

    s"parse $simplest with success" in {
      MongoConnection.parseURI(simplest) mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withOpts = "mongodb://host1?foo=bar"

    s"parse $withOpts with success" in {
      MongoConnection.parseURI(withOpts) mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List("foo")))
    }

    val withPort = "mongodb://host1:27018"
    s"parse $withPort with success" in {
      MongoConnection.parseURI(withPort) mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27018),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withWrongPort = "mongodb://host1:68903"
    s"parse $withWrongPort with failure" in {
      MongoConnection.parseURI(withWrongPort).isFailure must beTrue
    }

    val withWrongPort2 = "mongodb://host1:kqjbce"
    s"parse $withWrongPort2 with failure" in {
      MongoConnection.parseURI(withWrongPort2).isFailure must beTrue
    }

    val withDb = "mongodb://host1/somedb"
    s"parse $withDb with success" in {
      MongoConnection.parseURI(withDb) mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = Some("somedb"),
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withAuth = "mongodb://user123:passwd123@host1/somedb"
    s"parse $withAuth with success" in {
      MongoConnection.parseURI(withAuth) mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val wrongWithAuth = "mongodb://user123:passwd123@host1"
    s"parse $wrongWithAuth with failure" in {
      MongoConnection.parseURI(wrongWithAuth).isFailure must beTrue
    }

    val fullFeatured = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authMode=scram-sha1"

    s"parse $fullFeatured with success" in {
      MongoConnection.parseURI(fullFeatured) mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(authMode = ScramSha1Authentication),
          ignoredOptions = List("foo")))
    }

    val withAuthParamAndSource = "mongodb://user123:;qGu:je/LX}nN\\8@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authSource=authdb"

    s"parse $withAuthParamAndSource with success" in {
      MongoConnection.parseURI(withAuthParamAndSource) mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate(
            "authdb", "user123", ";qGu:je/LX}nN\\8")),
          options = MongoConnectionOptions(authSource = Some("authdb")),
          ignoredOptions = List("foo")))
    }
  }
}
