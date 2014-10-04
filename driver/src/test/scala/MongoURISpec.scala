import scala.util.{ Failure, Success, Try }

import org.specs2.mutable._

import reactivemongo.api._
import reactivemongo.api.MongoConnection.ParsedURI
import reactivemongo.core.nodeset.Authenticate

class MongoURISpec extends Specification {

  "MongoConnection URI parser" should {
    val simplest = "mongodb://host1"
    s"parse $simplest with success" in {
      val p = MongoConnection.parseURI(simplest)
      p mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withOpts = "mongodb://host1?foo=bar"
    s"parse $withOpts with success" in {
      val p = MongoConnection.parseURI(withOpts)
      p mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List("foo")))
    }

    val withPort = "mongodb://host1:27018"
    s"parse $withPort with success" in {
      val p = MongoConnection.parseURI(withPort)
      p mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27018),
          db = None,
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withWrongPort = "mongodb://host1:68903"
    s"parse $withWrongPort with failure" in {
      val p = MongoConnection.parseURI(withWrongPort)
      p.isFailure mustEqual true
    }

    val withWrongPort2 = "mongodb://host1:kqjbce"
    s"parse $withWrongPort2 with failure" in {
      val p = MongoConnection.parseURI(withWrongPort2)
      p.isFailure mustEqual true
    }

    val withDb = "mongodb://host1/somedb"
    s"parse $withDb with success" in {
      val p = MongoConnection.parseURI(withDb)
      p mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = Some("somedb"),
          authenticate = None,
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val withAuth = "mongodb://user123:passwd123@host1/somedb"
    s"parse $withAuth with success" in {
      val p = MongoConnection.parseURI(withAuth)
      p mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27017),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(),
          ignoredOptions = List()))
    }

    val wrongWithAuth = "mongodb://user123:passwd123@host1"
    s"parse $wrongWithAuth with failure" in {
      val p = MongoConnection.parseURI(wrongWithAuth)
      p.isFailure mustEqual true
    }

    val fullFeatured = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar"
    s"parse $fullFeatured with success" in {
      val p = MongoConnection.parseURI(fullFeatured)
      p mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(),
          ignoredOptions = List("foo")))
    }

    val withAuthParamAndSource = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authSource=authdb"
    s"parse $withAuthParamAndSource with success" in {
      val p = MongoConnection.parseURI(withAuthParamAndSource)
      p mustEqual Success(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("authdb", "user123", "passwd123")),
          options = MongoConnectionOptions(authSource = Some("authdb")),
          ignoredOptions = List("foo")))
    }
  }
}