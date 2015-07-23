import scala.util.{ Failure, Success, Try }

import reactivemongo.api.{
  MongoConnection,
  MongoConnectionOptions,
  ScramSha1Authentication
}, MongoConnection.ParsedURI
import reactivemongo.core.nodeset.Authenticate
import reactivemongo.api.commands.WriteConcern

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

    val withWriteConcern = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcern=journaled"

    s"parse $withWriteConcern with success" in {
      MongoConnection.parseURI(withWriteConcern) must_== Success(ParsedURI(
        hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
        options = MongoConnectionOptions(
          writeConcern = WriteConcern.Journaled),
        ignoredOptions = Nil))

    }

    val withWriteConcernWMaj = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernW=majority"

    s"parse $withWriteConcernWMaj with success" in {
      MongoConnection.parseURI(withWriteConcernWMaj) must_== Success(ParsedURI(
        hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
        options = MongoConnectionOptions(
          writeConcern = WriteConcern.Default.copy(w = WriteConcern.Majority)),
        ignoredOptions = Nil))

    }

    val withWriteConcernWTag = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernW=anyTag"

    s"parse $withWriteConcernWTag with success" in {
      MongoConnection.parseURI(withWriteConcernWTag) must_== Success(ParsedURI(
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
      MongoConnection.parseURI(withWriteConcernWAck) must_== Success(ParsedURI(
        hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
        options = MongoConnectionOptions(
          writeConcern = WriteConcern.Default.copy(
            w = WriteConcern.WaitForAknowledgments(5))),
        ignoredOptions = Nil))

    }

    val withWriteConcernJournaled = "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernJ=true"

    s"parse $withWriteConcernJournaled with success" in {
      MongoConnection.parseURI(withWriteConcernJournaled) must_== Success(
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
      MongoConnection.parseURI(withWriteConcernNJ) must_== Success(
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
      MongoConnection.parseURI(withWriteConcernTmout) must_== Success(
        ParsedURI(
          hosts = List("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          authenticate = Some(Authenticate("somedb", "user123", "passwd123")),
          options = MongoConnectionOptions(
            writeConcern = WriteConcern.Default.copy(wtimeout = Some(1543))),
          ignoredOptions = Nil))

    }
  }
}
