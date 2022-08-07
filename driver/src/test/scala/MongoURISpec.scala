import scala.collection.immutable.ListSet

import scala.concurrent.Future

import reactivemongo.api.{
  Compressor,
  MongoConnection,
  MongoConnectionOptions,
  ReadConcern,
  ScramSha1Authentication,
  ScramSha256Authentication,
  X509Authentication,
  WriteConcern
}, MongoConnection.URIParsingException

import reactivemongo.core.errors.ReactiveMongoException

import org.specs2.concurrent.ExecutionEnv

import org.specs2.specification.core.Fragments

import reactivemongo.api.tests.{ ParsedURI, parseURIWithDB }

final class MongoURISpec(implicit ee: ExecutionEnv)
    extends org.specs2.mutable.Specification {

  "Mongo URI".title

  import MongoConnectionOptions.Credential
  import tests.Common, Common.timeout

  section("unit")
  "MongoConnection URI parser" should {
    val simplest = "mongodb://host1"

    s"parse $simplest with success" in {
      parseURI(simplest) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27017),
          db = None,
          options = MongoConnectionOptions.default,
          ignoredOptions = List.empty
        )
      ).awaitFor(timeout)
    }

    "fail without DB" in {
      parseURIWithDB(simplest, srvRecResolver(), txtResolver())
        .aka("with DB") must throwA[URIParsingException](
        s"Missing\\ database\\ name:\\ $simplest"
      ).awaitFor(Common.timeout)
    }

    val withOpts = "mongodb://host1?foo=bar"

    s"parse $withOpts with success" in {
      val expected = ParsedURI(
        hosts = ListSet("host1" -> 27017),
        db = None,
        options = MongoConnectionOptions.default,
        ignoredOptions = List("foo")
      )

      parseURI(withOpts) must beTypedEqualTo(expected).awaitFor(timeout) and {
        Common.driver.connect(expected) must throwA[IllegalArgumentException](
          "The connection URI contains unsupported options: foo"
        ).awaitFor(Common.timeout)
      }
    }

    val withPort = "mongodb://host1:27018"
    s"parse $withPort with success" in {
      parseURI(withPort) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018),
          db = None,
          options = MongoConnectionOptions.default,
          ignoredOptions = List.empty
        )
      ).awaitFor(timeout)
    }

    val withWrongPort = "mongodb://host1:68903"
    s"parse $withWrongPort with failure" in {
      parseURI(withWrongPort) must throwA[Exception].awaitFor(timeout)
    }

    val withWrongPort2 = "mongodb://host1:kqjbce"
    s"parse $withWrongPort2 with failure" in {
      parseURI(withWrongPort2) must throwA[Exception].awaitFor(timeout)
    }

    val withDb = "mongodb://host1/somedb"
    s"parse $withDb with success" in {
      parseURI(withDb) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27017),
          db = Some("somedb"),
          options = MongoConnectionOptions.default,
          ignoredOptions = List.empty
        )
      ).awaitFor(timeout)
    }

    val withAuth = "mongodb://user123:passwd123@host1/somedb"
    s"parse $withAuth with success" in {
      parseURI(withAuth) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27017),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(credentials =
            Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = List.empty
        )
      ).awaitFor(timeout)
    }

    val wrongWithAuth = "mongodb://user123:passwd123@host1"
    s"parse $wrongWithAuth with failure" in {
      parseURI(wrongWithAuth) must throwA[Exception].awaitFor(timeout)
    }

    val fullFeatured =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authenticationMechanism=scram-sha1"

    s"parse $fullFeatured with success" in {
      parseURI(fullFeatured) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            authenticationMechanism = ScramSha1Authentication,
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = List("foo")
        )
      ).awaitFor(timeout)
    }

    val scramSha256 =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authenticationMechanism=scram-sha256"

    s"parse $scramSha256 with success" in {
      parseURI(scramSha256) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            authenticationMechanism = ScramSha256Authentication,
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = List("foo")
        )
      ).awaitFor(timeout)
    }

    val withAuthParamAndSource1 =
      "mongodb://user123:;qGu:je/LX}nN\\8@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authenticationDatabase=authdb"

    // 'authSource' to be compatible with MongoDB Atlas options in SRV
    val withAuthParamAndSource2 =
      "mongodb://user123:;qGu:je/LX}nN\\8@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authSource=authdb"

    s"parse $withAuthParamAndSource1 with success" in {
      val expected = ParsedURI(
        hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
        db = Some("somedb"),
        options = MongoConnectionOptions.default.copy(
          authenticationDatabase = Some("authdb"),
          credentials =
            Map("authdb" -> Credential("user123", Some(";qGu:je/LX}nN\\8")))
        ),
        ignoredOptions = List("foo")
      )

      parseURI(withAuthParamAndSource1) must beTypedEqualTo(expected).awaitFor(
        timeout
      ) and {
        parseURI(withAuthParamAndSource2) must beTypedEqualTo(expected)
          .awaitFor(timeout)
      }
    }

    val withAuthModeX509WithNoUser =
      "mongodb://host1:27018,host2:27019,host3:27020/somedb?foo=bar&authenticationMechanism=x509"

    s"parse $withAuthModeX509WithNoUser with success" in {
      parseURI(withAuthModeX509WithNoUser) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            authenticationMechanism = X509Authentication,
            credentials = Map("somedb" -> Credential("", None))
          ),
          ignoredOptions = List("foo")
        )
      ).awaitFor(timeout)
    }

    val withAuthModeX509WithUser =
      "mongodb://username@test.com,CN=127.0.0.1,OU=TEST_CLIENT,O=TEST_CLIENT,L=LONDON,ST=LONDON,C=UK@host1:27018,host2:27019,host3:27020/somedb?foo=bar&authenticationMechanism=x509"

    s"parse $withAuthModeX509WithUser with success" in {
      parseURI(withAuthModeX509WithUser) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            authenticationMechanism = X509Authentication,
            credentials = Map(
              "somedb" -> Credential(
                "username@test.com,CN=127.0.0.1,OU=TEST_CLIENT,O=TEST_CLIENT,L=LONDON,ST=LONDON,C=UK",
                None
              )
            )
          ),
          ignoredOptions = List("foo")
        )
      ).awaitFor(timeout)
    }

    val withWriteConcern =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcern=journaled"

    s"parse $withWriteConcern with success" in {
      parseURI(withWriteConcern) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            writeConcern = WriteConcern.Journaled,
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = Nil
        )
      ).awaitFor(timeout)
    }

    val withWriteConcernWMaj =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?w=majority"

    s"parse $withWriteConcernWMaj with success" in {
      parseURI(withWriteConcernWMaj) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            writeConcern = WriteConcern.Default.copy(w = WriteConcern.Majority),
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = Nil
        )
      ).awaitFor(timeout)
    }

    val withWriteConcernWTag =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernW=anyTag"

    s"parse $withWriteConcernWTag with success" in {
      parseURI(withWriteConcernWTag) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            writeConcern =
              WriteConcern.Default.copy(w = WriteConcern.TagSet("anyTag")),
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = Nil
        )
      ).awaitFor(timeout)
    }

    val withWriteConcernWAck =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernW=5"

    s"parse $withWriteConcernWAck with success" in {
      parseURI(withWriteConcernWAck) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            writeConcern = WriteConcern.Default.copy(
              w = WriteConcern.WaitForAcknowledgments(5)
            ),
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = Nil
        )
      ).awaitFor(timeout)
    }

    val withWriteConcernJournaled =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?journal=true"

    s"parse $withWriteConcernJournaled with success" in {
      parseURI(withWriteConcernJournaled) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            writeConcern = WriteConcern.Default.copy(j = true),
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = Nil
        )
      ).awaitFor(timeout)
    }

    val withWriteConcernNJ =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?writeConcernJ=false&writeConcern=journaled"

    s"parse $withWriteConcernNJ with success" in {
      parseURI(withWriteConcernNJ) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            writeConcern = WriteConcern.Journaled.copy(j = false),
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = Nil
        )
      ).awaitFor(timeout)
    }

    val withWriteConcernTmout =
      "mongodb://user123:passwd123@host1:27018,host2:27019,host3:27020/somedb?wtimeoutMS=1543"

    s"parse $withWriteConcernTmout with success" in {
      parseURI(withWriteConcernTmout) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("host1" -> 27018, "host2" -> 27019, "host3" -> 27020),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            writeConcern = WriteConcern.Default.copy(wtimeout = Some(1543)),
            credentials =
              Map("somedb" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = Nil
        )
      ).awaitFor(timeout)
    }

    val withKeyStore =
      "mongodb://host1?keyStore=file:///tmp/foo&keyStoreType=PKCS12&keyStorePassword=bar"

    s"fail to parse $withKeyStore" in {
      parseURI(withKeyStore) must beLike[ParsedURI] {
        case uri =>
          uri.options.keyStore must beSome(
            MongoConnectionOptions.KeyStore(
              resource = new java.io.File("/tmp/foo").toURI,
              password = Some("bar".toCharArray),
              storeType = "PKCS12",
              trust = true
            )
          )

      }.awaitFor(timeout)
    }

    val defaultFo = "mongodb://host1?rm.failover=default"

    s"parse $defaultFo with success" in {
      parseURI(defaultFo) must beLike[ParsedURI] {
        case uri =>
          strategyStr(
            uri
          ) must_=== "100 milliseconds100 milliseconds200 milliseconds300 milliseconds500 milliseconds600 milliseconds700 milliseconds800 milliseconds1000 milliseconds1100 milliseconds1200 milliseconds" and {
            parseURI("mongodb://host1?retryWrites=true") must beTypedEqualTo(
              uri
            ).awaitFor(timeout)
          }
      }.awaitFor(timeout)
    }

    val remoteFo = "mongodb://host1?rm.failover=remote&writeConcernJ=true"

    s"parse $remoteFo with success" in {
      parseURI(remoteFo) must beLike[ParsedURI] {
        case uri =>
          strategyStr(
            uri
          ) must_=== "100 milliseconds100 milliseconds200 milliseconds300 milliseconds500 milliseconds600 milliseconds700 milliseconds800 milliseconds1000 milliseconds1100 milliseconds1200 milliseconds1300 milliseconds1500 milliseconds1600 milliseconds1700 milliseconds1800 milliseconds2000 milliseconds"
      }.awaitFor(timeout)
    }

    val strictFo = "mongodb://host1?rm.failover=strict&writeConcernJ=true"

    s"parse $strictFo with success" in {
      parseURI(strictFo) must beLike[ParsedURI] {
        case uri =>
          strategyStr(
            uri
          ) must_=== "100 milliseconds100 milliseconds200 milliseconds300 milliseconds500 milliseconds600 milliseconds" and {
            parseURI(
              "mongodb://host1?retryWrites=false&writeConcernJ=true"
            ) must beTypedEqualTo(uri).awaitFor(timeout)
          }
      }.awaitFor(timeout)
    }

    val customFo = "mongodb://host1?rm.failover=123ms:4x5&writeConcernJ=true"

    s"parse $customFo with success" in {
      parseURI(customFo) must beLike[ParsedURI] {
        case uri =>
          strategyStr(
            uri
          ) must_=== "123 milliseconds615 milliseconds1230 milliseconds1845 milliseconds2460 milliseconds"
      }.awaitFor(timeout)
    }

    val invalidNoNodes = "mongodb://?writeConcern=journaled"

    s"fail to parse $invalidNoNodes" in {
      parseURI(invalidNoNodes) must throwA[URIParsingException](
        "No valid host in the URI: ''"
      ).awaitFor(timeout)

    }

    val ignoredRs = "mongodb://host1?replicaSet=foo"

    s"ignore 'replicaSet' option" in {
      parseURI(ignoredRs) must beLike[ParsedURI] {
        case uri => uri.ignoredOptions must beEmpty
      }.awaitFor(timeout)
    }

    val foInvalidDelay = "mongodb://host1?rm.failover=123ko:4x5"

    s"fail to parse $foInvalidDelay" in {
      parseURI(foInvalidDelay) must beLike[ParsedURI] {
        case uri => uri.ignoredOptions.headOption must beSome("rm.failover")
      }.awaitFor(timeout)
    }

    val foInvalidRetry = "mongodb://host1?rm.failover=123ms:Ax5"

    s"fail to parse $foInvalidRetry" in {
      parseURI(foInvalidRetry) must beLike[ParsedURI] {
        case uri => uri.ignoredOptions.headOption must beSome("rm.failover")
      }.awaitFor(timeout)
    }

    val foInvalidFactor = "mongodb://host1?rm.failover=123ms:2xO"

    s"fail to parse $foInvalidFactor" in {
      parseURI(foInvalidFactor) must beLike[ParsedURI] {
        case uri => uri.ignoredOptions.headOption must beSome("rm.failover")
      }.awaitFor(timeout)
    }

    val monRefMS =
      "mongodb://host1?heartbeatFrequencyMS=567&rm.failover=123ms:4x5"

    s"parse $monRefMS with success" in {
      parseURI(monRefMS) must beLike[ParsedURI] {
        case uri => uri.options.heartbeatFrequencyMS must_=== 567
      }.awaitFor(timeout)
    }

    val maxInFlight = "mongodb://host1?rm.maxInFlightRequestsPerChannel=128"

    s"parse $maxInFlight with success" in {
      parseURI(maxInFlight) must beLike[ParsedURI] {
        case uri => uri.options.maxInFlightRequestsPerChannel must beSome(128)
      }.awaitFor(timeout)
    }

    val maxNonQueryableHeartbeats =
      "mongodb://host1?rm.maxNonQueryableHeartbeats=5"

    s"parse $maxNonQueryableHeartbeats with success" in {
      parseURI(maxNonQueryableHeartbeats) must beLike[ParsedURI] {
        case uri => uri.options.maxNonQueryableHeartbeats must_=== 5
      }.awaitFor(timeout)
    }

    "ignore option maxNonQueryableHeartbeats=0" in {
      parseURI("mongodb://host1?rm.maxNonQueryableHeartbeats=0").aka(
        "parsed URI"
      ) must beLike[ParsedURI] {
        case uri =>
          uri.options.maxNonQueryableHeartbeats must_=== 30 and {
            uri.ignoredOptions must (contain("rm.maxNonQueryableHeartbeats"))
          }
      }.awaitFor(timeout)
    }

    val invalidMonRef1 = "mongodb://host1?heartbeatFrequencyMS=A"

    s"fail to parse $invalidMonRef1" in {
      parseURI(invalidMonRef1) must beLike[ParsedURI] {
        case uri =>
          uri.ignoredOptions.headOption must beSome("heartbeatFrequencyMS")
      }.awaitFor(timeout)
    }

    val invalidMonRef2 = "mongodb://host1?heartbeatFrequencyMS=50"

    s"fail to parse $invalidMonRef2 (heartbeatFrequencyMS < 500)" in {
      parseURI(invalidMonRef2) must beLike[ParsedURI] {
        case uri =>
          uri.ignoredOptions.headOption must beSome("heartbeatFrequencyMS")
      }.awaitFor(timeout)
    }

    val invalidIdle =
      "mongodb://host1?maxIdleTimeMS=99&heartbeatFrequencyMS=500"

    s"fail to parse $invalidIdle (with maxIdleTimeMS < heartbeatFrequencyMS)" in {
      parseURI(invalidIdle) must throwA[MongoConnection.URIParsingException](
        "Invalid URI options: maxIdleTimeMS\\(99\\) < heartbeatFrequencyMS\\(500\\)"
      ).awaitFor(timeout)
    }

    val validSeedList = "mongodb+srv://usr:pwd@mongo.domain.tld/foo"

    s"parse seed list with success from $validSeedList" in {
      import org.xbill.DNS.{ Name, Record, SRVRecord, Type }

      def records = Array[Record](
        new SRVRecord(
          Name.fromConstantString("mongo.domain.tld."),
          Type.SRV,
          3600,
          1,
          1,
          27017,
          Name.fromConstantString("mongo1.domain.tld.")
        ),
        new SRVRecord(
          Name.fromConstantString("mongo.domain.tld."),
          Type.SRV,
          3600,
          1,
          1,
          27018,
          Name.fromConstantString("mongo2.domain.tld.")
        )
      )

      parseURI(
        validSeedList,
        srvRecResolver { name =>
          if (name == "mongo.domain.tld") {
            records
          } else {
            throw new IllegalArgumentException(s"Unexpected name '$name'")
          }
        }
      ) must beLike[ParsedURI] {
        case uri =>
          uri.db must beSome("foo") and {
            // enforced by default when seed list ...
            uri.options.sslEnabled must beTrue and {
              uri.hosts must_=== ListSet(
                "mongo1.domain.tld" -> 27017,
                "mongo2.domain.tld" -> 27018
              )
            } and {
              uri.options.credentials must_=== Map(
                "foo" -> Credential("usr", Some("pwd"))
              )
            }
          }
      }.awaitFor(timeout)
    }

    s"fail to parse seed list when target hosts are not with same base" in {
      import org.xbill.DNS.{ Name, Record, SRVRecord, Type }

      def records = Array[Record](
        new SRVRecord(
          Name.fromConstantString("mongo.domain.tld."),
          Type.SRV,
          3600,
          1,
          1,
          27017,
          Name.fromConstantString("mongo1.other.tld.")
        ),
        new SRVRecord(
          Name.fromConstantString("mongo.domain.tld."),
          Type.SRV,
          3600,
          1,
          1,
          27018,
          Name.fromConstantString("mongo2.other.tld.")
        )
      )

      parseURI(
        validSeedList,
        srvRecResolver { name =>
          if (name == "mongo.domain.tld") {
            records
          } else {
            throw new IllegalArgumentException(s"Unexpected name '$name'")
          }
        }
      ) must throwA[ReactiveMongoException](
        ".*mongo1\\.other\\.tld\\. is not subdomain of domain\\.tld\\..*"
      ).awaitFor(timeout)
    }

    s"fail to parse seed list when non-SRV records are resolved" in {
      import org.xbill.DNS.{ Name, Record, ARecord, Type }

      def records = Array[Record](
        new ARecord(
          Name.fromConstantString("mongo.domain.tld."),
          Type.A,
          3600,
          java.net.InetAddress.getLoopbackAddress
        )
      )

      parseURI(validSeedList, srvRecResolver(_ => records))
        .aka("failure") must throwA[ReactiveMongoException](
        ".*Unexpected record: mongo\\.domain\\.tld\\..*"
      ).awaitFor(timeout)
    }

    val fullFeaturedSeedList =
      "mongodb+srv://user123:passwd123@service.domain.tld/somedb?foo=bar&ssl=false"

    s"parse $fullFeatured with success" in {
      import org.xbill.DNS.{ Name, Record, SRVRecord, Type }

      parseURI(
        uri = fullFeaturedSeedList,
        srvResolver = srvRecResolver(_ =>
          Array[Record](
            new SRVRecord(
              Name.fromConstantString("mongo.domain.tld."),
              Type.SRV,
              3600,
              1,
              1,
              27017,
              Name.fromConstantString("mongo1.domain.tld.")
            )
          )
        ),
        txts = txtResolver({ name =>
          if (name == "service.domain.tld") {
            ListSet(
              "authenticationMechanism=scram-sha1",
              "authenticationDatabase=admin&ignore=this"
            )
          } else {
            throw new IllegalArgumentException(s"Unexpected: $name")
          }
        })
      ) must beTypedEqualTo(
        ParsedURI(
          hosts = ListSet("mongo1.domain.tld" -> 27017),
          db = Some("somedb"),
          options = MongoConnectionOptions.default.copy(
            sslEnabled = false, // overriden from URI
            authenticationDatabase = Some("admin"),
            authenticationMechanism = ScramSha1Authentication,
            credentials =
              Map("admin" -> Credential("user123", Some("passwd123")))
          ),
          ignoredOptions = List("foo", "ignore")
        )
      ).awaitFor(timeout)

    }

    val validName = "validName1"
    val withValidName = s"mongodb://host1?appName=$validName"

    s"parse $withValidName with success" in {
      parseURI(withValidName).map(_.options.appName) must beSome(validName)
        .awaitFor(timeout)
    }

    val withInvalidName = s"mongodb://host1?appName="

    s"ignore empty application name in $withInvalidName" in {
      parseURI(withInvalidName).map(_.options.appName) must beNone.awaitFor(
        timeout
      )
    }

    val withNameTooLong = {
      val tooLong = Array.fill[Byte](136)(72: Byte)
      s"""mongodb://host1?appName=${new String(tooLong, "UTF-8")}"""
    }

    s"ignore too long application name" in {
      parseURI(withNameTooLong).map(_.options.appName) must beNone.awaitFor(
        timeout
      )
    }

    "support readConcern" >> {
      Fragments.foreach(
        Seq(
          ReadConcern.Local,
          ReadConcern.Majority,
          ReadConcern.Linearizable,
          ReadConcern.Available
        )
      ) { c =>
        s"for $c" in {
          parseURI(s"mongodb://host1?readConcernLevel=${c.level}")
            .map(_.options.readConcern) must beTypedEqualTo(c).awaitFor(timeout)
        }
      }
    }

    "parse valid compressor" >> {
      Fragments.foreach(
        Seq[(String, ListSet[Compressor])](
          "snappy" -> ListSet(Compressor.Snappy),
          "zstd" -> ListSet(Compressor.Zstd),
          "zlib" -> ListSet(Compressor.Zlib.DefaultCompressor),
          "snappy,zstd" -> ListSet(Compressor.Snappy, Compressor.Zstd),
          "noop,zlib" -> ListSet(Compressor.Zlib.DefaultCompressor)
        )
      ) {
        case (name, compressors) =>
          name in {
            parseURI(s"mongodb://host?compressors=${name}").map(
              _.options.compressors
            ) must beTypedEqualTo(compressors).awaitFor(timeout)
          }
      }
    }

    "reject zlibCompressionLevel without zlib compressor" in {
      parseURI("mongodb://host?compressors=snappy&zlibCompressionLevel=1").map(
        _.options.compressors
      ) must beTypedEqualTo(ListSet[Compressor](Compressor.Snappy))
        .awaitFor(timeout)
    }

    "support zlibCompressionLevel" >> {
      Fragments.foreach(-1 to 9) {
        case level =>
          s"at ${level}" in {
            val uri: String =
              s"mongodb://host?compressors=zlib&zlibCompressionLevel=${level}"

            parseURI(uri).map(_.options.compressors) must beTypedEqualTo(
              ListSet[Compressor](Compressor.Zlib(level))
            ).awaitFor(timeout)
          }
      }
    }

    "reject invalid compressor" >> {
      Fragments.foreach(
        Seq[(String, ListSet[Compressor])](
          "foo" -> ListSet.empty[Compressor],
          "snappy,bar" -> ListSet(Compressor.Snappy),
          "lorem,zstd" -> ListSet(Compressor.Zstd)
        )
      ) {
        case (setting, compressors) =>
          setting in {
            parseURI(s"mongodb://host?compressors=${setting}")
              .aka("parsed") must beLike[ParsedURI] {
              case uri =>
                uri.ignoredOptions must_=== List("compressors") and {
                  uri.options.compressors must_=== compressors
                }
            }.awaitFor(timeout)
          }
      }
    }
  }

  section("unit")

  // ---

  import org.xbill.DNS.Record
  import reactivemongo.util.{ SRVRecordResolver, TXTResolver }

  private def srvRecResolver(
      services: String => Array[Record] = _ => Array.empty
    ): SRVRecordResolver = { _ => { (name: String) => Future(services(name)) } }

  private def txtResolver(
      resolve: String => ListSet[String] = _ => ListSet.empty
    ): TXTResolver = { (name: String) => Future(resolve(name)) }

  def parseURI(
      uri: String,
      srvResolver: SRVRecordResolver = srvRecResolver(),
      txts: TXTResolver = txtResolver()
    ) = reactivemongo.api.tests.parseURI(uri, srvResolver, txts)

  def strategyStr(uri: ParsedURI): String = {
    val fos = uri.options.failoverStrategy

    (1 to fos.retries)
      .foldLeft((new StringBuilder()) ++= fos.initialDelay.toString) { (d, i) =>
        d ++= (fos.initialDelay * (fos.delayFactor(i).toLong)).toString
      }
      .result()
  }
}
