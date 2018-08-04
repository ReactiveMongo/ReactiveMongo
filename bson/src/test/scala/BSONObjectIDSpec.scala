import reactivemongo.bson.BSONObjectID
import reactivemongo.bson.utils.Converters

class BSONObjectIDSpec extends org.specs2.mutable.Specification {
  "BSONObjectID" title

  section("unit")
  "Object ID" should {
    "equal when created with string" in {
      val objectID = BSONObjectID.generate()

      BSONObjectID.parse(objectID.stringify).
        aka("parsed") must beSuccessfulTry[BSONObjectID].like {
          case oid => objectID.valueAsArray must_== oid.valueAsArray
        }
    }

    "equal another instance of BSONObjectID with the same value" in {
      val objectID = BSONObjectID.generate()

      BSONObjectID.parse(objectID.stringify).
        aka("parsed") must beSuccessfulTry[BSONObjectID].like {
          case oid => objectID must_== oid
        }
    }

    "not equal another newly generated instance of BSONObjectID" in {
      val objectID = BSONObjectID.generate()

      BSONObjectID.parse(BSONObjectID.generate().stringify).
        aka("parsed") must beSuccessfulTry[BSONObjectID].like {
          case oid => objectID must not(beEqualTo(oid))
        }
    }
  }

  "Converters" should {
    "generate strings equal each other" in {
      val objectID = "506fff5bb8f6b133007b5bcf"
      val hex = Converters.str2Hex(objectID)
      val string = Converters.hex2Str(hex)

      string must_== objectID
    }

    "generate bytes equal bytes converted from string" in {
      val objectID = BSONObjectID.generate()
      val bytes = Converters.str2Hex(objectID.stringify)

      objectID.valueAsArray must_== bytes
    }
  }
  section("unit")
}
