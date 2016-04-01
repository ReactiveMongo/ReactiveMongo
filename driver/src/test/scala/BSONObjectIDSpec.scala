import reactivemongo.bson.BSONObjectID
import reactivemongo.bson.utils.Converters

class BSONObjectIDSpec extends org.specs2.mutable.Specification {
  "BSONObjectID" title

  "Object ID" should {
    "equal when created with string" in {
      val objectID = BSONObjectID.generate()
      val sameObjectID = BSONObjectID(objectID.stringify)

      objectID.valueAsArray must_== sameObjectID.valueAsArray
    }

    "equal another instance of BSONObjectID with the same value" in {
      val objectID = BSONObjectID.generate()
      val sameObjectID = BSONObjectID(objectID.stringify)

      objectID must_== sameObjectID
    }

    "not equal another newly generated instance of BSONObjectID" in {
      val objectID = BSONObjectID.generate()
      val nextObjectID = BSONObjectID(BSONObjectID.generate().stringify)

      objectID must not(beEqualTo(nextObjectID))
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
}
