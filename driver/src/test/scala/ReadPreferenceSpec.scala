import reactivemongo.bson.{ BSONArray, BSONDocument }
import reactivemongo.api.ReadPreference

import org.specs2.specification.core.Fragments

import reactivemongo.api.tests.{ bsonReadPref => bson }

class ReadPreferenceSpec extends org.specs2.mutable.Specification {
  "Read preference" title

  section("unit")
  "BSON read preference" should {
    Fragments.foreach[(ReadPreference, String)](Seq(
      ReadPreference.primary -> "primary",
      ReadPreference.secondary -> "secondary",
      ReadPreference.nearest -> "nearest")) {
      case (pref, mode) =>
        s"""be encoded as '{ "mode": "$mode" }'""" in {
          bson(pref) must_== BSONDocument("mode" -> mode)
        }
    }

    "be taggable and" >> {
      val tagSet = List(
        Map("foo" -> "bar", "lorem" -> "ipsum"),
        Map("dolor" -> "es"))
      val bsonTags = BSONArray(
        BSONDocument("foo" -> "bar", "lorem" -> "ipsum"),
        BSONDocument("dolor" -> "es"))

      Fragments.foreach[(ReadPreference, String)](Seq(
        ReadPreference.primaryPreferred(tagSet) -> "primaryPreferred",
        ReadPreference.secondary(tagSet) -> "secondary",
        ReadPreference.secondaryPreferred(tagSet) -> "secondaryPreferred",
        ReadPreference.nearest(tagSet) -> "nearest")) {
        case (pref, mode) =>
          val expected = BSONDocument("mode" -> mode, "tags" -> bsonTags)

          s"be encoded as '${BSONDocument pretty expected}'" in {
            bson(pref) must_== expected
          }
      }
    }
  }
  section("unit")
}
