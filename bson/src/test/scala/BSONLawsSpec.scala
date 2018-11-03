import reactivemongo.bson._

import org.typelevel.discipline.specs2.mutable.Discipline

import spire.laws.GroupLaws
import spire.algebra.{
  Additive,
  AdditiveSemigroup,
  AdditiveMonoid,
  Eq,
  Monoid,
  Semigroup
}

import reactivemongo.BSONValueFixtures

class BSONLawsSpecs
  extends org.specs2.mutable.Specification with Discipline {

  "BSON laws" title

  import BSONCheck._
  import LawEvidences._

  { // Addition semigroup
    val semigroup = new Semigroup[BSONValue] {
      def combine(x: BSONValue, y: BSONValue): BSONValue = BSONValue.Addition(x, y)
    }
    implicit val additive: AdditiveSemigroup[BSONValue] = Additive(semigroup)

    checkAll("BSONValue", GroupLaws[BSONValue].additiveSemigroup)
  }

  { // Composition monoid
    val monoid = new Monoid[ElementProducer] {
      val empty = ElementProducer.Empty
      def combine(x: ElementProducer, y: ElementProducer): ElementProducer =
        ElementProducer.Composition(x, y)
    }

    implicit val additive: AdditiveMonoid[ElementProducer] =
      Additive(monoid)

    checkAll("ElementProducer", GroupLaws[ElementProducer].additiveMonoid)

    "foo" in {
      /*
      val a = BSONElement("97300710", BSONJavaScriptWS("bar()"))
      val b = BSONElement("808965863", BSONDouble(0.0012345D))
      val c = BSONElement("97300710", BSONJavaScript("bar()"))
       */

      val a = BSONDocument("foo" -> "bar")
      val b = BSONArray(BSONString("lorem"))
      val c = BSONElement("-471589850", BSONBoolean(false))

      val op = ElementProducer.Composition.apply _

      println(s"---> ${BSONArray pretty op(b, c).asInstanceOf[BSONArray]}")

      val r1 = op(op(a, b), c)
      val r2 = op(a, op(b, c))

      println(s"r1 = $r1")
      println(s"r2 = $r2 / ${BSONArray pretty op(b, c).asInstanceOf[BSONArray]}")

      println(s"r1 = ${BSONDocument pretty r1.asInstanceOf[BSONDocument]}")
      println(s"r2 = ${BSONDocument pretty r2.asInstanceOf[BSONDocument]}")

      /*
      println(s"r1 = ${BSONArray pretty r1.asInstanceOf[BSONArray]}")
      println(s"r2 = ${BSONArray pretty r2.asInstanceOf[BSONArray]}")
       */

      r1 must_=== r2
    } tag "wip"
  }
}

object LawEvidences {
  implicit def defaultEq[T <% Any]: Eq[T] = new Eq[T] {
    def eqv(x: T, y: T): Boolean = x == y
  }
}

object BSONCheck {
  import org.scalacheck._

  val bsonValueGen: Gen[BSONValue] =
    Gen.oneOf[BSONValue](BSONValueFixtures.bsonValueFixtures)

  val elementProducerGen: Gen[ElementProducer] =
    Gen.oneOf[ElementProducer](BSONValueFixtures.elementProducerFixtures)

  implicit val bsonValueArb: Arbitrary[BSONValue] = Arbitrary(bsonValueGen)

  implicit val elementProducerArb: Arbitrary[ElementProducer] =
    Arbitrary(elementProducerGen)

}
