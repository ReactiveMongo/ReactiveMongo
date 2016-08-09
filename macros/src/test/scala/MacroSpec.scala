import reactivemongo.bson.{
  BSON,
  BSONDocument,
  BSONDouble,
  BSONReader,
  BSONInteger,
  BSONWriter,
  Macros
}
import reactivemongo.bson.exceptions.DocumentKeyNotFound

class MacroSpec extends org.specs2.mutable.Specification {
  "Macros" title

  import MacroTest._

  "Formatter" should {
    "handle primitives" in {
      roundtrip(
        Primitives(1.2, "hai", true, 42, Long.MaxValue),
        Macros.handler[Primitives]
      )
    }

    "support nesting" in {
      implicit val personFormat = Macros.handler[Person]
      val doc = Pet("woof", Person("john", "doe"))
      roundtrip(doc, Macros.handler[Pet])
    }

    "support option" in {
      val format = Macros.handler[Optional]
      val some = Optional("some", Some("value"))
      val none = Optional("none", None)
      roundtrip(some, format)
      roundtrip(none, format)
    }

    "support seq" in {
      roundtrip(
        WordLover("john", Seq("hello", "world")),
        Macros.handler[WordLover]
      )
    }

    "support single member case classes" in {
      roundtrip(
        Single("Foo"),
        Macros.handler[Single]
      )
    }

    "support single member options" in {
      val f = Macros.handler[OptionalSingle]
      roundtrip(OptionalSingle(Some("foo")), f)
      roundtrip(OptionalSingle(None), f)
    }

    "support case class definitions inside an object" in {
      import Nest._
      roundtrip(Nested("foo"), Macros.handler[Nested])
    }

    "handle overloaded apply correctly" in {
      val doc1 = OverloadedApply("hello")
      val doc2 = OverloadedApply(List("hello", "world"))
      val f = Macros.handler[OverloadedApply]
      roundtrip(doc1, f)
      roundtrip(doc2, f)
    }

    "case class and handler inside trait" in {
      val t = new NestModule {}
      roundtrip(t.Nested("it works"), t.format)
    }

    "case class inside trait with handler outside" in {
      val t = new NestModule {}
      import t._ //you need Nested in scope because t.Nested won't work
      val format = Macros.handler[Nested]
      roundtrip(Nested("it works"), format)
    }

    "respect compilation options" in {
      val format = Macros.handlerOpts[Person, Macros.Options.Verbose] //more stuff in compiler log
      roundtrip(Person("john", "doe"), format)
    }

    "persist class name on demand" in {
      val person = Person("john", "doe")
      val format = Macros.handlerOpts[Person, Macros.Options.SaveClassName]
      val doc = format write person

      doc.getAs[String]("className") mustEqual Some("MacroTest.Person") and {
        roundtrip(person, format)
      }
    }

    "persist simple class name on demand" in {
      val person = Person("john", "doe")
      val format = Macros.handlerOpts[Person, Macros.Options.SaveSimpleName]
      val doc = format write person

      doc.getAs[String]("className") mustEqual Some("Person")
      roundtrip(person, format)
    }

    "handle union types (ADT)" in {
      import Union._
      import Macros.Options._
      val a = UA(1)
      val b = UB("hai")
      val format = Macros.handlerOpts[UT, UnionType[UA \/ UB \/ UC \/ UD]]

      /* TODO: Remove
      println(BSONDocument pretty (format write a))
      println(BSONDocument pretty (format write b))
       */

      format.write(a).getAs[String]("className").
        aka("class #1") must beSome("MacroTest.Union.UA") and {
          format.write(b).getAs[String]("className").
            aka("class #2") must beSome("MacroTest.Union.UB")
        } and roundtrip(a, format) and roundtrip(b, format)
    }

    "handle union types (ADT) with simple names" in {
      import Union._
      import Macros.Options._
      val a = UA(1)
      val b = UB("hai")
      val format = Macros.handlerOpts[UT, SimpleUnionType[UA \/ UB \/ UC \/ UD]]

      /* TODO: Remove
      println(BSONDocument pretty (format write a))
      println(BSONDocument pretty (format write b))
       */

      format.write(a).getAs[String]("className") must beSome("UA")
      format.write(b).getAs[String]("className") must beSome("UB")

      roundtrip(a, format) and roundtrip(b, format)
    }

    "handle recursive structure" in {
      import TreeModule._
      //handlers defined at tree module
      val tree: Tree = Node(Leaf("hi"), Node(Leaf("hello"), Leaf("world")))
      roundtrip(tree, Tree.bson)
    }

    "grab an implicit handler for type used in union" in {
      import TreeCustom._
      val tree: Tree = Node(Leaf("hi"), Node(Leaf("hello"), Leaf("world")))
      val serialized = BSON writeDocument tree
      val deserialized = BSON.readDocument[Tree](serialized)
      val expected = Node(Leaf("hai"), Node(Leaf("hai"), Leaf("hai")))

      deserialized mustEqual expected
    }

    "handle empty case classes" in {
      val empty = Empty()
      val format = Macros.handler[Empty]

      roundtrip(empty, format)
    }

    "do nothing with objects" in {
      val format = Macros.handler[EmptyObject.type]
      roundtrip(EmptyObject, format)
    }

    "handle ADTs with objects" in {
      import IntListModule._

      roundtripImp[IntList](Tail)
      roundtripImp[IntList](Cons(1, Cons(2, Cons(3, Tail))))
    }

    "automate Union on sealed traits" in {
      import Macros.Options._
      import Union._
      implicit val format = Macros.handlerOpts[UT, AllImplementations]

      format.write(UA(1)).getAs[String]("className").
        aka("class #1") must beSome("MacroTest.Union.UA") and {
          format.write(UB("buzz")).getAs[String]("className").
            aka("class #2") must beSome("MacroTest.Union.UB")
        } and roundtripImp[UT](UA(17)) and roundtripImp[UT](UB("foo")) and {
          roundtripImp[UT](UC("bar")) and roundtripImp[UT](UD("baz"))
        }
    }

    "support automatic implementations search with nested traits" in {
      import Macros.Options._
      import InheritanceModule._
      implicit val format = Macros.handlerOpts[T, AllImplementations]

      format.write(A()).getAs[String]("className").
        aka("class #1") must beSome("MacroTest.InheritanceModule.A") and {
          format.write(B).getAs[String]("className").
            aka("class #2") must beSome("MacroTest.InheritanceModule.B")
        } and {
          roundtripImp[T](A()) and roundtripImp[T](B) and roundtripImp[T](C())
        }
    }

    "automate Union on sealed traits with simple name" in {
      import Macros.Options._
      import Union._
      implicit val format = Macros.handlerOpts[UT, SimpleAllImplementations]

      format.write(UA(1)).getAs[String]("className") must beSome("UA")
      format.write(UB("buzz")).getAs[String]("className") must beSome("UB")

      roundtripImp[UT](UA(17)) and roundtripImp[UT](UB("foo")) and {
        roundtripImp[UT](UC("bar")) and roundtripImp[UT](UD("baz"))
      }
    }

    "support automatic implementations search with nested traits with simple name" in {
      import Macros.Options._
      import InheritanceModule._
      implicit val format = Macros.handlerOpts[T, SimpleAllImplementations]

      format.write(A()).getAs[String]("className") must beSome("A")
      format.write(B).getAs[String]("className") must beSome("B")

      roundtripImp[T](A()) and roundtripImp[T](B) and roundtripImp[T](C())
    }

    "support overriding keys with annotations" in {
      implicit val format = Macros.handler[RenamedId]
      val doc = RenamedId(value = "some value")
      val serialized = format write doc

      /* TODO: Remove
      println("renaming")
      println(BSONDocument.pretty(serialized))
       */

      serialized mustEqual (
        BSONDocument("_id" -> doc.myID, "value" -> doc.value)
      ) and {
          format.read(serialized) must_== doc
        }
    }

    "skip ignored fields" in {
      val pairHandler = Macros.handler[Pair]
      val doc = pairHandler.write(Pair(left = "left", right = "right"))

      doc.isEmpty must beFalse and {
        doc.aka(BSONDocument.pretty(doc)) must beTypedEqualTo(
          BSONDocument("right" -> "right")
        )
      }
    }
  }

  "Reader" should {
    "throw meaningful exception if required field is missing" in {
      val personDoc = BSONDocument("firstName" -> "joe")

      Macros.reader[Person].read(personDoc) must throwA[DocumentKeyNotFound].
        like { case e => e.getMessage must contain("lastName") }
    }

    "throw meaningful exception if field has another type" in {
      val primitivesDoc = BSONDocument(
        "dbl" -> 2D, "str" -> "str", "bl" -> true, "int" -> 2D, "long" -> 2L
      )

      Macros.reader[Primitives].read(primitivesDoc).
        aka("read") must throwA[ClassCastException].like {
          case e =>
            e.getMessage must contain(classOf[BSONDouble].getName) and {
              e.getMessage must contain(classOf[BSONInteger].getName)
            }
        }
    }

    "be generated for a generated case class" in {
      implicit def singleReader = Macros.reader[Single]
      val r = Macros.reader[Foo[Single]]

      r.read(BSONDocument(
        "bar" -> BSONDocument("value" -> "A"),
        "lorem" -> "ipsum"
      )) must_== Foo(Single("A"), "ipsum")
    }
  }

  // ---

  def roundtrip[A](original: A, format: BSONReader[BSONDocument, A] with BSONWriter[A, BSONDocument]) = {
    val serialized = format write original
    val deserialized = format read serialized

    original mustEqual deserialized
  }

  def roundtripImp[A](data: A)(implicit format: BSONReader[BSONDocument, A] with BSONWriter[A, BSONDocument]) = roundtrip(data, format)

}
