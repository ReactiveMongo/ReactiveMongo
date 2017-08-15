import reactivemongo.bson.{
  BSON,
  BSONDocument,
  BSONDocumentHandler,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONDouble,
  BSONHandler,
  BSONInteger,
  BSONReader,
  BSONString,
  BSONWriter,
  Macros
}
import reactivemongo.bson.exceptions.DocumentKeyNotFound

import org.specs2.matcher.MatchResult

class MacroSpec extends org.specs2.mutable.Specification {
  "Macros" title

  import MacroTest._
  import BSONDocument.pretty

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

      roundtrip(some, format) and roundtrip(none, format)
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

      roundtrip(OptionalSingle(Some("foo")), f) and {
        roundtrip(OptionalSingle(None), f)
      }
    }

    "support generic case class Foo" >> {
      implicit def singleHandler = Macros.handler[Single]

      "directly" in {
        roundtrip(Foo(Single("A"), "ipsum"), Macros.handler[Foo[Single]])
      }

      "from generic function" in {
        def handler[T](implicit w: BSONDocumentWriter[T], r: BSONDocumentReader[T]) = Macros.handler[Foo[T]]

        roundtrip(Foo(Single("A"), "ipsum"), handler[Single])
      }
    }

    "support generic case class GenSeq" in {
      implicit def singleHandler = new BSONWriter[Single, BSONString] with BSONReader[BSONString, Single] with BSONHandler[BSONString, Single] {
        def write(single: Single) = BSONString(single.value)
        def read(str: BSONString) = Single(str.value)
      }

      implicit def optionHandler[T](implicit h: BSONHandler[BSONString, T]): BSONDocumentHandler[Option[T]] = new BSONDocumentReader[Option[T]] with BSONDocumentWriter[Option[T]] with BSONHandler[BSONDocument, Option[T]] {

        def read(doc: BSONDocument): Option[T] =
          doc.getAs[BSONString](f"$$some").map(h.read(_))

        def write(single: Option[T]) = single match {
          case Some(v) => BSONDocument(f"$$some" -> h.write(v))
          case _       => BSONDocument.empty
        }
      }

      def genSeqHandler[T: BSONDocumentHandler]: BSONDocumentHandler[GenSeq[T]] = Macros.handler[GenSeq[T]]

      val seq = GenSeq(Seq(Option.empty[Single], Option(Single("A"))), 1)

      roundtrip(seq, genSeqHandler[Option[Single]])
    }

    "handle overloaded apply correctly" in {
      val doc1 = OverloadedApply("hello")
      val doc2 = OverloadedApply(List("hello", "world"))
      val f = Macros.handler[OverloadedApply]

      roundtrip(doc1, f)
      roundtrip(doc2, f)
    }

    "handle overloaded apply with different number of arguments correctly" in {
      val doc1 = OverloadedApply2("hello", 5)
      val doc2 = OverloadedApply2("hello")
      val f = Macros.handler[OverloadedApply2]

      roundtrip(doc1, f)
      roundtrip(doc2, f)
    }

    "handle overloaded apply with 0 number of arguments correctly" in {
      val doc1 = OverloadedApply3("hello", 5)
      val doc2 = OverloadedApply3()
      val f = Macros.handler[OverloadedApply3]

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
      val format = Macros.handlerOpts[UT, UnionType[UA \/ UB \/ UC \/ UD \/ UF.type] with AutomaticMaterialization]

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
      val format = Macros.handlerOpts[UT, SimpleUnionType[UA \/ UB \/ UC \/ UD] with AutomaticMaterialization]

      format.write(a).getAs[String]("className") must beSome("UA") and {
        format.write(b).getAs[String]("className") must beSome("UB")
      } and roundtrip(a, format) and roundtrip(b, format)
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

      roundtripImp[IntList](Tail) and {
        roundtripImp[IntList](Cons(1, Cons(2, Cons(3, Tail))))
      }
    }

    "automate Union on sealed traits" in {
      import Macros.Options._
      import Union._
      implicit val format = Macros.handlerOpts[UT, AutomaticMaterialization]

      format.write(UA(1)).getAs[String]("className").
        aka("class #1") must beSome("MacroTest.Union.UA") and {
          format.write(UB("buzz")).getAs[String]("className").
            aka("class #2") must beSome("MacroTest.Union.UB")
        } and roundtripImp[UT](UA(17)) and roundtripImp[UT](UB("foo")) and {
          roundtripImp[UT](UC("bar")) and roundtripImp[UT](UD("baz"))
        } and roundtripImp[UT](UF)
    }

    "support automatic implementations search with nested traits" in {
      import Macros.Options._
      import InheritanceModule._
      implicit val format = Macros.handlerOpts[T, AllImplementations with AutomaticMaterialization]

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
      implicit val format = Macros.handlerOpts[UT, SimpleAllImplementations with AutomaticMaterialization]

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

      serialized must beTypedEqualTo(
        BSONDocument("_id" -> doc.myID, "value" -> doc.value)
      ) and {
          format.read(serialized) must_== doc
        }
    }

    "skip ignored fields" >> {
      "with Pair type" in {
        val pairHandler = Macros.handler[Pair]
        val doc = pairHandler.write(Pair(left = "left", right = "right"))

        doc.aka(pretty(doc)) must beTypedEqualTo(
          BSONDocument("right" -> "right")
        )
      }

      "along with Key annotation" in {
        // TODO: Macros.reader or handler (manage Ignore with reader?)
        implicit val handler: BSONDocumentWriter[IgnoredAndKey] =
          Macros.writer[IgnoredAndKey]

        val doc = handler.write(IgnoredAndKey(Person("john", "doe"), "foo"))

        doc.aka(pretty(doc)) must beTypedEqualTo(
          BSONDocument("second" -> "foo")
        )
      }
    }

    "be generated for class class with self reference" in {
      val h = Macros.handler[Bar]
      val bar1 = Bar("bar1", None)
      val doc1 = BSONDocument("name" -> "bar1")

      h.read(doc1) must_== bar1 and {
        h.read(BSONDocument("name" -> "bar2", "next" -> doc1)).
          aka("bar2") must_== Bar("bar2", Some(bar1))
      } and (h.write(bar1) must_== doc1) and {
        h.write(Bar("bar2", Some(bar1))) must_== BSONDocument(
          "name" -> "bar2", "next" -> doc1
        )
      }
    }

    "handle case class with implicits" >> {
      val doc1 = BSONDocument("pos" -> 2, "text" -> "str")
      val doc2 = BSONDocument("ident" -> "id", "value" -> 23.456D)
      val fixture1 = WithImplicit1(2, "str")
      val fixture2 = WithImplicit2("id", 23.456D)

      def readSpec1(r: BSONDocumentReader[WithImplicit1]) =
        r.read(doc1) must_== fixture1

      def writeSpec2(w: BSONDocumentWriter[WithImplicit2[Double]]) =
        w.write(fixture2) must_== doc2

      "to generate reader" in readSpec1(Macros.reader[WithImplicit1])

      "to generate writer with type parameters" in writeSpec2(
        Macros.writer[WithImplicit2[Double]]
      )

      "to generate handler" in {
        val f1 = Macros.handler[WithImplicit1]
        val f2 = Macros.handler[WithImplicit2[Double]]

        readSpec1(f1) and (f1.write(fixture1) must_== doc1) and {
          writeSpec2(f2) and (f2.read(doc2) must_== fixture2)
        }
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

    "be generated for a generic case class" in {
      implicit def singleReader = Macros.reader[Single]
      val r = Macros.reader[Foo[Single]]

      r.read(BSONDocument(
        "bar" -> BSONDocument("value" -> "A"),
        "lorem" -> "ipsum"
      )) must_== Foo(Single("A"), "ipsum")
    }

    "be generated for class class with self reference" in {
      val r = Macros.reader[Bar]
      val bar1 = Bar("bar1", None)
      val doc1 = BSONDocument("name" -> "bar1")

      r.read(doc1) must_== bar1 and {
        r.read(BSONDocument("name" -> "bar2", "next" -> doc1)).
          aka("bar2") must_== Bar("bar2", Some(bar1))
      }
    }
  }

  "Writer" should {
    "be generated for a generic case class" in {
      implicit def singleWriter = Macros.writer[Single]
      val w = Macros.writer[Foo[Single]]

      w.write(Foo(Single("A"), "ipsum")) must_== BSONDocument(
        "bar" -> BSONDocument("value" -> "A"),
        "lorem" -> "ipsum"
      )
    }

    "be generated for class class with self reference" in {
      val w = Macros.writer[Bar]
      val bar1 = Bar("bar1", None)
      val doc1 = BSONDocument("name" -> "bar1")

      w.write(bar1) must_== doc1 and {
        w.write(Bar("bar2", Some(bar1))) must_== BSONDocument(
          "name" -> "bar2", "next" -> doc1
        )
      }
    }
  }

  // ---

  def roundtrip[A](original: A)(implicit reader: BSONReader[BSONDocument, A], writer: BSONWriter[A, BSONDocument]): MatchResult[Any] = {
    def serialized = writer write original
    def deserialized = reader read serialized

    original mustEqual deserialized
  }

  def roundtrip[A](original: A, format: BSONReader[BSONDocument, A] with BSONWriter[A, BSONDocument]): MatchResult[Any] = roundtrip(original)(format, format)

  def roundtripImp[A](data: A)(implicit reader: BSONReader[BSONDocument, A], writer: BSONWriter[A, BSONDocument]) = roundtrip(data)

}
