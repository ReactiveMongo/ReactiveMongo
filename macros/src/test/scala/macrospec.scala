import reactivemongo.bson._
import org.specs2.mutable._

class Macros extends Specification {
  type Handler[A] = BSONDocumentReader[A] with BSONDocumentWriter[A]  with BSONHandler[BSONDocument, A]

  def roundtrip[A](original: A, format: BSONReader[BSONDocument, A] with BSONWriter[A, BSONDocument]) = {
    val serialized = format write original
    val deserialized = format read serialized
    original mustEqual deserialized
  }

  case class Person(firstName: String, lastName: String)
  case class Pet(name: String, owner: Person)
  case class Primitives(dbl: Double, str: String, bl: Boolean, int: Int, long: Long)
  case class Optional(name: String, value: Option[String])
  case class Single(value: String)
  case class OptionalSingle(value: Option[String])
  case class SingleTuple(value: (String, String))
  case class User(_id: BSONObjectID = BSONObjectID.generate, name: String)
  case class WordLover(name: String, words: Seq[String])

  object Nest {
    case class Nested(name: String)
  }

  case class OverloadedApply(string: String)
  object OverloadedApply {
    def apply(n: Int) {
      println(n)
    }

    def apply(seq: Seq[String]): OverloadedApply = OverloadedApply(seq mkString " ")
  }

  object Union {
    sealed trait UT
    case class UA(n: Int) extends UT
    case class UB(s: String) extends UT
    case class UC(s: String) extends UT
    case class UD(s: String) extends UT
  }

  trait NestModule {
    case class Nested(name: String)
    val format = Macros.handler[Nested]
  }

  object TreeModule {
    //due to compiler limitations(read: only workaround I found), handlers must be defined here
    //and explicit type annotations added to enable compiler to use implicit handlers recursively

    sealed trait Tree
    case class Node(left: Tree, right: Tree) extends Tree
    case class Leaf(data: String) extends Tree

    object Tree {
      import Macros.Options._
      implicit val bson: Handler[Tree] = Macros.handlerOpts[Tree, UnionType[Node \/ Leaf]]
    }
  }

  object TreeCustom{
    sealed trait Tree
    case class Node(left: Tree, right: Tree) extends Tree
    case class Leaf(data: String) extends Tree

    object Leaf {
      private val helper = Macros.handler[Leaf]
      implicit val bson: Handler[Leaf] = new BSONDocumentReader[Leaf] with BSONDocumentWriter[Leaf] with BSONHandler[BSONDocument, Leaf] {
        def write(t: Leaf): BSONDocument = helper.write(Leaf("hai"))
        def read(bson: BSONDocument): Leaf = helper read bson
      }
    }

    object Tree {
      import Macros.Options._
      implicit val bson: Handler[Tree] = Macros.handlerOpts[Tree, UnionType[Node \/ Leaf] with Verbose]
    }
  }

  "Formatter" should {
    "handle primitives" in {
      roundtrip(
        Primitives(1.2, "hai", true, 42, Long.MaxValue),
        Macros.handler[Primitives])
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
        Macros.handler[Single])
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
      doc.getAs[String]("className") mustEqual Some("Macros.Person")
      roundtrip(person, format)
    }

    "handle union types(ADT)" in {
      import Union._
      import Macros.Options._
      val a = UA(1)
      val b = UB("hai")
      val format = Macros.handlerOpts[UT, UnionType[UA \/ UB \/ UC \/ UD]]
      println(BSONDocument pretty (format write a))
      println(BSONDocument pretty (format write b))
      roundtrip(a, format)
      roundtrip(b, format)
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
      val expected = Node(Leaf("hai"), Node(Leaf("hai"),Leaf("hai")))
      assert( deserialized === expected )
    }
  }
}