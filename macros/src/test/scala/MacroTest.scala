import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONHandler,
  BSONObjectID,
  Macros
}
import reactivemongo.bson.Macros.Annotations.{ Flatten, Key, Ignore }

object MacroTest {
  type Handler[A] = BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A]

  case class Person(firstName: String, lastName: String)
  case class Pet(name: String, owner: Person)
  case class Primitives(dbl: Double, str: String, bl: Boolean, int: Int, long: Long)
  case class Optional(name: String, value: Option[String])
  case class Single(value: String)
  case class OptionalSingle(value: Option[String])
  case class SingleTuple(value: (String, String))
  case class User(_id: BSONObjectID = BSONObjectID.generate(), name: String)
  case class WordLover(name: String, words: Seq[String])
  case class Empty()
  object EmptyObject

  case class WithImplicit1(pos: Int, text: String)(implicit x: Numeric[Int])
  case class WithImplicit2[N: Numeric](ident: String, value: N)

  case class RenamedId(
    @Key("_id") myID: BSONObjectID = BSONObjectID.generate(),
    @CustomAnnotation value: String)

  case class Foo[T](bar: T, lorem: String)
  case class Bar(name: String, next: Option[Bar])

  case class GenSeq[A](items: Seq[A], count: Int)

  object Nest {
    case class Nested(name: String)
  }

  case class OverloadedApply(string: String)
  object OverloadedApply {
    def apply(n: Int) = { /* println(n) */ }

    def apply(seq: Seq[String]): OverloadedApply =
      OverloadedApply(seq mkString " ")
  }

  case class OverloadedApply2(string: String, number: Int)
  object OverloadedApply2 {
    def apply(string: String): OverloadedApply2 = OverloadedApply2(string, 0)
  }

  case class OverloadedApply3(string: String, number: Int)
  object OverloadedApply3 {
    def apply(): OverloadedApply3 = OverloadedApply3("", 0)
  }

  object Union {
    sealed trait UT
    case class UA(n: Int) extends UT

    class UB(val s: String) extends UT {
      override def equals(that: Any): Boolean = that match {
        case other: UB => s == other.s
        case _         => false
      }

      override def hashCode: Int = s.hashCode
    }

    object UB {
      def apply(s: String): UB = new UB(s)
      def unapply(ub: UB): Option[String] = Some(ub.s)
      implicit val handler: Handler[UB] = Macros.handler[UB]
    }

    case class UC(s: String) extends UT
    case class UD(s: String) extends UT
    object UE extends UT
    case object UF extends UT

    case object DoNotExtendsA
    object DoNotExtendsB
  }

  trait NestModule {
    case class Nested(name: String)
    val format: Handler[Nested] = Macros.handler[Nested]
  }

  object TreeModule {
    /*
     Due to compiler limitations (read: only workaround I found),
     handlers must be defined here and explicit type annotations added
     to enable compiler to use implicit handlers recursively.
     */

    sealed trait Tree

    case class Node(left: Tree, right: Tree) extends Tree
    implicit val nodeHandler: Handler[Node] = Macros.handler[Node]

    case class Leaf(data: String) extends Tree
    implicit val leafHandler: Handler[Leaf] = Macros.handler[Leaf]

    object Tree {
      import Macros.Options._
      implicit val bson: Handler[Tree] =
        Macros.handlerOpts[Tree, UnionType[Node \/ Leaf]]
    }
  }

  object TreeCustom {
    sealed trait Tree

    case class Node(left: Tree, right: Tree) extends Tree
    implicit val nodeHandler: Handler[Node] = Macros.handler[Node]

    case class Leaf(data: String) extends Tree

    object Leaf {
      private val helper: Handler[Leaf] = Macros.handler[Leaf]
      implicit val bson: Handler[Leaf] = new BSONDocumentReader[Leaf] with BSONDocumentWriter[Leaf] with BSONHandler[BSONDocument, Leaf] {
        def write(t: Leaf): BSONDocument = helper.write(Leaf("hai"))
        def read(bson: BSONDocument): Leaf = helper read bson
      }
    }

    object Tree {
      import Macros.Options._

      implicit val bson: Handler[Tree] =
        Macros.handlerOpts[Tree, UnionType[Node \/ Leaf]]
      //Macros.handlerOpts[Tree, UnionType[Node \/ Leaf] with Verbose]
    }
  }

  object IntListModule {
    sealed trait IntList

    case class Cons(head: Int, tail: IntList) extends IntList
    implicit val consHandler: Handler[Cons] = Macros.handler[Cons]

    case object Tail extends IntList
    implicit val tailHandler: Handler[Tail.type] = Macros.handler[Tail.type]

    object IntList {
      import Macros.Options.{ UnionType, \/ }

      implicit val bson: Handler[IntList] =
        Macros.handlerOpts[IntList, UnionType[Cons \/ Tail.type]]
    }
  }

  object InheritanceModule {
    sealed trait T

    case class A() extends T
    implicit val ah: Handler[A] = Macros.handler[A]

    case object B extends T
    implicit val bh: Handler[B.type] = Macros.handler[B.type]

    sealed trait TT extends T
    case class C() extends TT
    implicit val ch: Handler[C] = Macros.handler[C]
  }

  case class Pair(@Ignore left: String, right: String)

  case class IgnoredAndKey(
    @Ignore a: Person,
    @Key("second") b: String)

  case class Range(start: Int, end: Int)

  object Range {
    implicit val handler = Macros.handler[Range]
  }

  // Flatten
  case class LabelledRange(
    name: String,
    @Flatten range: Range)

  case class InvalidRecursive(
    property: String,
    @Flatten parent: InvalidRecursive)

  case class InvalidNonDoc(@Flatten name: String)
}
