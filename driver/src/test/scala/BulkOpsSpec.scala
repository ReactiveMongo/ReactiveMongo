package reactivemongo

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import reactivemongo.api.bson.{ BSONArray, BSONDocument }
import reactivemongo.api.collections.BulkOps._

import org.specs2.concurrent.ExecutionEnv

final class BulkOpsSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Bulk operations".title

  val doc1 = BSONDocument("foo" -> 1)
  val doc2 = BSONDocument("bar" -> "lorem ipsum", "int" -> 2)

  val bsonSize1 = BSONArray(doc1, doc1).byteSize - BSONArray.empty.byteSize

  def producer1 = bulks[BSONDocument](
    documents = Seq.empty,
    maxBsonSize = bsonSize1,
    maxBulkSize = 2)(_.byteSize)

  val bsonSize2 = BSONArray(doc2, doc2).byteSize - BSONArray.empty.byteSize

  val producer2Docs = Seq(doc1, doc1, doc2, doc1, doc2)
  def producer2 = bulks[BSONDocument](
    documents = producer2Docs,
    maxBsonSize = bsonSize2,
    maxBulkSize = 2)(_.byteSize)

  implicit val docOrdering: math.Ordering[BSONDocument] =
    math.Ordering.by[BSONDocument, Int](_.hashCode)

  // ---

  section("unit")
  "Preparation" should {
    "produce 1 single empty stage" in {
      producer1 must beLike[BulkProducer[BSONDocument]] {
        case prod1 => prod1() must beRight.like {
          case BulkStage(bulk, None) => bulk must beEmpty
        }
      }
    }

    s"produce 1 bulk [#1, #1] then fail as #2(${doc2.byteSize}) > ${bsonSize1}" in {
      bulks[BSONDocument](
        documents = Seq(doc1, doc1, doc2, doc1),
        maxBsonSize = bsonSize1,
        maxBulkSize = 2)(_.byteSize).
        aka("bulk producer") must beLike[BulkProducer[BSONDocument]] {
          case prod1 => prod1() must beRight.like {
            case BulkStage(bulk1, Some(prod2)) =>
              bulk1.toList must_== List(doc1, doc1) and {
                prod2() must beLeft(
                  s"size of document #2 exceed the maxBsonSize: ${doc2.byteSize} + 3 > ${bsonSize1}")
              }
          }
        }
    }

    s"produce 5 bulks [#1, #1, #2, #1, #2]" in {
      producer2 must beLike[BulkProducer[BSONDocument]] {
        case prod1 => prod1() must beRight.like {
          case BulkStage(bulk1, Some(prod2)) =>
            bulk1.toList must_== List(doc1, doc1) and {
              prod2() must beRight.like {
                case BulkStage(bulk2, Some(prod3)) =>
                  bulk2.toList must_== List(doc2, doc1) and {
                    prod3() must beRight.like {
                      case BulkStage(bulk3, None) =>
                        bulk3.toList must_== List(doc2)
                    }
                  }
              }
            }
        }
      }
    }

    "take minimal size into account into total size" in {
      bulks[BSONDocument](
        documents = producer2Docs,
        maxBsonSize = doc1.byteSize,
        maxBulkSize = 2)(_.byteSize) must beLike[BulkProducer[BSONDocument]] {
        case prod1 =>
          prod1() must beLeft(s"size of document #0 exceed the maxBsonSize: ${doc1.byteSize} + 3 > ${doc1.byteSize}")
      }
    }

    "take into account the fact that keys increase in size in a larger array" in {
      val ExpectedFirstBulk = List.fill(10)(doc1)
      val ExpectedSecondBulk = List(doc1)

      bulks[BSONDocument](
        documents = Seq.fill(11)(doc1),
        maxBsonSize = (doc1.byteSize + 1 + 2 /*docElementByteOverhead*/ ) * 11,
        maxBulkSize = 11)(_.byteSize) must beLike[BulkProducer[BSONDocument]] {
          case prod1 =>
            prod1() must beRight.like {
              case BulkStage(ExpectedFirstBulk, Some(prod2)) =>
                prod2() must beRight(BulkStage(ExpectedSecondBulk, None))
            }
        }
    }

  }

  "Application" should {
    "be successfully ordered" >> {
      val app = bulkApply[BSONDocument, Iterable[BSONDocument]](
        _: BulkProducer[BSONDocument])(Future.successful(_), None)(
          _: ExecutionContext)

      "for producer1" in {
        app(producer1, ee.ec) must beEqualTo(Nil).await
      }

      "for producer2" in {
        app(producer2, ee.ec) must beEqualTo(Seq(
          Seq(doc1, doc1), Seq(doc2, doc1), Seq(doc2))).await
      }
    }

    val recoverWithEmpty =
      Option[Exception => Future[Iterable[BSONDocument]]] { _ =>
        Future.successful(Seq.empty[BSONDocument])
      }

    "be successfully unordered" >> {
      val app = bulkApply[BSONDocument, Iterable[BSONDocument]](
        _: BulkProducer[BSONDocument])(Future.successful(_), recoverWithEmpty)(
          _: ExecutionContext)

      "for producer1" in {
        app(producer1, ee.ec) must beEqualTo(Nil).await
      }

      "for producer2" in {
        app(producer2, ee.ec).map(
          _.flatten.sorted) must beEqualTo(producer2Docs.sorted).await
      }
    }

    // Function failing each two bulks
    val foo: Iterable[BSONDocument] => Future[Iterable[BSONDocument]] = {
      @volatile var i = 0

      { bulk =>
        i += 1

        if (i % 2 == 0) {
          Future.failed[Iterable[BSONDocument]](new Exception("Foo"))
        } else {
          Future.successful(bulk)
        }
      }
    }

    "be failed on the first failure for producer2" in eventually(2, 1.seconds) {
      val app = bulkApply[BSONDocument, Iterable[BSONDocument]](
        _: BulkProducer[BSONDocument])(foo, None)(_: ExecutionContext)

      app(producer2, ee.ec) must throwA[Exception]("Foo").await
    }

    "be successfully unordered even with failures" >> {
      val app = bulkApply[BSONDocument, Iterable[BSONDocument]](
        _: BulkProducer[BSONDocument])(foo, recoverWithEmpty)(
          _: ExecutionContext)

      "for producer1" in {
        app(producer1, ee.ec) must beEqualTo(Nil).await
      }

      "for producer2" in {
        app(producer2, ee.ec).map(
          _.flatten.size) must beLessThan(producer2Docs.size).await
      }
    }
  }
  section("unit")
}
