package reactivemongo

import reactivemongo.bson.{
  BSONDocument => LegacyDocument,
  BSONDocumentHandler => LegacyDocHandler,
  BSONDocumentReader => LegacyDocReader,
  BSONDocumentWriter => LegacyDocWriter,
  BSONDouble => LegacyDouble,
  BSONHandler => LegacyHandler,
  BSONInteger => LegacyInteger,
  BSONReader => LegacyReader,
  BSONValue => LegacyValue,
  BSONWriter => LegacyWriter
}

import reactivemongo.api.bson.{
  BSONDocument,
  BSONDocumentHandler,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONDouble,
  BSONHandler,
  BSONInteger,
  BSONLong,
  BSONReader,
  BSONWriter
}

import org.specs2.specification.core.Fragment

final class HandlerConverterSpec
  extends org.specs2.mutable.Specification with ConverterFixtures {

  "Handler converters" title

  import reactivemongo.api.bson.compat._

  "Converters" should {
    Fragment.foreach(fixtures) {
      case (legacy, bson) =>
        "from legacy" >> {
          s"convert reader for $legacy" in {
            implicit val lr = LegacyReader[LegacyValue, Long] { _ => 1L }
            def br: BSONReader[Long] = lr

            toReader(lr).readTry(BSONLong(2L)) must beSuccessfulTry(1L) and {
              br.readTry(BSONLong(3L)) must beSuccessfulTry(1L)
            }
          }

          s"convert writer for $legacy" in {
            implicit val lw = LegacyWriter[Unit, LegacyValue] { _ => legacy }
            def bw: BSONWriter[Unit] = lw

            toWriter(lw).writeTry({}) must beSuccessfulTry(bson) and {
              bw.writeTry({}) must beSuccessfulTry(bson)
            }
          }

          s"convert handler for $legacy" in {
            object Foo

            implicit val lh = LegacyHandler[LegacyInteger, Foo.type](
              _ => Foo,
              _ => LegacyInteger(1))

            def bh: BSONHandler[Foo.type] = lh

            bh.writeTry(Foo) must beSuccessfulTry(BSONInteger(1)) and {
              bh.readTry(BSONInteger(2)) must beSuccessfulTry(Foo)
            }
          }
        }

        "to legacy" >> {
          s"convert reader to $bson" in {
            implicit val br = BSONReader[Unit] { _ => () }
            def lr: LegacyReader[LegacyValue, Unit] = br

            fromReader(br).read(LegacyInteger(1)) must_=== ({}) and {
              lr.read(LegacyInteger(2)) must_=== ({})
            }
          }

          s"convert writer to $bson" in {
            implicit val bw = BSONWriter[Int] { _ => bson }
            def lw: LegacyWriter[Int, LegacyValue] = bw

            fromWriter(bw).write(1) must_=== legacy and {
              lw.write(2) must_=== legacy
            }
          }

          s"convert handler to $bson" in {
            object Bar

            implicit val bh = BSONHandler[Bar.type](
              _ => Bar,
              _ => BSONDouble(1.2D))

            def lh: LegacyHandler[LegacyValue, Bar.type] = bh

            lh.write(Bar) must_=== LegacyDouble(1.2D) and {
              lh.read(LegacyDouble(3.4D)) must_=== Bar
            }
          }
        }
    }

    "from legacy document" >> {
      "in writer" in {
        val doc = BSONDocument("ok" -> 1)
        implicit val lw = LegacyDocWriter[Double] { _ =>
          LegacyDocument("ok" -> 1)
        }
        def bw1: BSONDocumentWriter[Double] = lw
        def bw2 = implicitly[BSONDocumentWriter[Double]]

        toWriter(lw).writeTry(1.0D) must beSuccessfulTry(doc) and {
          bw1.writeTry(1.1D) must beSuccessfulTry(doc)
        } and {
          bw2.writeTry(1.2D) must beSuccessfulTry(doc)
        }
      }

      "in reader" in {
        implicit val lr: LegacyDocReader[Float] = LegacyDocReader[Float](_ => 1.2F)
        def br1: BSONDocumentReader[Float] = lr
        def br2 = implicitly[BSONDocumentReader[Float]]

        toDocumentReader(lr).readTry(
          BSONDocument("ok" -> 1)) must beSuccessfulTry(1.2F) and {
            br1.readTry(BSONDocument("ok" -> 2)) must beSuccessfulTry(1.2F)
          } and {
            br2.readTry(BSONDocument("ok" -> 3)) must beSuccessfulTry(1.2F)
          }
      }

      "in handler" in {
        implicit val lh = LegacyDocHandler[None.type](
          _ => None,
          _ => LegacyDocument.empty)

        val bh: BSONDocumentHandler[None.type] = lh

        bh.readTry(BSONDocument("ok" -> 1)) must beSuccessfulTry(None) and {
          bh.writeTry(None) must beSuccessfulTry(BSONDocument.empty)
        }
      }
    }

    "to legacy document" >> {
      "in writer" in {
        val doc = LegacyDocument("ok" -> 2)
        implicit val bw = BSONDocumentWriter[Int](_ => BSONDocument("ok" -> 2))
        def lw: LegacyDocWriter[Int] = bw

        fromWriter(bw).write(1) must_=== doc and {
          lw.write(2) must_=== doc
        }
      }

      "in reader" in {
        implicit val br = BSONDocumentReader[None.type](_ => None)
        def lr: LegacyDocReader[None.type] = br

        fromDocumentReader(br).read(LegacyDocument("ok" -> 1)) must beNone and {
          lr.read(LegacyDocument("ok" -> 2)) must beNone
        }
      }

      "in handler" in {
        implicit val bh = BSONDocumentHandler[Unit](
          _ => (),
          _ => BSONDocument("foo" -> 1L))

        val lh: LegacyDocHandler[Unit] = bh

        lh.read(LegacyDocument("ok" -> 1)) must_=== ({}) and {
          lh.write({}) must_=== LegacyDocument("foo" -> 1L)
        }
      }
    }
  }
}
