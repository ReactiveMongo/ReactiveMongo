import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{
  Cursor,
  CursorFlattener,
  CursorProducer,
  WrappedCursor
}

import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection

trait Cursor1Spec { spec: CursorSpec =>
  def group1 = {
    val nDocs = 16517

    s"insert $nDocs records" in {
      def insert(rem: Int, bulks: Seq[Future[Unit]]): Future[Unit] = {
        if (rem == 0) {
          Future.sequence(bulks).map(_ => {})
        } else {
          val len = if (rem < 256) rem else 256
          def prepared = nDocs - rem

          def bulk = coll.insert(false).many(
            for (i <- 0 until len) yield {
              val n = i + prepared
              BSONDocument("i" -> n, "record" -> s"record$n")
            }).map(_ => {})

          insert(rem - len, bulk +: bulks)
        }
      }

      insert(nDocs, Seq.empty).map { _ =>
        info(s"inserted $nDocs records")
      } aka "fixtures" must beTypedEqualTo({}).await(1, timeout)
    }

    "request for cursor query" in {
      import reactivemongo.api.tests.{
        makeRequest => req,
        nextResponse,
        Response
      }

      def cursor(batchSize: Int = 0) =
        coll.find(matchAll("makeReq1")).batchSize(batchSize).cursor()

      req(cursor(nDocs + 1), nDocs + 1) must beLike[Response] {
        case resp =>
          val r = resp.reply

          r.cursorID aka "cursor ID #1" must_=== 0 and {
            r.startingFrom must_=== 0
          } and {
            r.numberReturned aka "returned" must_=== nDocs
          }
      }.await(1, timeout) and {
        req(cursor(nDocs), 1) must beLike[Response] {
          case resp =>
            val r = resp.reply

            r.cursorID aka "cursor ID #2" must_=== 0 and {
              r.startingFrom must_=== 0
            } and {
              r.numberReturned must_=== 1
            }
        }.await(1, timeout)
      } and {
        req(cursor(128), Int.MaxValue) must beLike[Response] {
          case resp =>
            val r = resp.reply

            r.cursorID aka "cursor ID #3" must not(beEqualTo(0)) and {
              r.startingFrom must_=== 0
            } and {
              r.numberReturned must_=== 128
            }
        }.await(1, timeout)
      } and {
        req(cursor(), 10) must beLike[Response] {
          case resp =>
            val r = resp.reply

            r.cursorID aka "cursor ID #4" must_=== 0 and {
              r.startingFrom must_=== 0
            } and {
              r.numberReturned must_=== 10
            }
        }.await(1, timeout)
      } and {
        req(cursor(), 101) must beLike[Response] {
          case resp =>
            val r = resp.reply

            r.cursorID aka "cursor ID #5" must_== 0 and {
              r.startingFrom must_=== 0
            } and {
              r.numberReturned must_=== 101 /* default batch size */
            }
        }.await(1, timeout)
      } and {
        req(cursor(), Int.MaxValue /* unlimited */ ) must beLike[Response] {
          case resp =>
            val r = resp.reply

            r.cursorID aka "cursor ID #6" must not(beEqualTo(0)) and {
              r.startingFrom must_=== 0
            } and {
              r.numberReturned must_=== 101 /* default batch size */
            }
        }.await(1, timeout)
      } and {
        val batchSize = 128
        val max = (batchSize * 2) - 1
        val cur = cursor(batchSize)
        @volatile var r1: Response = null // Workaround to avoid nesting .await

        req(cur, max) must beLike[Response] {
          case r =>
            val rp1 = r.reply
            r1 = r

            rp1.cursorID aka "cursor ID #7a" must not(beEqualTo(0)) and {
              rp1.startingFrom must_=== 0
            } and {
              rp1.numberReturned must_=== batchSize
            }
        }.await(1, timeout) and {
          nextResponse(cur, max)(ee.ec, r1) must beSome[Response].like {
            case r2 =>
              val rp2 = r2.reply

              rp2.cursorID aka "cursor ID #7b" must_=== 0 and {
                rp2.startingFrom aka "from #7b" must_=== 128
              } and {
                rp2.numberReturned must_=== (batchSize - 1)
              } and {
                nextResponse(cur, 1)(ee.ec, r2) must beNone.await(1, timeout)
              }
          }.await(1, timeout)
        }
      }
    }

    { // headOption
      def headOptionSpec(c: => BSONCollection, timeout: FiniteDuration) = {
        "find first document when matching" in {
          c.find(matchAll("headOption1")).cursor().
            headOption must beSome[BSONDocument].await(1, timeout)
        }

        "find first document when not matching" in {
          c.find(BSONDocument("i" -> -1)).cursor().
            headOption must beNone.await(1, timeout)
        }
      }

      "with the default connection" >> {
        headOptionSpec(coll, timeout)
      }

      "with the slow connection" >> {
        headOptionSpec(slowColl, slowTimeout)
      }
    }

    "peek operation" in {
      import reactivemongo.api.tests.{ decoder, reader => docReader, pack }
      implicit val reader = docReader[Int] { decoder.int(_, "i").get }

      def cursor = coll.find(matchAll("foo")).
        sort(BSONDocument("i" -> 1)).batchSize(2).cursor[Int]()

      val cn = s"${db.name}.${coll.name}"

      cursor.headOption must beSome(0).await(1, timeout) and {
        cursor.peek[List](100) must beLike[Cursor.Result[List[Int]]] {
          case Cursor.Result(0 :: 1 :: Nil, ref1) => // batchSize(2) ~> 0,1
            ref1.collectionName must_=== cn and {
              def getMore = db.getMore(pack, ref1)

              getMore.peek[Seq](3) must beLike[Cursor.Result[Seq[Int]]] {
                case Cursor.Result(2 +: 3 +: 4 +: Nil, ref2) =>
                  ref2.cursorId must_=== ref1.cursorId and {
                    ref2.collectionName must_=== cn
                  } and {
                    db.getMore(pack, ref2).
                      peek[Seq](2) must beLike[Cursor.Result[Seq[Int]]] {
                        case Cursor.Result(5 +: 6 +: Nil, ref3) =>
                          ref3.cursorId must_=== ref1.cursorId and {
                            ref3.collectionName must_=== cn
                          }
                      }.await(1, timeout)
                  }
              }.await(1, timeout) and {
                getMore.peek[Set](1) must beLike[Cursor.Result[Set[Int]]] {
                  case Cursor.Result(s, ref4) =>
                    s must_=== Set(7) and {
                      ref4.cursorId must_=== ref1.cursorId
                    } and {
                      ref4.collectionName must_=== cn
                    }
                }.await(1, timeout)
              }
            }
        }.await(1, timeout)
      }
    }

    // head
    "find first document when matching" in {
      coll.find(matchAll("head1") ++ ("i" -> 0)).cursor[BSONDocument]().head.
        map(_ -- "_id") must beTypedEqualTo(BSONDocument(
          "i" -> 0, "record" -> "record0")).await(1, timeout)
    }

    "find first document when not matching" in {
      Await.result(
        coll.find(BSONDocument("i" -> -1)).cursor().head,
        timeout) must throwA[Cursor.NoSuchResultException.type]
    }

    "read one optional document with success" in {
      coll.find(matchAll("one1")).one[BSONDocument].
        aka("findOne") must beSome[BSONDocument].await(0, timeout)
    }

    "read one document with success" in {
      coll.find(matchAll("one2") ++ ("i" -> 1)).requireOne[BSONDocument].
        map(_ -- "_id") must beTypedEqualTo(BSONDocument(
          "i" -> 1, "record" -> "record1")).await(0, timeout)
    }

    "collect with limited maxDocs" >> {
      val max = (nDocs / 8).toInt

      def spec(cl: => Future[BSONCollection]) =
        cl.flatMap { c =>
          c.find(matchAll("collectLimit")).batchSize(997).cursor().
            collect[List](max, Cursor.FailOnError[List[BSONDocument]]())
        } must haveSize[List[BSONDocument]](max).await(1, timeout)

      "without session" in {
        spec(Future successful coll)
      }

      "with session" in {
        spec(db.startSession().map(_.collection(coll.name)))
      }
    }

    { // Fold
      def foldSpec1(c: => BSONCollection, timeout: FiniteDuration) = {
        "get 10 first docs" in {
          c.find(matchAll("cursorspec1")).cursor().
            collect[List](10, Cursor.FailOnError[List[BSONDocument]]()).
            map(_.size) must beTypedEqualTo(10).await(1, timeout)
        }

        { // .fold
          "fold all the documents" in eventually(2, timeout / 2L) {
            c.find(matchAll("cursorspec2a")).batchSize(2096).cursor().fold(0)(
              { (st, _) => debug(s"fold: $st"); st + 1 }).
              aka("result size") must beTypedEqualTo(16517).
              await(1, timeout) and {
                c.find(matchAll("cursorspec2b")).
                  batchSize(2096).cursor().fold(0, -1)(
                    { (st, _) => st + 1 }) must beTypedEqualTo(16517).
                    await(0, timeout * 2L)
              }
          }

          "fold only 1024 documents" in eventually(2, timeout / 2L) {
            c.find(matchAll("cursorspec3")).batchSize(256).
              cursor().fold(0, 1024)((st, _) => st + 1).
              aka("result size") must beTypedEqualTo(1024).
              await(0, timeout)
          }
        }

        { // .foldWhile
          "fold while all the documents" in {
            c.find(matchAll("cursorspec4a")).
              batchSize(2096).cursor().foldWhile(0)(
                { (st, _) => debug(s"foldWhile: $st"); Cursor.Cont(st + 1) }).
                aka("result size") must beTypedEqualTo(16517).
                await(1, timeout + (timeout / 2L))
          }

          "fold while only 1024 documents" in {
            c.find(matchAll("cursorspec5a")).batchSize(256).
              cursor().foldWhile(0, 1024)(
                (st, _) => Cursor.Cont(st + 1)).
                aka("result size") must beTypedEqualTo(1024).await(1, timeout)
          }

          "fold while successfully with async function" >> {
            "all the documents" in {
              coll.find(matchAll("cursorspec4b")).
                batchSize(2096).cursor().foldWhileM(0)(
                  (st, _) => Future.successful(Cursor.Cont(st + 1))).
                  aka("result size") must beTypedEqualTo(16517).await(1, timeout)
            }

            "only 1024 documents" in {
              coll.find(matchAll("cursorspec5b")).
                batchSize(256).cursor().foldWhileM(0, 1024)(
                  (st, _) => Future.successful(Cursor.Cont(st + 1))).
                  aka("result size") must beTypedEqualTo(1024).await(1, timeout)
            }
          }
        }

        { // .foldBulk
          "fold the bulks for all the documents" in {
            c.find(matchAll("cursorspec6a")).
              batchSize(2096).cursor().foldBulks(0)({ (st, bulk) =>
                debug(s"foldBulk: $st")
                Cursor.Cont(st + bulk.size)
              }) aka "result size" must beTypedEqualTo(16517).await(1, timeout)
          }

          "fold the bulks for 1024 documents" in {
            c.find(matchAll("cursorspec7a")).
              batchSize(256).cursor().foldBulks(0, 1024)(
                (st, bulk) => Cursor.Cont(st + bulk.size)).
                aka("result size") must beTypedEqualTo(1024).await(1, timeout)
          }

          "fold the bulks with async function" >> {
            "for all the documents" in {
              coll.find(matchAll("cursorspec6b")).
                batchSize(2096).cursor().foldBulksM(0)(
                  (st, bulk) => Future.successful(Cursor.Cont(st + bulk.size))).
                  aka("result size") must beTypedEqualTo(16517).
                  await(1, timeout)
            }

            "for 1024 documents" in {
              coll.find(matchAll("cursorspec7b")).
                batchSize(256).cursor().foldBulksM(0, 1024)(
                  (st, bulk) => Future.successful(Cursor.Cont(st + bulk.size))).
                  aka("result size") must beTypedEqualTo(1024).await(1, timeout)
            }
          }
        }
      }

      "with the default connection" >> {
        foldSpec1(coll, timeout)
      }

      "with the slow connection" >> {
        foldSpec1(slowColl, slowTimeout * 2L)
      }
    }

    "produce a custom cursor for the results" in {
      implicit def fooProducer[T] = new CursorProducer[T] {
        type ProducedCursor = FooCursor[T]

        def produce(base: Cursor.WithOps[T]): ProducedCursor =
          new DefaultFooCursor(base)
      }

      implicit object fooFlattener extends CursorFlattener[FooCursor] {
        type Flattened[T] = FooCursor[T]

        def flatten[T](future: Future[FooCursor[T]]) =
          new FlattenedFooCursor(future)
      }

      val cursor = coll.find(matchAll("cursorspec10")).cursor[BSONDocument]()

      cursor.foo must_=== "Bar" and {
        Cursor.flatten(Future.successful(cursor)).foo must_=== "raB"
      } and {
        val extCursor: FooExtCursor[BSONDocument] = new DefaultFooCursor(cursor)

        // Check resolution as super type (FooExtCursor <: FooCursor)
        val flattened = Cursor.flatten[BSONDocument, FooCursor](
          Future.successful[FooExtCursor[BSONDocument]](extCursor))

        flattened must beAnInstanceOf[FooCursor[BSONDocument]] and {
          flattened must not(beAnInstanceOf[FooExtCursor[BSONDocument]])
        } and {
          flattened.foo must_=== "raB"
        }
      }
    }
  }

  // ---

  private sealed trait FooCursor[T] extends Cursor[T] { def foo: String }

  private sealed trait FooExtCursor[T] extends FooCursor[T]

  private class DefaultFooCursor[T](val wrappee: Cursor[T])
    extends FooExtCursor[T] with WrappedCursor[T] {
    val foo = "Bar"
  }

  private class FlattenedFooCursor[T](cursor: Future[FooCursor[T]])
    extends reactivemongo.api.FlattenedCursor[T](cursor) with FooCursor[T] {

    val foo = "raB"
  }
}
