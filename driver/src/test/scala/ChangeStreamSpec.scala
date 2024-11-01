import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{ ChangeStreams, Cursor }
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection

import org.specs2.concurrent.ExecutionEnv
import org.specs2.execute.AsResult
import org.specs2.matcher.Matcher

import util.{ MongoSkips, WithTemporaryDb }
import util.BsonMatchers._
import util.WithTemporaryCollection._

final class ChangeStreamSpec(
    implicit
    val ee: ExecutionEnv)
    extends org.specs2.mutable.Specification
    with WithTemporaryDb
    with MongoSkips {

  "Change stream".title

  import tests.Common.timeout

  "Change stream of a collection" should {
    "have an empty cursor when a new cursor is opened without resuming" in {
      skipIfNotRSAndNotVersionAtLeast(MongoWireVersion.V36) {
        withTmpCollection(db) { (coll: BSONCollection) =>
          // given
          val cursor = coll.watch[BSONDocument]().cursor[Cursor.WithOps]
          val testDocument = BSONDocument("_id" -> "test", "foo" -> "bar")

          // when
          val results = for {
            resultBefore <- cursor.headOption
            _ <- coll.insert.one(testDocument)
            resultAfter <- cursor.headOption
          } yield (resultBefore, resultAfter)

          // then
          results must beLike[(Option[BSONDocument], Option[BSONDocument])] {
            case (resultBefore, resultAfter) =>
              (resultBefore must beNone) and (resultAfter must beNone)
          }.await(2, 3L * timeout)
        }
      }
    }

    "return the next change event when a new cursor is folded upon" in {
      skipIfNotRSAndNotVersionAtLeast(MongoWireVersion.V36) {
        withTmpCollection(db) { (coll: BSONCollection) =>
          // given
          val cursor = coll.watch[BSONDocument]().cursor[Cursor.WithOps]
          val testDocument = BSONDocument("_id" -> "test", "foo" -> "bar")

          // when
          val result = foldOne(cursor)
          // The cursor needs to have been opened by the time
          // the insert operation is executed.
          //
          // We have no means to guarantee that with the Cursor API,
          // so the following is a best-effort solution,
          // but a race-condition is still possible and may lead
          // to a false negative test result.
          val forkedInsertion = delayBy(500.millis) {
            coll.insert(ordered = false).one(testDocument)
          }

          // then
          (forkedInsertion.map(_ => {}) must haveCompleted) and (result must {
            {
              haveField[String]("operationType").that(beTypedEqualTo("insert"))
            } and {
              haveField[BSONDocument]("documentKey").that {
                haveField[String]("_id").that(beTypedEqualTo("test"))
              }
            } and {
              haveField[BSONDocument]("fullDocument").that(beTypedEqualTo(
                testDocument
              ))
            }
          }.await(2, timeout))
        }
      }
    }

    "resume with the next event after a known id" in {
      skipIfNotRSAndNotVersionAtLeast(MongoWireVersion.V36) {
        withTmpCollection(db) { (coll: BSONCollection) =>
          import coll.AggregationFramework.ChangeStream.ResumeAfter

          // given
          val initialCursor = coll.watch[BSONDocument]().cursor[Cursor.WithOps]
          val testDocument1 =
            BSONDocument("_id" -> "resume_test1", "foo" -> "bar")
          val testDocument2 =
            BSONDocument("_id" -> "resume_test2", "foo" -> "baz")

          // when
          val result = foldOne(initialCursor).flatMap { firstEvent =>
            firstEvent.get("_id") match {
              case Some(eventId) =>
                coll
                  .watch(offset = Some(ResumeAfter(eventId)))
                  .cursor[Cursor.WithOps]
                  .head

              case _ => Future.failed(new Exception("The event had no id"))
            }
          }

          // See comment above
          val forkedInsertion = delayBy(500.millis) {
            coll.insert(ordered = false).many(Seq(testDocument1, testDocument2))
          }

          // then
          (forkedInsertion.map(_ => {}) must haveCompleted) and (result must {
            {
              haveField[String]("operationType").that(beTypedEqualTo("insert"))
            } and {
              haveField[BSONDocument]("documentKey").that {
                haveField[String]("_id").that(beTypedEqualTo("resume_test2"))
              }
            } and {
              haveField[BSONDocument]("fullDocument").that(beTypedEqualTo(
                testDocument2
              ))
            }
          }.await(2, timeout))
        }
      }
    }

    "resume with the same event after a known operation time" in {
      skipIfNotRSAndNotVersionAtLeast(MongoWireVersion.V40) {
        withTmpCollection(db) { (coll: BSONCollection) =>
          import coll.AggregationFramework.ChangeStream.StartAt

          // given
          val initialCursor = coll.watch[BSONDocument]().cursor[Cursor.WithOps]
          val testDocument1 =
            BSONDocument("_id" -> "clusterTime_test1", "foo" -> "bar")
          val testDocument2 =
            BSONDocument("_id" -> "clusterTime_test2", "foo" -> "baz")

          // when
          val result = foldOne(initialCursor).flatMap { firstEvent =>
            firstEvent.long("clusterTime") match {
              case Some(clusterTime) =>
                coll
                  .watch[BSONDocument](
                    offset = Some(StartAt(operationTime = clusterTime))
                  )
                  .cursor[Cursor.WithOps]
                  .head

              case _ =>
                Future.failed(new Exception("The event had no clusterTime"))
            }
          }
          // See comment above
          val forkedInsertion = delayBy(500.millis) {
            coll.insert(ordered = false).many(Seq(testDocument1, testDocument2))
          }

          // then
          (forkedInsertion.map(_ => {}) must haveCompleted) and (result must {
            {
              haveField[String]("operationType").that(beTypedEqualTo("insert"))
            } and {
              haveField[BSONDocument]("documentKey").that {
                haveField[String]("_id").that(beTypedEqualTo(
                  "clusterTime_test1"
                ))
              }
            } and {
              haveField[BSONDocument]("fullDocument").that(beTypedEqualTo(
                testDocument1
              ))
            }
          }.await(1, timeout))
        }
      }
    }

    "lookup the most recent document version" in {
      skipIfNotRSAndNotVersionAtLeast(MongoWireVersion.V36) {
        withTmpCollection(db) { (coll: BSONCollection) =>
          type R = (String, Future[Unit], Future[BSONDocument])

          val fieldName = "foo"
          val lastValue = "bar3"

          // This test is a bit more tricky. We want to first capture
          // the insert event so that we know where we will resume.
          // Then we produce two update events, resume the stream
          // with the first update event, but check that the looked-up document
          // corresponds to the second update.
          def result: R = {
            // given
            val initialCursor =
              coll.watch[BSONDocument]().cursor[Cursor.WithOps]
            val id = s"lookup_test${System.identityHashCode(initialCursor)}"
            val testDocument = BSONDocument("_id" -> id, fieldName -> "bar1")

            import coll.AggregationFramework.ChangeStream.ResumeAfter

            val res = foldOne(initialCursor).flatMap { firstEvent =>
              firstEvent.get("_id") match {
                case None => Future.failed(new Exception("The event had no id"))
                case Some(eventId) =>
                  // when
                  for {
                    _ <- coll
                      .update(ordered = false)
                      .one(
                        BSONDocument("_id" -> id),
                        BSONDocument(
                          f"$$set" -> BSONDocument(fieldName -> "bar2")
                        )
                      )
                    _ <- coll
                      .update(ordered = false)
                      .one(
                        BSONDocument("_id" -> id),
                        BSONDocument(
                          f"$$set" -> BSONDocument(fieldName -> lastValue)
                        )
                      )
                    resumedCursor = coll
                      .watch[BSONDocument](
                        offset = Some(ResumeAfter(eventId)),
                        fullDocumentStrategy =
                          Some(ChangeStreams.FullDocumentStrategy.UpdateLookup)
                      )
                      .cursor[Cursor.WithOps]

                    event <- delayBy(500.millis)(resumedCursor.head)
                  } yield event
              }
            }

            // See comment above
            (
              id,
              delayBy(500.millis) {
                coll.insert(ordered = false).one(testDocument).map(_ => {})
              },
              res
            )
          }

          // then
          result must beLike[R] {
            case (id, insertion, res) =>
              insertion must beTypedEqualTo({}).awaitFor(timeout) and {
                res must {
                  {
                    haveField[String]("operationType").that(beTypedEqualTo(
                      "update"
                    ))
                  } and {
                    haveField[BSONDocument]("documentKey").that {
                      haveField[String]("_id").that(beTypedEqualTo(id))
                    }
                  } and {
                    haveField[BSONDocument]("fullDocument").that {
                      haveField[String](fieldName).that(beTypedEqualTo(
                        lastValue
                      ))
                    }
                  }
                }.await(1, timeout)
              }
          }.eventually(retries = 2, sleep = timeout)
        }
      }
    }
  }

  @inline private def skipIfNotRSAndNotVersionAtLeast[R: AsResult](
      version: MongoWireVersion
    )(r: => R
    ) = skippedIf(isNotReplicaSet, isNotAtLeast(db, version))(r)

  // head will always fail on a changeStream cursor, so we need to fold a single element
  private def foldOne[T](cursor: Cursor.WithOps[T]): Future[T] =
    cursor.collect[List](maxDocs = 1, Cursor.FailOnError()).flatMap { result =>
      result.headOption match {
        case Some(value) => Future.successful(value)
        case _           => Future.failed(new NoSuchElementException())

      }
    }

  private def delayBy[T](
      duration: FiniteDuration
    )(f: => Future[T]
    ): Future[T] = {
    val promise = Promise[T]()

    tests.Common.driverSystem.scheduler
      .scheduleOnce(duration)(f.onComplete(promise.complete))

    promise.future
  }

  @inline private def haveCompleted: Matcher[Future[Unit]] =
    beTypedEqualTo(()).await(1, timeout)

}
