package aggregation

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

import akka.actor.ActorSystem
import api.ChangeStreams
import org.specs2.concurrent.ExecutionEnv
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{Cursor, CursorOptions, ReadConcern}
import reactivemongo.bson.{BSONDocument, BSONValue}
import tests.Common
import tests.Common.timeout

class ChangeStreamSpec(implicit val ee: ExecutionEnv)
  extends org.specs2.mutable.Specification
    with WithTemporaryDb {

  "Change streams specs".title

  sequential

  if (Common.replSetOn) {
    lazy val coll: BSONCollection = {
      val c: BSONCollection = db("changeStreamTests")
      scala.concurrent.Await.result(c.create(), timeout * 2)
      c
    }

    "the change stream of a collection" should {
      "return the next change event" in {
        // given
        val cursor = watch(coll)().cursor
        val testDocument = BSONDocument(
          "_id" -> "test",
          "foo" -> "bar",
        )

        // when
        val result = foldOne(cursor)
        // The cursor needs to have been opened by the time the insert operation is executed
        // We have no means to guarantee that with the Cursor API, so the following is a best-effort solution,
        // but a race-condition is still possible and may lead to a false negative test result.
        delayBy(500.millis) {
          coll.insert(ordered = false).one(testDocument)
        } must beEqualTo(()).await

        // then
        result.map { event =>
          event.getAs[String]("operationType") must beSome("insert")
          event.getAs[BSONDocument]("documentKey")
            .flatMap(_.getAs[String]("_id")) must beSome("test")
          event.getAs[BSONDocument]("fullDocument") must beSome(testDocument)
        }.await(retries = 2, 1.second)
      }

      "resume with the next event after a known id" in {
        // given
        val cursor = watch(coll)().cursor
        val testDocument1 = BSONDocument(
          "_id" -> "resume_test1",
          "foo" -> "bar",
        )
        val testDocument2 = BSONDocument(
          "_id" -> "resume_test2",
          "foo" -> "baz",
        )

        // when
        val firstEventFuture = foldOne(cursor)
        // See comment above
        delayBy(500.millis) {
          coll.insert(ordered = false).many(Seq(testDocument1, testDocument2))
        } must beEqualTo(()).await
        val result = firstEventFuture.flatMap { firstEvent =>
          firstEvent.get("_id") match {
            case None => Future.failed(new Exception("The event had no id"))
            case Some(eventId) => foldOne(watch(coll)(resumeAfter = Some(eventId)).cursor)
          }
        }

        // then
        result.map { event =>
          event.getAs[String]("operationType") must beSome("insert")
          event.getAs[BSONDocument]("documentKey")
            .flatMap(_.getAs[String]("_id")) must beSome("resume_test2")
          event.getAs[BSONDocument]("fullDocument") must beSome(testDocument2)
        }.await(retries = 2, 1.second)
      }

      "resume with the same event after a known operation time" in {
        // given
        val cursor = watch(coll)().cursor
        val testDocument1 = BSONDocument(
          "_id" -> "clusterTime_test1",
          "foo" -> "bar",
        )
        val testDocument2 = BSONDocument(
          "_id" -> "clusterTime_test2",
          "foo" -> "baz",
        )

        // when
        val firstEventFuture = foldOne(cursor)
        // See comment above
        delayBy(500.millis) {
          coll.insert(ordered = false).many(Seq(testDocument1, testDocument2))
        } must beEqualTo(()).await
        val result = firstEventFuture.flatMap { firstEvent =>
          firstEvent.get("clusterTime") match {
            case None => Future.failed(new Exception("The event had no clusterTime"))
            case Some(clusterTime) => foldOne(watch(coll)(startAtOperationTime = Some(clusterTime)).cursor)
          }
        }

        // then
        result.map { event =>
          event.getAs[String]("operationType") must beSome("insert")
          event.getAs[BSONDocument]("documentKey")
            .flatMap(_.getAs[String]("_id")) must beSome("clusterTime_test1")
          event.getAs[BSONDocument]("fullDocument") must beSome(testDocument1)
        }.await(retries = 2, 1.second)
      }

      "lookup the most recent document version" in {
        // given
        val cursor = watch(coll)().cursor
        val id = "lookup_test1"
        val fieldName = "foo"
        val lastValue = "bar3"
        val testDocument = BSONDocument(
          "_id" -> id,
          fieldName -> "bar1",
        )

        // This test is a bit more tricky. We want to first capture the insert event so that we know where we will
        // resume. Then we produce two update events, resume the stream with the first update event, but check that the
        // looked-up document corresponds to the second update.
        val firstEventFuture = foldOne(cursor)
        // See comment above
        delayBy(500.millis) {
          coll.insert(ordered = false).one(testDocument)
        } must beEqualTo(()).await
        val result = firstEventFuture.flatMap { firstEvent =>
          firstEvent.get("_id") match {
            case None => Future.failed(new Exception("The event had no id"))
            case Some(eventId) =>

              // when
              for {
                _ <- coll.update(ordered = false).one(
                  BSONDocument("_id" -> id),
                  BSONDocument(f"$$set" -> BSONDocument(fieldName -> "bar2"))
                )
                _ <- coll.update(ordered = false).one(
                  BSONDocument("_id" -> id),
                  BSONDocument(f"$$set" -> BSONDocument(fieldName -> lastValue))
                )
                event <- foldOne(watch(coll)(
                  resumeAfter = Some(eventId),
                  fullDocument = Some(ChangeStreams.FullDocument.UpdateLookup)
                ).cursor)
              } yield event
          }
        }

        // then
        result.map { event =>
          event.getAs[String]("operationType") must beSome("update")
          event.getAs[BSONDocument]("documentKey")
            .flatMap(_.getAs[String]("_id")) must beSome(id)
          event.getAs[BSONDocument]("fullDocument")
            .flatMap(_.getAs[String](fieldName)) must beSome(lastValue)
        }.await(retries = 2, 1.second)
      }
    }
  } else {
    "untestable because the target mongo server is not within a Replica Set" in skipped
  }

  private def watch(collection: BSONCollection)(
    resumeAfter: Option[BSONValue] = None,
    startAtOperationTime: Option[BSONValue] = None,
    fullDocument: Option[ChangeStreams.FullDocument] = None
  ) = {
    import collection.BatchCommands.AggregationFramework.ChangeStream
    collection.aggregatorContext[BSONDocument](
      firstOperator = ChangeStream(resumeAfter, startAtOperationTime, fullDocument),
      readConcern = Some(ReadConcern.Majority),
      cursorOptions = CursorOptions.empty.tailable,
    ).prepared[Cursor.WithOps]
  }

  private val actorSystem = ActorSystem("changeStreams")

  override def afterAll: Unit = {
    Await.ready(actorSystem.terminate(), timeout)
    super.afterAll
  }

  private def foldOne[T](cursor: Cursor.WithOps[T]): Future[T] = {
    cursor.collect[List](maxDocs = 1, Cursor.FailOnError()).flatMap { result =>
      Future.fromTry(Try(result.head))
    }
  }

  private def delayBy(duration: FiniteDuration)(f: => Future[_]): Future[Unit] = {
    val promise = Promise[Unit]()
    actorSystem.scheduler.scheduleOnce(duration)(f.map(_ => ()).onComplete(promise.complete))
    promise.future
  }

}
