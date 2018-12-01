package reactivemongo
package bson

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class BSONBooleanLikeHandlerBenchmark {

  val values: List[BSONValue] = BSONValueFixtures.bsonBoolFixtures ++ BSONValueFixtures.bsonIntFixtures ++ BSONValueFixtures.bsonDoubleFixtures ++ BSONValueFixtures.bsonLongFixtures ++ BSONValueFixtures.bsonDecimalFixtures ++ Seq(BSONNull, BSONUndefined)

  lazy val handler: BSONReader[BSONValue, _] = bsonBooleanLikeReader[BSONValue]
  @inline def unsafeHandler: BSONReader[BSONValue, _] = handler

  @Benchmark
  def valueAsSuccessfulTry() = values.foreach { v =>
    assert(v.asTry(handler).isSuccess)
    assert((v: BSONValue).asTry(unsafeHandler).isSuccess)
  }

  @Benchmark
  def valueAsFailedTry() = values.foreach { v =>
    assert(!(v: BSONValue).asTry[Unsupported.type].isSuccess)
  }

  @Benchmark
  def valueAsSuccessfulOpt() = values.foreach { v =>
    assert(v.asOpt(handler).isDefined)
    assert((v: BSONValue).asOpt(unsafeHandler).isDefined)
  }

  @Benchmark
  def valueAsFailedOpt() = values.foreach { v =>
    assert((v: BSONValue).asOpt[Unsupported.type].isEmpty)
  }

  private object Unsupported {
    implicit def reader: BSONReader[BSONValue, Unsupported.type] =
      BSONReader[BSONValue, Unsupported.type] { _ => ??? }
  }
}
