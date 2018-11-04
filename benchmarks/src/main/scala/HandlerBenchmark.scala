package reactivemongo
package bson

import org.openjdk.jmh.annotations._

sealed trait HandlerBenchmark {
  /* TODO
  protected def values: Traversable[BSONValue]
  protected def handler: BSONReader[_ <: BSONValue, _]
  private def h = handler.afterRead { v => (v: BSONValue) }

  @Benchmark
  def valueAsTry() = values.foreach { v =>
    assert(v.asTry(h).isSuccess)
    assert(!v.asTry[Unsupported.type].isSuccess)
  }

  @Benchmark
  def valueAsOpt() = values.foreach { v =>
    assert(v.asOpt(handler).isDefined)
    assert(v.asOpt[Unsupported.type].isEmpty)
  }

  private object Unsupported {
    implicit def reader: BSONReader[_ <: BSONValue, Unsupported.type] =
      BSONReader[BSONValue, Unsupported.type] { _ => ??? }
  }
   */
}

// ---

@State(Scope.Benchmark)
class BSONBooleanHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonBoolFixtures
  lazy val handler = BSONBooleanHandler
}

@State(Scope.Benchmark)
class BSONDateTimeHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonDateTimeFixtures
  lazy val handler = BSONDateTimeHandler
}

@State(Scope.Benchmark)
class BSONDecimalHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonDecimalFixtures
  lazy val handler = BSONDecimalHandler
}

@State(Scope.Benchmark)
class BSONDoubleHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonDoubleFixtures
  lazy val handler = BSONDoubleHandler
}

@State(Scope.Benchmark)
class BSONIntegerHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonIntFixtures
  lazy val handler = BSONIntegerHandler
}

@State(Scope.Benchmark)
class BSONLongHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonLongFixtures
  lazy val handler = BSONLongHandler
}

@State(Scope.Benchmark)
class BSONStrHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonStrFixtures
  lazy val handler = BSONStringHandler
}

@State(Scope.Benchmark)
class BSONBinaryHandlerBenchmark extends HandlerBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonBinFixtures
  lazy val handler = BSONBinaryHandler
}
