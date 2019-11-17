package reactivemongo
package bson

import org.openjdk.jmh.annotations._

sealed trait HandlerBenchmark[B <: BSONValue] {
  protected def values: Traversable[B]
  protected def handler: BSONReader[B, _]
  protected def unsafeHandler: BSONReader[BSONValue, _]

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

// ---

@State(Scope.Benchmark)
class BSONBooleanHandlerBenchmark extends HandlerBenchmark[BSONBoolean] {
  val values = BSONValueFixtures.bsonBoolFixtures

  val handler = BSONBooleanHandler
  lazy val unsafeHandler = handler.beforeRead {
    case b @ BSONBoolean(_) => b
    case _                  => sys.error("Fail")
  }
}

@State(Scope.Benchmark)
class BSONDateTimeHandlerBenchmark extends HandlerBenchmark[BSONDateTime] {
  val values = BSONValueFixtures.bsonDateTimeFixtures

  val handler = BSONDateTimeHandler
  lazy val unsafeHandler = handler.beforeRead {
    case dt @ BSONDateTime(_) => dt
    case _                    => sys.error("Fail")
  }
}

@State(Scope.Benchmark)
class BSONDecimalHandlerBenchmark extends HandlerBenchmark[BSONDecimal] {
  val values = BSONValueFixtures.bsonDecimalFixtures.filter {
    case BSONDecimal.NegativeZero => false
    case dec                      => !dec.isInfinite && !dec.isNaN
  }

  val handler = BSONDecimalHandler
  lazy val unsafeHandler = handler.beforeRead {
    case dec: BSONDecimal => dec
    case _                => sys.error("Fail")
  }
}

@State(Scope.Benchmark)
class BSONDoubleHandlerBenchmark extends HandlerBenchmark[BSONDouble] {
  val values = BSONValueFixtures.bsonDoubleFixtures

  val handler = BSONDoubleHandler
  lazy val unsafeHandler = handler.beforeRead {
    case d @ BSONDouble(_) => d
    case _                 => sys.error("Fail")
  }
}

@State(Scope.Benchmark)
class BSONIntegerHandlerBenchmark extends HandlerBenchmark[BSONInteger] {
  val values = BSONValueFixtures.bsonIntFixtures

  val handler = BSONIntegerHandler
  lazy val unsafeHandler = handler.beforeRead {
    case i @ BSONInteger(_) => i
    case _                  => sys.error("Fail")
  }
}

@State(Scope.Benchmark)
class BSONLongHandlerBenchmark extends HandlerBenchmark[BSONLong] {
  val values = BSONValueFixtures.bsonLongFixtures

  val handler = BSONLongHandler
  lazy val unsafeHandler = handler.beforeRead {
    case l @ BSONLong(_) => l
    case _               => sys.error("Fail")
  }
}

@State(Scope.Benchmark)
class BSONStringHandlerBenchmark extends HandlerBenchmark[BSONString] {
  val values = BSONValueFixtures.bsonStrFixtures

  val handler = BSONStringHandler
  lazy val unsafeHandler = handler.beforeRead {
    case str @ BSONString(_) => str
    case _                   => sys.error("Fail")
  }
}

@State(Scope.Benchmark)
class BSONBinaryHandlerBenchmark extends HandlerBenchmark[BSONBinary] {
  val values = BSONValueFixtures.bsonBinFixtures

  val handler = BSONBinaryHandler
  lazy val unsafeHandler = handler.beforeRead {
    case bin @ BSONBinary(_, _) => bin
    case _                      => sys.error("Fail")
  }
}
