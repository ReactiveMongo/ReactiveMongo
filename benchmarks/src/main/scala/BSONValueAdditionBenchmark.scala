package reactivemongo

import org.openjdk.jmh.annotations._

import reactivemongo.bson.BSONValue

// ElementProducer.Composition(x, y)

@State(Scope.Benchmark)
class BSONBooleanAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonBoolFixtures
}

@State(Scope.Benchmark)
class BSONConstAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonConstFixtures
}

@State(Scope.Benchmark)
class BSONDateTimeAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonDateTimeFixtures
}

@State(Scope.Benchmark)
class BSONDecimalAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonDecimalFixtures
}

@State(Scope.Benchmark)
class BSONDoubleAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonDoubleFixtures
}

@State(Scope.Benchmark)
class BSONIntegerAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonIntFixtures
}

@State(Scope.Benchmark)
class BSONLongAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonLongFixtures
}

@State(Scope.Benchmark)
class BSONStringAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonStrFixtures
}

@State(Scope.Benchmark)
class BSONObjectIDAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonOidFixtures
}

@State(Scope.Benchmark)
class BSONJavascriptAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonJSFixtures
}

@State(Scope.Benchmark)
class BSONJavascriptWsAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonJSWsFixtures
}

@State(Scope.Benchmark)
class BSONTimestampAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonTsFixtures
}

@State(Scope.Benchmark)
class BSONBinaryAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonBinFixtures
}

@State(Scope.Benchmark)
class BSONRegexAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonRegexFixtures
}

@State(Scope.Benchmark)
class BSONArrayAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonArrayFixtures
}

@State(Scope.Benchmark)
class BSONDocumentAdditionBenchmark
  extends BSONValueAdditionBenchmark {
  val values: Seq[BSONValue] = BSONValueFixtures.bsonDocFixtures
}

// ---

sealed trait BSONValueAdditionBenchmark {
  protected def values: Seq[BSONValue]

  // TODO: Use iterator instead of Stream
  private def stream = Stream(scala.util.Random.shuffle(values): _*)

  private def gen(): Stream[BSONValue] = stream #::: gen()

  private var fixtures: Stream[BSONValue] = Stream.empty

  private var _1: BSONValue = _
  private var _2: BSONValue = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    fixtures match {
      case a #:: b #:: tail => {
        _1 = a
        _2 = b
        fixtures = tail
      }

      case _ => {
        fixtures = gen()
        setup()
      }
    }
  }

  @Benchmark
  def apply(): BSONValue = BSONValue.Addition(_1, _2)
}
