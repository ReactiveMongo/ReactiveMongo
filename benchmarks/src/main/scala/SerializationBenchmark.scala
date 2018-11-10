package reactivemongo
package bson

import org.openjdk.jmh.annotations._

import reactivemongo.bson.buffer.{
  DefaultBufferHandler,
  ArrayBSONBuffer,
  ReadableBuffer,
  WritableBuffer
}, DefaultBufferHandler.serialize

sealed trait SerializationBenchmark {
  private var written: WritableBuffer = _
  private var value: BSONValue = _

  private var input: ReadableBuffer = _
  private var output: WritableBuffer = _

  protected def setupSerialization[B <: BSONValue](values: Seq[B]): Unit =
    values.headOption.foreach { v =>
      value = v

      written = DefaultBufferHandler.serialize(
        v, (new ArrayBSONBuffer).writeByte(value.code)).writeString("field")
    }

  @Setup(Level.Invocation)
  def setupInvocation(): Unit = {
    output = new ArrayBSONBuffer
    input = written.toReadableBuffer
  }

  @Benchmark
  def write(): WritableBuffer =
    DefaultBufferHandler.serialize(value, output)

  @Benchmark
  def read(): Unit = {
    val result = DefaultBufferHandler.deserialize(input)
    result == ("field" -> value)
  }
}

@State(Scope.Benchmark)
class BSONBooleanSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonBoolFixtures)

}

@State(Scope.Benchmark)
class BSONDateTimeSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonDateTimeFixtures)

}

@State(Scope.Benchmark)
class BSONDecimalSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonDecimalFixtures)

}

@State(Scope.Benchmark)
class BSONDoubleSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonDoubleFixtures)

}

@State(Scope.Benchmark)
class BSONIntegerSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonIntFixtures)
}

@State(Scope.Benchmark)
class BSONLongSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonLongFixtures)
}

@State(Scope.Benchmark)
class BSONStringSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonStrFixtures)
}

@State(Scope.Benchmark)
class BSONObjectIDSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonOidFixtures)
}

@State(Scope.Benchmark)
class BSONJavascriptSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonJSFixtures)
}

@State(Scope.Benchmark)
class BSONJavascriptWsSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonJSWsFixtures)
}

@State(Scope.Benchmark)
class BSONTimestampSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonTsFixtures)
}

@State(Scope.Benchmark)
class BSONBinarySerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonBinFixtures)
}

@State(Scope.Benchmark)
class BSONRegexSerializationBenchmark extends SerializationBenchmark {
  @Setup(Level.Iteration)
  def setup(): Unit = setupSerialization(BSONValueFixtures.bsonRegexFixtures)
}
