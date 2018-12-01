package reactivemongo.bson

import org.openjdk.jmh.annotations._

import reactivemongo.bson.buffer.{
  ArrayBSONBuffer,
  ReadableBuffer,
  WritableBuffer
}

@State(Scope.Benchmark)
class ReadableBufferBenchmark {
  private val str = "loremIpsumDolor"

  private var fixture: WritableBuffer = _
  private var input: ReadableBuffer = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    fixture = new ArrayBSONBuffer().writeString(str)
  }

  @Setup(Level.Invocation)
  def setupInvocation(): Unit = {
    input = fixture.toReadableBuffer
  }

  @Benchmark
  def readInt(): Unit = {
    assert((input.readInt() - 1) == str.size)
  }

  @Benchmark
  def readString(): Unit =
    assert(input.readString == str)

  @Benchmark
  def toWritableBuffer(): Unit =
    assert(input.toWritableBuffer.index == input.size)

}
