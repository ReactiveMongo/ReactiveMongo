package reactivemongo.bson

import scala.util.Random

import org.openjdk.jmh.annotations._

import reactivemongo.bson.buffer.{
  ArrayBSONBuffer,
  ReadableBuffer,
  WritableBuffer
}

@State(Scope.Benchmark)
class WritableBufferBenchmark {
  private var bytes: Array[Byte] = null
  private var emptyBuffer: ArrayBSONBuffer = null
  private var bytesBuffer: ArrayBSONBuffer = null

  @Setup(Level.Iteration)
  def setup(): Unit = {
    bytes = Array.ofDim[Byte](8096)
    Random.nextBytes(bytes)

    emptyBuffer = new ArrayBSONBuffer()
    bytesBuffer = new ArrayBSONBuffer().writeBytes(bytes)
  }

  @Benchmark // Only for internal testing, not part of runtime/API
  def empty(): ArrayBSONBuffer = new ArrayBSONBuffer()

  @Benchmark
  def size(): Int = emptyBuffer.index

  @Benchmark
  def setInt(): ArrayBSONBuffer = emptyBuffer.setInt(0, 10)

  @Benchmark
  def writeBytes(): ArrayBSONBuffer = emptyBuffer.writeBytes(bytes)

  @Benchmark
  def writeByte(): WritableBuffer = emptyBuffer.writeByte(0x04: Byte)

  @Benchmark
  def writeInt(): WritableBuffer = emptyBuffer.writeInt(20)

  @Benchmark
  def writeLong(): WritableBuffer = emptyBuffer.writeLong(1234L)

  @Benchmark
  def writeDouble(): WritableBuffer = emptyBuffer.writeDouble(567.78D)

  @Benchmark
  def array(): Array[Byte] = {
    val res = bytesBuffer.array
    assert(java.util.Arrays.equals(bytes, res))
    res
  }
}
