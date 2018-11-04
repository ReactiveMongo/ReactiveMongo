package reactivemongo

import java.util.UUID

import org.openjdk.jmh.annotations._

import reactivemongo.bson.BSONBinary

@State(Scope.Benchmark)
class BSONBinaryUUIDBenchmark {
  var uuid: UUID = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    uuid = UUID.randomUUID()
  }

  @Benchmark
  def fromUUID(): BSONBinary = BSONBinary(uuid)
}
