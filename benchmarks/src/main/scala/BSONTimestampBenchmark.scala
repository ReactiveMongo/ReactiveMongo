package reactivemongo
package bson

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class BSONTimestampBenchmark {
  private val timeMs = 1574884443000L
  private val timeSec = 366L
  private val ordinal = -1368554632

  @Benchmark
  def fromTimeMS() = {
    val ts = BSONTimestamp(timeMs)

    assert(ts.time == timeSec && ts.ordinal == ordinal && ts.value == timeMs)
  }

  @Benchmark
  def fromSecOrdinal() = {
    val ts = BSONTimestamp(timeSec, ordinal)

    // !! Wrong time & ordinal (fix in Bison)
    assert(ts.time == 4294966929L && ts.ordinal == -1368554632 &&
      ts.value == -1573326584968L)
  }
}
