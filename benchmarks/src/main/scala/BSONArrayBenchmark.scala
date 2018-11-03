package reactivemongo
package bson

import scala.util.Random

import org.openjdk.jmh.annotations._

import reactivemongo.bson.buffer.{ ArrayBSONBuffer, ReadableBuffer }

@State(Scope.Benchmark)
class BSONArrayBenchmark {
  val values: Seq[BSONArray] =
    BSONValueFixtures.bsonArrayFixtures.filterNot(_.isEmpty)

  import BSONArrayBenchmark.bigArray

  private def gen(): Iterator[BSONArray] = values.iterator ++ gen()

  private var fixtures: Iterator[BSONArray] = Iterator.empty

  protected var bigSet: BSONArray = _
  protected var smallSet: BSONArray = _

  private val bsonArray = BSONArray(BSONBoolean(true))
  private val bsonArrayBytes = Array[Byte](9, 0, 0, 0, 8, 48, 0, 1, 0)
  private var emptyBuffer: ArrayBSONBuffer = _
  private var serializedBuffer: ReadableBuffer = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val data: Option[BSONArray] = {
      if (fixtures.hasNext) {
        Option(fixtures.next())
      } else {
        Option.empty[BSONArray]
      }
    }

    data match {
      case Some(a) => {
        bigSet = bigArray()
        smallSet = a
      }

      case _ => {
        fixtures = gen()
        setup()
      }
    }

    emptyBuffer = new ArrayBSONBuffer()
  }

  @Setup(Level.Invocation)
  def setupInvocation(): Unit = {
    serializedBuffer = new ArrayBSONBuffer().
      writeBytes(bsonArrayBytes).toReadableBuffer
  }

  @Benchmark
  def emptyArray() = BSONArray.empty

  @Benchmark
  def prettyPrintArray() = BSONArray.pretty(smallSet)

  @Benchmark
  def varArgArray(): BSONArray = BSONArray(bigSet.values)

  @Benchmark
  final def valuesBigSet() = bigSet.values

  @Benchmark
  final def valuesSmallSet() = smallSet.values

  @Benchmark
  final def headOptionBigSet = bigSet.headOption

  @Benchmark
  final def headOptionSmallSet = smallSet.headOption

  @Benchmark
  final def isEmptyBigSet: Boolean = bigSet.isEmpty

  @Benchmark
  final def isEmptySmallSet: Boolean = smallSet.isEmpty

  @Benchmark
  final def sizeBigSet: Int = bigSet.size

  @Benchmark
  final def sizeSmallSet: Int = smallSet.size

  @Benchmark
  final def appendSingleBigSet(): BSONArray =
    bigSet ++ BSONString("foo")

  @Benchmark
  final def appendSingleSmallSet(): BSONArray =
    smallSet ++ BSONString("bar")

  @Benchmark
  final def writeBigArray() = BSONArray.write(bigSet, emptyBuffer)

  @Benchmark
  final def readArray(): Unit = {
    val arr = BSONArray.read(serializedBuffer)
    assert(arr.filter(_ == bsonArray).isSuccess)
  }
}

object BSONArrayBenchmark {
  private[bson] def bigArray() = {
    val s = Seq.newBuilder[BSONValue]
    def randomValues() = Random.shuffle(BSONValueFixtures.bsonValueFixtures)
    var vs = randomValues().iterator

    (0 to 128).foreach { _ =>
      if (!vs.hasNext) {
        vs = randomValues().iterator
      }

      s += vs.next()
    }

    BSONArray(s.result())
  }
}
