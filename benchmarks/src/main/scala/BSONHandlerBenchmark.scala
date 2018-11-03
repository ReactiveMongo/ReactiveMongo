package reactivemongo
package bson

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class BSONDocumentHandlerBenchmark {
  var smallDoc: BSONDocument = _
  var bigDoc: BSONDocument = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    smallDoc = BSONDocument("_id" -> 1, "name" -> "foo", "bar" -> 1.23D)
    bigDoc = BSONDocumentBenchmark.bigDocument()
  }

  @Benchmark
  def mapReaderSmallDoc(): Unit = {
    val res = MapReader[String, BSONValue].readTry(smallDoc)
    assert(res.isSuccess)
  }

  @Benchmark
  def mapReaderBigDoc(): Unit = {
    val res = MapReader[String, BSONValue].readTry(bigDoc)
    assert(res.isSuccess)
  }

  @Benchmark
  def identityReaderDoc(): Unit = {
    val res = implicitly[BSONReader[BSONDocument, BSONDocument]].readTry(bigDoc)
    assert(res.isSuccess)
  }

  @Benchmark
  def identityReader(): Unit = {
    val res = implicitly[BSONReader[BSONValue, BSONValue]].readTry(smallDoc)
    assert(res.isSuccess)
  }

  @Benchmark
  def identityWriterDoc(): Unit = {
    val r = implicitly[BSONWriter[BSONDocument, BSONDocument]].writeTry(bigDoc)
    assert(r.isSuccess)
  }

  @Benchmark
  def identityWriter(): Unit = {
    val res = implicitly[BSONWriter[BSONValue, BSONValue]].writeTry(smallDoc)
    assert(res.isSuccess)
  }
}

@State(Scope.Benchmark)
class BSONArrayHandlerBenchmark {
  var smallArray: BSONArray = _
  var bigArray: BSONArray = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    smallArray = BSONArray(1, "foo", 1.23D)
    bigArray = BSONArrayBenchmark.bigArray()
  }

  @Benchmark
  def seqReaderSmallArray(): Unit = {
    val res = bsonArrayToCollectionReader[Seq, BSONValue].readTry(smallArray)
    assert(res.isSuccess)
  }

  @Benchmark
  def listReaderBigArray(): Unit = {
    val res = bsonArrayToCollectionReader[List, BSONValue].readTry(bigArray)
    assert(res.isSuccess)
  }
}
