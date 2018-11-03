package reactivemongo
package bson

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class BSONElementBenchmark {
  private var element: BSONElement = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    element = BSONElement("foo", BSONDocumentBenchmark.bigDocument())
  }

  @Benchmark
  def generateElement(): Traversable[BSONElement] = element.generate()
}
