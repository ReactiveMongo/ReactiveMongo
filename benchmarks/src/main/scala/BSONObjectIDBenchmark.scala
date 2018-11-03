package reactivemongo

import scala.util.{ Failure, Try }

import org.openjdk.jmh.annotations._

import reactivemongo.bson.BSONObjectID

@State(Scope.Benchmark)
class BSONObjectIDGenerateBenchmark {
  @Benchmark
  def generate(): BSONObjectID = BSONObjectID.generate()
}

@State(Scope.Benchmark)
class BSONObjectIDStringReprBenchmark {
  private def gen(): Seq[BSONObjectID] =
    scala.util.Random.shuffle(BSONValueFixtures.bsonOidFixtures)

  var fixtures = Seq.empty[BSONObjectID]
  var strings = Seq.empty[String]

  var oid: BSONObjectID = _
  var str: String = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    (fixtures.headOption, strings.headOption) match {
      case (Some(v), Some(s)) => {
        oid = v
        str = s

        fixtures = fixtures.tail
        strings = strings.tail
      }

      case _ => {
        fixtures = gen()
        strings = fixtures.map(_.stringify)
        setup()
      }
    }
  }

  @Benchmark
  def stringify(): String = oid.stringify

  @Benchmark
  def parse(): Try[BSONObjectID] = BSONObjectID.parse(str)
}

// TODO: HandlerSpec
// TODO: SerializationSpec
