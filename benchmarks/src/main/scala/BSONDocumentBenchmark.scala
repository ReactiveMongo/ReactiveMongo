package reactivemongo
package bson

import org.openjdk.jmh.annotations._

import reactivemongo.bson.buffer.{
  ArrayBSONBuffer,
  ReadableBuffer,
  WritableBuffer
}

@State(Scope.Benchmark)
class BSONDocumentBenchmark {
  val values: Seq[BSONDocument] =
    BSONValueFixtures.bsonDocFixtures.filterNot(_.isEmpty)

  import BSONDocumentBenchmark.bigDocument

  private def gen(): Iterator[BSONDocument] = values.iterator ++ gen()

  private var fixtures: Iterator[BSONDocument] = Iterator.empty

  protected var bigSet: BSONDocument = _

  protected var smallSet: BSONDocument = _
  private var smallMap: Map[String, BSONValue] = _

  private val safeMap: Map[String, String] = Map(
    "Lorem ipsum" -> "dolor sit amet",
    "consectetur" -> "adipiscing elit",
    "sed do eiusmod tempor incididunt" -> "ut labore et dolore magna aliqua",
    "Ut enim ad" -> "minim veniam",
    "quis nostrud exercitation ullamco laboris nisi" -> "ut aliquip ex ea commodo consequat",
    "Duis aute irure dolor in reprehenderit in voluptate velit" -> "esse cillum dolore eu fugiat nulla pariatur",
    "Excepteur sint occaecat" -> "cupidatat non proident",
    "sunt in culpa" -> "qui officia deserunt mollit anim id est laborum")

  private val otherMap = Map(
    "foo" -> BigDecimal("123.45"),
    "bar lorem" -> BigDecimal(0L),
    "ipsum" -> BigDecimal(10000))

  private val complexMap: Map[Int, String] = (safeMap ++ otherMap.map {
    case (k, v) => k -> v.toString
  }).map {
    case (k, v) => k.hashCode -> v
  }

  protected var key: String = _

  private var emptyBuffer: ArrayBSONBuffer = _
  private var serializedBuffer: ReadableBuffer = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val data: Option[BSONDocument] = {
      if (fixtures.hasNext) {
        Option(fixtures.next())
      } else {
        Option.empty[BSONDocument]
      }
    }

    data match {
      case Some(a) => {
        bigSet = bigDocument()
        smallSet = a
        smallMap = a.toMap

        bigSet.elements.lastOption match {
          case Some(BSONElement(k, _)) =>
            key = k

          case _ =>
            sys.error(s"No element found: $a")
        }
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
    serializedBuffer = new ArrayBSONBuffer().writeBytes(
      SerializationFixtures.expectedWholeDocumentBytes).toReadableBuffer
  }

  @Benchmark
  def opsEmptyDocument() = documentOps(BSONDocument.empty)

  @Benchmark
  def opsElementsToDocument() = documentOps(BSONDocument(bigSet.elements: _*))

  @Benchmark
  def opsMapToDocument() = documentOps(BSONDocument(smallSet.toMap))

  @Benchmark
  def opsConcatDocument() = documentOps(bigSet ++ smallSet)

  @Benchmark
  def opsVarArgsToDocument() = documentOps(varArgsToDocument)

  @Benchmark
  def opsSmallSet() = documentOps(smallSet)

  @Benchmark
  def opsBigSet() = documentOps(bigSet)

  @Benchmark
  def opsSmallSetMinusKey() = documentOps(smallSet -- key)

  @Benchmark
  def opsBigSetMinusKey() = documentOps(bigSet -- key)

  private def documentOps(doc: => BSONDocument) = {
    // removeSingleKey
    doc -- key -- "notFoundKey"
    doc.--("notFoundKey", key)

    // prettyPrintDocument
    BSONDocument.pretty(doc)

    // elements
    doc.elements

    // contains
    doc.contains(key)
    doc.contains("notFoundKey")

    // values
    doc.values

    // toMap
    doc.toMap

    // headOption
    doc.headOption

    // isEmpty
    doc.isEmpty

    // size
    doc.size

    // appendSingle
    doc ++ BSONElement("_append", BSONString(key))

    // get
    doc.get(key)

    // write
    BSONDocument.write(doc, emptyBuffer)

    // byteSize
    doc.byteSize
  }

  @Benchmark
  def emptyDocument() = BSONDocument.empty

  @Benchmark
  def prettyPrintDocument() = BSONDocument.pretty(smallSet)

  @Benchmark
  def elementsToDocument() = BSONDocument(bigSet.elements: _*)

  /* Benchmark the implements conversions */
  @Benchmark
  def varArgsToDocument() = BSONDocument(
    BSONElement("foo", BSONString("bar")),
    "ipsum" -> BSONDouble(4D), // implicit conv. tuple as BSONElement
    "bolo" -> true, // implicit conv. true as BSONBoolean, tuple as BSONElement
    "option" -> Some("A"), // implicit Some as BSONString, tuple as BSONElement
    "emptyBsonValue" -> Option.empty[BSONValue],
    "emptyOption" -> Option.empty[String],
    "lorem" -> None // implicit conv. as ignored element (due to None)
  )

  @Benchmark
  def mapToDocument() = BSONDocument(smallSet.toMap)

  @Benchmark
  def concatDocuments(): ElementProducer =
    ElementProducer.Composition(bigSet, smallSet)

  @Benchmark
  def elementsBigSet() = bigSet.elements

  @Benchmark
  def elementsSmallSet() = smallSet.elements

  @Benchmark
  def containsBigSet(): Boolean = bigSet.contains(key)

  @Benchmark
  def containsSmallSet(): Boolean = smallSet.contains(key)

  @Benchmark
  def valuesBigSet() = bigSet.values

  @Benchmark
  def valuesSmallSet() = smallSet.values

  @Benchmark
  def toMapBigSet() = bigSet.toMap

  @Benchmark
  def toMapSmallSet() = smallSet.toMap

  @Benchmark
  def headOptionBigSet: Option[BSONElement] = bigSet.headOption

  @Benchmark
  def headOptionSmallSet: Option[BSONElement] = smallSet.headOption

  @Benchmark
  def isEmptyBigSet: Boolean = bigSet.isEmpty

  @Benchmark
  def isEmptySmallSet: Boolean = smallSet.isEmpty

  @Benchmark
  def sizeBigSet: Int = bigSet.size

  @Benchmark
  def sizeSmallSet: Int = smallSet.size

  @Benchmark
  def appendSingleBigSet(): BSONDocument =
    bigSet ++ BSONElement("_append", BSONString(key))

  @Benchmark
  def appendSingleSmallSet(): BSONDocument =
    smallSet ++ BSONElement("_append", BSONString(key))

  @Benchmark
  def getBigSet(): Option[BSONValue] = bigSet.get(key)

  @Benchmark
  def getSmallSet(): Option[BSONValue] = smallSet.get(key)

  @Benchmark
  def writeBigDocument(): WritableBuffer =
    BSONDocument.write(bigSet, emptyBuffer)

  @Benchmark
  def byteSize(): Int = bigSet.byteSize

  @Benchmark
  def readDocument(): Unit = {
    val doc = BSONDocument.read(serializedBuffer)
    assert(!doc.isEmpty)
  }

  @Benchmark
  def getAsOpt() = {
    bigSet.getAs[BSONValue](key)
    smallSet.getAs[BSONValue](key)
  }

  @Benchmark
  def getAsTry() = {
    bigSet.getAsTry[BSONValue](key)
    smallSet.getAsTry[BSONValue](key)
  }

  @Benchmark
  def getAsUnflattenedTry() = {
    assert(bigSet.getAsUnflattenedTry[BSONValue](key).
      toOption.flatten.isDefined)

    assert(bigSet.getAsUnflattenedTry[String]("_null").
      toOption.flatten.isEmpty)

    assert(smallSet.getAsUnflattenedTry[BSONValue](key).
      toOption.flatten.isEmpty)
  }

  @Benchmark
  def readAsCustomMap() = {
    assert(bigSet.asTry[Map[String, BSONValue]](MapReader).isSuccess)
  }

  @Benchmark
  def readAsSimpleMap() = {
    assert(bigSet.asTry[Map[String, BSONValue]](MapReader).isSuccess)
  }

  @Benchmark
  def writeFromSafeMap() = {
    assert(MapWriter[String, String].writeTry(safeMap).isSuccess)
  }

  @Benchmark
  def writeFromBSONMap() = {
    assert(MapWriter[String, BSONValue].writeTry(smallMap).isSuccess)
  }

  implicit object IntStringWriter extends BSONWriter[Int, BSONString] {
    def write(i: Int) = BSONString(i.toString)
  }

  @Benchmark
  def writeFromComplexMap() = {
    assert(MapWriter[Int, String].writeTry(complexMap).isSuccess)
  }

  @Benchmark
  def writeFromMap() = {
    assert(MapWriter[String, BigDecimal].writeTry(otherMap).isSuccess)
  }
}

object BSONDocumentBenchmark {
  private[bson] def bigDocument(): BSONDocument = {
    val m = Map.newBuilder[String, BSONValue]
    def values() = BSONValueFixtures.bsonValueFixtures.iterator
    var vs = values()

    (0 to 128).foreach { i =>
      if (!vs.hasNext) {
        vs = values()
      }

      m += s"field${i}" -> vs.next()
    }

    m += "_null" -> BSONNull

    BSONDocument(m.result())
  }
}
