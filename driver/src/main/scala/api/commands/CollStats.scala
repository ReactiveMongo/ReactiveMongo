package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

@deprecated("Internal: will be made private", "0.16.0")
class CollStats(val scale: Option[Int] = None)
  extends Product with Serializable
  with CollectionCommand with CommandWithResult[CollStatsResult] {

  val productArity = 1

  def productElement(n: Int): Any = scale

  def canEqual(that: Any): Boolean = that match {
    case _: CollStats => true
    case _            => false
  }

  override def equals(that: Any): Boolean = that match {
    case other: CollStats =>
      this.scale == other.scale

    case _ =>
      false
  }

  override def hashCode: Int = scale.hashCode

  override def toString: String = s"CollStats($scale)"
}

object CollStats
  extends scala.runtime.AbstractFunction1[Option[Int], CollStats] {

  @inline def apply(scale: Option[Int]): CollStats = new CollStats(scale)

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[CollStats]] = {
    val builder = pack.newBuilder

    pack.writer[ResolvedCollectionCommand[CollStats]] { cmd =>
      val elms = Seq.newBuilder[pack.ElementProducer]

      elms += builder.elementProducer(
        "collStats", builder.string(cmd.collection))

      cmd.command.scale.foreach { scale =>
        elms += builder.elementProducer("scale", builder.int(scale))
      }

      builder.document(elms.result())
    }
  }

  def reader[P <: SerializationPack](pack: P): pack.Reader[CollStatsResult] = {
    val decoder = pack.newDecoder

    pack.reader[CollStatsResult] { doc =>
      (for {
        ns <- decoder.string(doc, "ns")
        count <- decoder.int(doc, "count")
        sz <- decoder.double(doc, "size")
        avg = decoder.double(doc, "avgObjSize")
        ss <- decoder.double(doc, "storageSize")
        ne = decoder.int(doc, "numExtents")
        ni <- decoder.int(doc, "nindexes")
        le = decoder.int(doc, "lastExtentSize")
        pf = decoder.double(doc, "paddingFactor")
        sf = decoder.int(doc, "systemFlags")
        uf = decoder.int(doc, "userFlags")
        ti <- decoder.int(doc, "totalIndexSize")
        is <- decoder.child(doc, "indexSizes").map { d =>
          decoder.names(d).toList.flatMap { nme =>
            decoder.int(d, nme).map(nme -> _)
          }
        }
        cp = decoder.booleanLike(doc, "capped").getOrElse(false)
        mx = decoder.long(doc, "max")
        ms = decoder.double(doc, "maxSize")
      } yield CollStatsResult(
        ns, count, sz, avg, ss, ne, ni, le, pf, sf,
        uf, ti, is, cp, mx, ms)).get
    }
  }
}

/**
 * Various information about a collection.
 *
 * @param ns The fully qualified collection name.
 * @param count The number of documents in this collection.
 * @param size The size in bytes (or in bytes / scale, if any).
 * @param averageObjectSize The average object size in bytes (or in bytes / scale, if any).
 * @param storageSize Preallocated space for the collection.
 * @param numExtents Number of extents (contiguously allocated chunks of datafile space, only for mmapv1 storage engine).
 * @param nindexes Number of indexes.
 * @param lastExtentSize Size of the most recently created extent (only for mmapv1 storage engine).
 * @param paddingFactor Padding can speed up updates if documents grow (only for mmapv1 storage engine).
 * @param systemFlags System flags.
 * @param userFlags User flags.
 * @param indexSizes Size of specific indexes in bytes.
 * @param capped States if this collection is capped.
 * @param max The maximum number of documents of this collection, if capped.
 * @param maxSize The maximum size in bytes (or in bytes / scale, if any) of this collection, if capped.
 */
case class CollStatsResult( // TODO: Move to `api` package
  ns: String,
  count: Int,
  size: Double,
  averageObjectSize: Option[Double],
  storageSize: Double,
  numExtents: Option[Int],
  nindexes: Int,
  lastExtentSize: Option[Int],
  paddingFactor: Option[Double],
  systemFlags: Option[Int],
  userFlags: Option[Int],
  totalIndexSize: Int,
  sizePerIndex: List[(String, Int)],
  capped: Boolean,
  max: Option[Long],
  maxSize: Option[Double] = None) {
  @inline def indexSizes: Array[(String, Int)] = sizePerIndex.toArray

  override def toString = s"""CollStatsResult($ns, capped = $capped, count = $count, size = $size, avgObjSize = $averageObjectSize, storageSize = $storageSize, numExtents = $numExtents, nindexes = $nindexes, lastExtentSize = $lastExtentSize, paddingFactor = $paddingFactor, systemFlags = $systemFlags, userFlags = $userFlags, sizePerIndex = ${sizePerIndex.mkString("[ ", ", ", " ]")}, max = $max)"""
}
