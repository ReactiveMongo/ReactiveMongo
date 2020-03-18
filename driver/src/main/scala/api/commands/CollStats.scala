package reactivemongo.api.commands

import reactivemongo.api.{ CollectionStats, SerializationPack }

private[reactivemongo] final class CollStats(val scale: Option[Int] = None)
  extends CollectionCommand with CommandWithResult[CollectionStats] {

  override def equals(that: Any): Boolean = that match {
    case other: CollStats =>
      this.scale == other.scale

    case _ =>
      false
  }

  override def hashCode: Int = scale.hashCode

  override def toString: String = s"CollStats($scale)"
}

private[reactivemongo] object CollStats {
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

  def reader[P <: SerializationPack](pack: P): pack.Reader[CollectionStats] = {
    val decoder = pack.newDecoder

    pack.reader[CollectionStats] { doc =>
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
      } yield new CollectionStats(
        ns, count, sz, avg, ss, ne, ni, le, pf, sf,
        uf, ti, is, cp, mx, ms)).get
    }
  }
}
