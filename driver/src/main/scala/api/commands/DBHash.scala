package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

// See https://docs.mongodb.com/manual/reference/command/dbHash/
private[reactivemongo] final class DBHash(val collections: Seq[String])
  extends Command with CommandWithResult[DBHashResult]

final case class DBHashResult( // TODO: Move to `api` package
  host: String,
  collectionHashes: Map[String, String],
  md5: String,
  timeMillis: Long)

private[reactivemongo] object DBHash {
  def commandWriter[P <: SerializationPack](pack: P): pack.Writer[DBHash] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => element }

    pack.writer[DBHash] { hash =>
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements += element("dbHash", builder.int(1))

      hash.collections.headOption.foreach { first =>
        val arr = builder.array(
          builder.string(first),
          hash.collections.tail.map(builder.string))

        elements += element("collections", arr)
      }

      builder.document(elements.result())
    }
  }
}

private[reactivemongo] object DBHashResult {
  import scala.collection.breakOut
  import CommandCodecs.{ dealingWithGenericCommandErrorsReader => cmdReader }

  def reader[P <: SerializationPack](pack: P): pack.Reader[DBHashResult] = {
    val decoder = pack.newDecoder

    cmdReader[pack.type, DBHashResult](pack) { doc =>
      (for {
        host <- decoder.string(doc, "host")
        timeMillis <- decoder.long(doc, "timeMillis")
        md5 <- decoder.string(doc, "md5")
        collections <- decoder.child(doc, "collections")
        names = decoder.names(collections)
        hashes: Map[String, String] = names.flatMap({ name =>
          decoder.string(collections, name).map { name -> _ }
        })(breakOut)
      } yield DBHashResult(host, hashes, md5, timeMillis)).get
    }
  }
}
