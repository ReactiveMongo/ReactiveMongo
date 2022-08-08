package reactivemongo.api.indexes

import reactivemongo.api.SerializationPack

/** Type of Index */
sealed trait IndexType {
  protected[indexes] def valueStr: String

  @inline override def toString = valueStr
}

object IndexType {

  object Ascending extends IndexType {
    val valueStr = "1"
    @inline override def toString = "asc"
  }

  object Descending extends IndexType {
    val valueStr = "-1"
    @inline override def toString = "desc"
  }

  object Geo2D extends IndexType {
    val valueStr = "2d"
  }

  object Geo2DSpherical extends IndexType {
    val valueStr = "2dsphere"
    @inline override def toString = valueStr
  }

  object GeoHaystack extends IndexType {
    val valueStr = "geoHaystack"
    @inline override def toString = valueStr
  }

  object Hashed extends IndexType {
    val valueStr = "hashed"
    @inline override def toString = valueStr
  }

  object Text extends IndexType {
    val valueStr = "text"
    @inline override def toString = valueStr
  }

  private[reactivemongo] def read[P <: SerializationPack](
      pack: P
    )(key: pack.Document,
      name: String
    ): Option[IndexType] = {
    val decoder = pack.newDecoder

    decoder
      .int(key, name)
      .map { i =>
        if (i > 0) Ascending
        else Descending
      }
      .orElse(decoder.string(key, name).flatMap {
        case Geo2D.valueStr          => Some(Geo2D)
        case Geo2DSpherical.valueStr => Some(Geo2DSpherical)
        case GeoHaystack.valueStr    => Some(GeoHaystack)
        case Hashed.valueStr         => Some(Hashed)
        case Text.valueStr           => Some(Text)
        case _                       => None
      })
  }

  @SuppressWarnings(Array("UnusedMethodParameter"))
  private[api] def write[P <: SerializationPack](
      pack: P
    )(builder: SerializationPack.Builder[pack.type]
    ): IndexType => pack.Value = {
    case _: Ascending.type =>
      builder.int(1)

    case _: Descending.type =>
      builder.int(-1)

    case t =>
      builder.string(t.valueStr)
  }
}
