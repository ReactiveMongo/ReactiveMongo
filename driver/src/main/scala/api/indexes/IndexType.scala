package reactivemongo.api.indexes

import reactivemongo.api.SerializationPack

import reactivemongo.bson.{
  BSONDouble,
  BSONInteger,
  BSONLong,
  BSONString,
  BSONValue
}

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

  private[reactivemongo] def unapply(value: BSONValue): Option[IndexType] =
    value match {
      case BSONInteger(i) if i > 0             => Some(Ascending)
      case BSONInteger(i) if i < 0             => Some(Descending)
      case BSONDouble(i) if i > 0              => Some(Ascending)
      case BSONDouble(i) if i < 0              => Some(Descending)
      case BSONLong(i) if i > 0                => Some(Ascending)
      case BSONLong(i) if i < 0                => Some(Descending)
      case BSONString(Geo2D.valueStr)          => Some(Geo2D)
      case BSONString(Geo2DSpherical.valueStr) => Some(Geo2DSpherical)
      case BSONString(GeoHaystack.valueStr)    => Some(GeoHaystack)
      case BSONString(Hashed.valueStr)         => Some(Hashed)
      case BSONString(Text.valueStr)           => Some(Text)
      case _                                   => None
    }

  private[reactivemongo] def apply(value: BSONValue): IndexType = value match {
    case IndexType(tpe) => tpe
    case _ =>
      throw new IllegalArgumentException("unsupported index type")
  }

  private[api] def write[P <: SerializationPack](pack: P)(builder: SerializationPack.Builder[pack.type]): IndexType => pack.Value = {
    case _: Ascending.type =>
      builder.int(1)

    case _: Descending.type =>
      builder.int(-1)

    case t =>
      builder.string(t.valueStr)
  }
}
