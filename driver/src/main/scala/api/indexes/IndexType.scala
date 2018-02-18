package reactivemongo.api.indexes

import reactivemongo.bson.{
  BSONDouble,
  BSONInteger,
  BSONLong,
  BSONString,
  BSONValue
}

/** Type of Index */
sealed trait IndexType {
  /** Value of the index (`{fieldName: value}`). */
  def value: BSONValue
  private[indexes] def valueStr: String
}

object IndexType {
  object Ascending extends IndexType {
    def value = BSONInteger(1)
    val valueStr = "1"
    @inline override def toString = "asc"
  }

  object Descending extends IndexType {
    def value = BSONInteger(-1)
    val valueStr = "-1"
    @inline override def toString = "desc"
  }

  object Geo2D extends IndexType {
    def value = BSONString(valueStr)
    val valueStr = "2d"
  }

  object Geo2DSpherical extends IndexType {
    def value = BSONString(valueStr)
    val valueStr = "2dsphere"
    @inline override def toString = valueStr
  }

  object GeoHaystack extends IndexType {
    def value = BSONString(valueStr)
    val valueStr = "geoHaystack"
    @inline override def toString = valueStr
  }

  object Hashed extends IndexType {
    def value = BSONString(valueStr)
    val valueStr = "hashed"
    @inline override def toString = valueStr
  }

  object Text extends IndexType {
    def value = BSONString(valueStr)
    val valueStr = "text"
    @inline override def toString = valueStr
  }

  def unapply(value: BSONValue): Option[IndexType] = value match {
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

  def apply(value: BSONValue): IndexType = value match {
    case IndexType(tpe) => tpe
    case _ =>
      throw new IllegalArgumentException("unsupported index type")
  }
}
