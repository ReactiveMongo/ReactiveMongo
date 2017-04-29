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
    def valueStr = "1"
  }

  object Descending extends IndexType {
    def value = BSONInteger(-1)
    def valueStr = "-1"
  }

  object Geo2D extends IndexType {
    def value = BSONString("2d")
    def valueStr = "2d"
  }

  object Geo2DSpherical extends IndexType {
    def value = BSONString("2dsphere")
    def valueStr = "2dsphere"
  }

  object GeoHaystack extends IndexType {
    def value = BSONString("geoHaystack")
    def valueStr = "geoHaystack"
  }

  object Hashed extends IndexType {
    def value = BSONString("hashed")
    def valueStr = "hashed"
  }

  object Text extends IndexType {
    def value = BSONString("text")
    def valueStr = "text"
  }

  def apply(value: BSONValue) = value match {
    case BSONInteger(i) if i > 0             => Ascending
    case BSONInteger(i) if i < 0             => Descending
    case BSONDouble(i) if i > 0              => Ascending
    case BSONDouble(i) if i < 0              => Descending
    case BSONLong(i) if i > 0                => Ascending
    case BSONLong(i) if i < 0                => Descending
    case BSONString(s) if s == "2d"          => Geo2D
    case BSONString(s) if s == "2dsphere"    => Geo2DSpherical
    case BSONString(s) if s == "geoHaystack" => GeoHaystack
    case BSONString(s) if s == "hashed"      => Hashed
    case BSONString(s) if s == "text"        => Text
    case _                                   => throw new IllegalArgumentException("unsupported index type")
  }
}
