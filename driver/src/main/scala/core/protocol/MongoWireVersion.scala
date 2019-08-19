package reactivemongo.core.protocol

sealed trait MongoWireVersion extends Ordered[MongoWireVersion] {
  /** The numeric representation */
  def value: Int

  final def compare(x: MongoWireVersion): Int =
    if (value == x.value) 0
    else if (value < x.value) -1
    else 1

  override lazy val hashCode = toString.hashCode
}

object MongoWireVersion {
  @deprecated("MongoDB 2.6 EOL reached by Oct 2016: https://www.mongodb.com/support-policy", "0.16.0")
  object V26 extends MongoWireVersion {
    val value = 2
    override val toString = "2.6"

    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V26.type]
  }

  @deprecated("MongoDB 3.0 EOL reached by Feb 2018: https://www.mongodb.com/support-policy", "0.16.0")
  object V30 extends MongoWireVersion {
    val value = 3
    override val toString = "3.0"
    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V30.type]
  }

  object V32 extends MongoWireVersion {
    val value = 4
    override val toString = "3.2"
    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V32.type]
  }

  object V34 extends MongoWireVersion {
    val value = 5
    override val toString = "3.4"
    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V34.type]
  }

  object V36 extends MongoWireVersion {
    val value = 6
    override val toString = "3.6"
    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V36.type]
  }

  object V40 extends MongoWireVersion {
    val value = 7
    override val toString = "4.0"
    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V40.type]
  }

  object V42 extends MongoWireVersion {
    val value = 8
    override val toString = "4.2"
    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V42.type]
  }

  def apply(v: Int): MongoWireVersion = {
    if (v <= V26.value) V26
    else if (v >= V42.value) V42
    else if (v >= V40.value) V40
    else if (v >= V36.value) V36
    else if (v >= V34.value) V34
    else if (v >= V32.value) V32
    else V30
  }

  def unapply(v: MongoWireVersion): Option[Int] = Some(v.value)
}
