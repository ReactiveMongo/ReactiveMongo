package reactivemongo.core.protocol

sealed trait MongoWireVersion extends Ordered[MongoWireVersion] {

  /** The numeric representation */
  def value: Int

  final def compare(x: MongoWireVersion): Int =
    if (value == x.value) 0
    else if (value < x.value) -1
    else 1

  override final lazy val hashCode = toString.hashCode

  override final def equals(that: Any): Boolean = that match {
    case other: MongoWireVersion =>
      this.value == other.value

    case _ =>
      false
  }
}

object MongoWireVersion {

  @deprecated(
    "MongoDB 3.0 EOL reached by Feb 2018: https://www.mongodb.com/support-policy",
    "0.16.0"
  )
  object V30 extends MongoWireVersion {
    val value = 3
    override val toString = "3.0"
  }

  object V32 extends MongoWireVersion {
    val value = 4
    override val toString = "3.2"
  }

  object V34 extends MongoWireVersion {
    val value = 5
    override val toString = "3.4"
  }

  object V36 extends MongoWireVersion {
    val value = 6
    override val toString = "3.6"
  }

  object V40 extends MongoWireVersion {
    val value = 7
    override val toString = "4.0"
  }

  object V42 extends MongoWireVersion {
    val value = 8
    override val toString = "4.2"
  }

  object V50 extends MongoWireVersion {
    val value = 13
    override val toString = "5.0"
  }

  object V51 extends MongoWireVersion {
    val value = 14
    override val toString = "5.1"
  }

  object V60 extends MongoWireVersion {
    val value = 17
    override val toString = "6.0"
  }

  def apply(v: Int): MongoWireVersion = {
    if (v >= V60.value) V60
    else if (v >= V51.value) V51
    else if (v >= V50.value) V50
    else if (v >= V42.value) V42
    else if (v >= V40.value) V40
    else if (v >= V36.value) V36
    else if (v >= V34.value) V34
    else if (v >= V32.value) V32
    else V30
  }

  def unapply(v: MongoWireVersion): Option[Int] = Some(v.value)
}
