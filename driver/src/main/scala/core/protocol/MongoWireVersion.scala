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
  /*
   * Original meaning of MongoWireVersion is more about protocol features.
   *
   * - RELEASE_2_4_AND_BEFORE (0)
   * - AGG_RETURNS_CURSORS (1)
   * - BATCH_COMMANDS (2)
   *
   * But wireProtocol=1 is virtually non-existent; Mongo 2.4 was 0 and Mongo 2.6 is 2.
   */
  @deprecated(message = "No longer supported", since = "0.12.0")
  object V24AndBefore extends MongoWireVersion {
    val value = 0
    override val toString = "<=2.4"

    override def equals(that: Any): Boolean = that == V24AndBefore
  }

  object V26 extends MongoWireVersion {
    val value = 2
    override val toString = "2.6"

    override def equals(that: Any): Boolean =
      that != null && that.isInstanceOf[V26.type]
  }

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

  def apply(v: Int): MongoWireVersion = {
    if (v <= V26.value) V26
    else if (v >= V34.value) V34
    else if (v >= V32.value) V32
    else V30
  }

  def unapply(v: MongoWireVersion): Option[Int] = Some(v.value)
}
