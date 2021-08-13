package reactivemongo.api

sealed trait Compressor {
  /** The compressor [[https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst#mongodb-handshake-amendment name]] (e.g `snappy`) */
  def name: String

  /** The compressor [[https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst#compressor-ids ID]] (e.g. `1` for snappy) */
  def id: Byte

  @inline override def toString = name
}

object Compressor {
  /**
   * The content of the message is uncompressed.
   * This is realistically only used for testing.
   */
  object Noop extends Compressor {
    val name = "noop"
    val id: Byte = 0
  }

  /** The content of the message is compressed using snappy. */
  object Snappy extends Compressor {
    val name = "snappy"
    val id: Byte = 1
  }

  /**
   * The content of the message is compressed using zlib.
   *
   * @param compressionLevel Zlib compression [[https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst#zlibcompressionlevel level]] (from -1 - 9)
   */
  sealed class Zlib private[api] (
    val compressionLevel: Int) extends Compressor {

    val name = Zlib.name
    val id: Byte = 2

    override def hashCode: Int = compressionLevel

    override def equals(that: Any): Boolean = that match {
      case other: Zlib =>
        this.compressionLevel == other.compressionLevel

      case _ =>
        false
    }

    override def toString = s"Zlib($compressionLevel)"
  }

  object Zlib {
    val name = "zlib"

    lazy val DefaultCompressor: Zlib = new Zlib(-1)

    def apply(compressionLevel: Int): Zlib = new Zlib(compressionLevel)

    def unapply(compressor: Compressor): Option[Int] = compressor match {
      case zlib: Zlib =>
        Some(zlib.compressionLevel)

      case _ =>
        Option.empty[Int]
    }
  }

  /** The content of the message is compressed using zstd. */
  object Zstd extends Compressor {
    val name = "zstd"
    val id: Byte = 3
  }
}
