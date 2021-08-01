package reactivemongo.core

private[reactivemongo] sealed trait Compressor {
  /** The compressor [[https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst#mongodb-handshake-amendment name]] (e.g `snappy`) */
  def name: String

  /** The compressor [[https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst#compressor-ids ID]] (e.g. `1` for snappy) */
  def id: Int
}

private[reactivemongo] object Compressor {
  /**
   * The content of the message is uncompressed.
   * This is realistically only used for testing.
   */
  object Noop extends Compressor {
    val name = "noop"
    val id = 0
  }

  /** The content of the message is compressed using snappy. */
  object Snappy extends Compressor {
    val name = "snappy"
    val id = 1
  }

  /**
   * The content of the message is compressed using zlib.
   *
   * @param compressionLevel Zlib compression [[https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst#zlibcompressionlevel level]] (from -1 - 9)
   */
  class Zlib(val compressionLevel: Int) extends Compressor {
    val name = "zlib"
    val id = 2
  }

  /** The content of the message is compressed using zstd. */
  object Zstd extends Compressor {
    val name = "zstd"
    val id = 3
  }
}
