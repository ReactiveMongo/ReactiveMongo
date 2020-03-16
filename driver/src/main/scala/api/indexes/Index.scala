package reactivemongo.api.indexes

import reactivemongo.bson.BSONDocument

import reactivemongo.api.{ Collation, Serialization, SerializationPack }

import com.github.ghik.silencer.silent

/**
 * A MongoDB index (excluding the namespace).
 *
 * Consider reading [[http://www.mongodb.org/display/DOCS/Indexes the documentation about indexes in MongoDB]].
 *
 * {{{
 * import reactivemongo.api.bson.BSONDocument
 * import reactivemongo.api.bson.collection.BSONSerializationPack
 * import reactivemongo.api.indexes.{ Index, IndexType }
 *
 * val bsonIndex = Index(
 *   key = Seq("name" -> IndexType.Ascending),
 *   name = Some("name_idx"),
 *   unique = false,
 *   background = false,
 *   sparse = false,
 *   expireAfterSeconds = None,
 *   storageEngine = None,
 *   weights = None,
 *   defaultLanguage = None,
 *   languageOverride = None,
 *   textIndexVersion = None,
 *   sphereIndexVersion = None,
 *   bits = None,
 *   min = None,
 *   max = None,
 *   bucketSize = None,
 *   collation = None,
 *   wildcardProjection = None,
 *   version = None,
 *   partialFilter = None,
 *   options = BSONDocument.empty)
 * }}}
 */
sealed abstract class Index {
  type Pack <: SerializationPack
  val pack: Pack

  /**
   * The index key (it can be composed of multiple fields).
   * This list should not be empty!
   */
  def key: Seq[(String, IndexType)] = Seq.empty // TODO#1.1: Remove impl

  /**
   * The name of this index (default: `None`).
   * If you provide none, a name will be computed for you.
   */
  def name: Option[String] = None

  /**
   * If this index should be built in background.
   * You should read [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-background%3Atrue the documentation about background indexing]] before using it.
   */
  def background: Boolean = false

  /** The flag to enforces uniqueness (default: `false`) */
  def unique: Boolean = false

  /**
   * Optional [[https://docs.mongodb.com/manual/core/index-partial/#partial-index-with-unique-constraints partial filter]]
   *
   * @since MongoDB 3.2
   */
  def partialFilter: Option[pack.Document]

  /**
   * The flags to indicates if the index to build
   * should only consider the documents that have the indexed fields
   * (default: `false`).
   *
   * See [[https://docs.mongodb.com/manual/indexes/#sparse-indexes the documentation]] on the consequences of such an index.
   */
  def sparse: Boolean = false

  /**
   * Optionally specifies a value, in seconds, as a [[https://docs.mongodb.com/manual/reference/glossary/#term-ttl TTL]] to control how long MongoDB retains documents in this collection.
   */
  def expireAfterSeconds: Option[Int] = None

  /**
   * Optionally specifies a configuration for the storage engine on a per-index basis when creating an index.
   *
   * @since MongoDB 3.0
   */
  def storageEngine: Option[pack.Document] = None

  /**
   * An optional document that contains field and weight pairs for [[https://docs.mongodb.com/manual/core/index-text/ text indexes]].
   */
  def weights: Option[pack.Document] = None

  /**
   * An optional default language for [[https://docs.mongodb.com/manual/core/index-text/ text indexes]].
   */
  def defaultLanguage: Option[String] = None

  /**
   * An optional language override for [[https://docs.mongodb.com/manual/core/index-text/ text indexes]].
   */
  def languageOverride: Option[String] = None

  /**
   * An optional `text` index [[https://docs.mongodb.com/manual/core/index-text/#text-versions version number]].
   */
  def textIndexVersion: Option[Int] = None

  /**
   * An optional `2dsphere` index [[https://docs.mongodb.com/manual/core/index-text/#text-versions version number]].
   */
  def _2dsphereIndexVersion: Option[Int] = None

  /**
   * Optionally indicates the precision of [[https://docs.mongodb.com/manual/reference/glossary/#term-geohash geohash]] for [[https://docs.mongodb.com/manual/core/2d/ 2d indexes]].
   */
  def bits: Option[Int] = None

  /**
   * Optionally indicates the lower inclusive boundary for longitude and latitude for [[https://docs.mongodb.com/manual/core/2d/ 2d indexes]].
   */
  def min: Option[Double] = None

  /**
   * Optionally indicates the upper inclusive boundary for longitude and latitude for [[https://docs.mongodb.com/manual/core/2d/ 2d indexes]].
   */
  def max: Option[Double] = None

  /**
   * Optionally specifies the number of units within which
   * to group the location values
   * for [[https://docs.mongodb.com/manual/core/geohaystack/ geoHaystack]]
   * indexes.
   */
  def bucketSize: Option[Double] = None

  /** An optional [[Collation]] (default: `None`) */
  def collation: Option[Collation] = None

  def wildcardProjection: Option[pack.Document] = None

  /**
   * Indicates the [[http://www.mongodb.org/display/DOCS/Index+Versions version]] of the index (1 for >= 2.0, else 0). You should let MongoDB decide.
   */
  def version: Option[Int] = None

  /**
   * Optional parameters for this index (typically specific to an IndexType like Geo2D).
   */
  def options: pack.Document

  /** The name of the index (a default one is computed if none). */
  lazy val eventualName: String = name.getOrElse(key.foldLeft("") {
    (name, kv) =>
      name + (if (name.length > 0) "_" else "") + kv._1 + "_" + kv._2.valueStr
  })

  private[api] lazy val tupled = Tuple21(key, name, background, unique, partialFilter, sparse, expireAfterSeconds, storageEngine, weights, defaultLanguage, languageOverride, textIndexVersion, _2dsphereIndexVersion, bits, min, max, bucketSize, collation, wildcardProjection, version, options)

  override def equals(that: Any): Boolean = that match {
    case other: Index => tupled == other.tupled
    case _            => false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"Index${tupled.toString}"
}

object Index {
  import reactivemongo.api.BSONSerializationPack

  type Aux[P] = Index { type Pack = P }

  type Default = Aux[Serialization.Pack]

  /**
   * {{{
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONSerializationPack
   * import reactivemongo.api.indexes.{ Index, IndexType }
   *
   * val bsonIndex = Index(BSONSerializationPack)(
   *   key = Seq("name" -> IndexType.Ascending),
   *   name = Some("name_idx"),
   *   unique = false,
   *   background = false,
   *   sparse = false,
   *   expireAfterSeconds = None,
   *   storageEngine = None,
   *   weights = None,
   *   defaultLanguage = None,
   *   languageOverride = None,
   *   textIndexVersion = None,
   *   sphereIndexVersion = None,
   *   bits = None,
   *   min = None,
   *   max = None,
   *   bucketSize = None,
   *   collation = None,
   *   wildcardProjection = None,
   *   version = None,
   *   partialFilter = None,
   *   options = BSONDocument.empty)
   * }}}
   */
  def apply[P <: SerializationPack](_pack: P)(
    key: Seq[(String, IndexType)],
    name: Option[String],
    unique: Boolean,
    background: Boolean,
    sparse: Boolean,
    expireAfterSeconds: Option[Int],
    storageEngine: Option[_pack.Document],
    weights: Option[_pack.Document],
    defaultLanguage: Option[String],
    languageOverride: Option[String],
    textIndexVersion: Option[Int],
    sphereIndexVersion: Option[Int],
    bits: Option[Int],
    min: Option[Double],
    max: Option[Double],
    bucketSize: Option[Double],
    collation: Option[Collation],
    wildcardProjection: Option[_pack.Document],
    version: Option[Int],
    partialFilter: Option[_pack.Document],
    options: _pack.Document): Index.Aux[P] = {
    def k = key
    def n = name
    def u = unique
    def b = background
    def s = sparse
    def e = expireAfterSeconds
    def se = storageEngine
    def w = weights
    def dl = defaultLanguage
    def lo = languageOverride
    def tiv = textIndexVersion
    def bs = bits
    def mi = min
    def mx = max
    def bu = bucketSize
    def cl = collation
    def wp = wildcardProjection
    def v = version
    def pf = partialFilter
    def o = options

    new Index {
      type Pack = P
      val pack: _pack.type = _pack

      override val key = k
      override val name = n
      override val unique = u
      override val background = b
      override val sparse = s
      override val expireAfterSeconds = e
      override val storageEngine = se
      override val weights = w
      override val defaultLanguage = dl
      override val languageOverride = lo
      override val textIndexVersion = tiv
      override val _2dsphereIndexVersion = sphereIndexVersion
      override val bits = bs
      override val min = mi
      override val max = mx
      override val bucketSize = bu
      override val collation = cl
      override val wildcardProjection = wp
      override val version = v
      val partialFilter = pf
      val options = o
    }
  }

  /**
   * {{{
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONSerializationPack
   * import reactivemongo.api.indexes.{ Index, IndexType }
   *
   * val bsonIndex = Index(
   *   key = Seq("name" -> IndexType.Ascending),
   *   name = Some("name_idx"),
   *   unique = false,
   *   background = false,
   *   sparse = false,
   *   expireAfterSeconds = None,
   *   storageEngine = None,
   *   weights = None,
   *   defaultLanguage = None,
   *   languageOverride = None,
   *   textIndexVersion = None,
   *   sphereIndexVersion = None,
   *   bits = None,
   *   min = None,
   *   max = None,
   *   bucketSize = None,
   *   collation = None,
   *   wildcardProjection = None,
   *   version = None,
   *   partialFilter = None,
   *   options = BSONDocument.empty)
   * }}}
   */
  def apply(
    key: Seq[(String, IndexType)],
    name: Option[String],
    unique: Boolean,
    background: Boolean,
    sparse: Boolean,
    expireAfterSeconds: Option[Int],
    storageEngine: Option[Serialization.Pack#Document],
    weights: Option[Serialization.Pack#Document],
    defaultLanguage: Option[String],
    languageOverride: Option[String],
    textIndexVersion: Option[Int],
    sphereIndexVersion: Option[Int],
    bits: Option[Int],
    min: Option[Double],
    max: Option[Double],
    bucketSize: Option[Double],
    collation: Option[Collation],
    wildcardProjection: Option[Serialization.Pack#Document],
    version: Option[Int],
    partialFilter: Option[Serialization.Pack#Document],
    options: Serialization.Pack#Document): Index.Aux[Serialization.Pack] =
    apply[Serialization.Pack](Serialization.internalSerializationPack)(
      key,
      name,
      unique,
      background,
      sparse,
      expireAfterSeconds,
      storageEngine,
      weights,
      defaultLanguage,
      languageOverride,
      textIndexVersion,
      sphereIndexVersion,
      bits,
      min,
      max,
      bucketSize,
      collation,
      wildcardProjection,
      version,
      partialFilter,
      options)

  /** '''EXPERIMENTAL:''' API may change */
  object Key {
    def unapplySeq(index: Index): Option[Seq[(String, IndexType)]] =
      Option(index).map(_.key)
  }
}

/**
 * A MongoDB namespaced index.
 * A MongoDB index is composed with the namespace (the fully qualified collection name) and the other fields of [[reactivemongo.api.indexes.Index]].
 *
 * Consider reading [[http://www.mongodb.org/display/DOCS/Indexes the documentation about indexes in MongoDB]].
 *
 * @param namespace The fully qualified name of the indexed collection.
 * @param index The other fields of the index.
 */
sealed trait NSIndex {
  type Pack <: SerializationPack

  def namespace: String

  def index: Index.Aux[Pack]

  lazy val (dbName: String, collectionName: String) = {
    val spanned = namespace.span(_ != '.')
    spanned._1 -> spanned._2.drop(1)
  }

  private[api] lazy val tupled = namespace -> index

  override def equals(that: Any): Boolean = that match {
    case other: NSIndex =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"NSIndex${tupled.toString}"
}

object NSIndex {
  type Aux[P <: SerializationPack] = NSIndex { type Pack = P }

  type Default = NSIndex.Aux[Serialization.Pack]

  def apply[P <: SerializationPack](
    namespace: String, index: Index.Aux[P]): NSIndex.Aux[P] = {
    @inline def nsp = namespace
    @inline def i = index
    new NSIndex {
      type Pack = P
      override val namespace = nsp
      override val index = i
    }
  }
}
