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
sealed abstract class Index extends Product with Serializable {
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
  private[api] def partialFilterDocument: Option[pack.Document]

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
   * If duplicates should be discarded (if unique = true; default: `false`).
   * _Warning_: you should read [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-dropDups%3Atrue the documentation]].
   */
  @deprecated("Since MongoDB 2.6", "0.19.1")
  def dropDups: Boolean = false

  /**
   * Indicates the [[http://www.mongodb.org/display/DOCS/Index+Versions version]] of the index (1 for >= 2.0, else 0). You should let MongoDB decide.
   */
  def version: Option[Int] = None

  /**
   * Optional parameters for this index (typically specific to an IndexType like Geo2D).
   */
  private[api] def optionDocument: pack.Document

  @deprecated("Internal: will be made private", "0.19.0")
  def partialFilter: Option[BSONDocument] = partialFilterDocument.flatMap {
    pack.bsonValue(_) match {
      case doc: BSONDocument =>
        Some(doc)

      case _ =>
        None
    }
  }

  @deprecated("Internal: will be made private", "0.19.0")
  def options: BSONDocument = pack.bsonValue(optionDocument) match {
    case doc: BSONDocument => doc
    case _                 => BSONDocument.empty
  }

  /** The name of the index (a default one is computed if none). */
  lazy val eventualName: String = name.getOrElse(key.foldLeft("") {
    (name, kv) =>
      name + (if (name.length > 0) "_" else "") + kv._1 + "_" + kv._2.valueStr
  })

  @deprecated("No longer a ReactiveMongo case class", "0.19.1")
  val productArity = 9

  def productElement(n: Int): Any = tupled.productElement(n)

  @silent(".*dropDups.*")
  private[api] lazy val tupled = Tuple9(key, name, unique, background, dropDups, sparse, version, partialFilterDocument, optionDocument)

  override def canEqual(that: Any): Boolean = that match {
    case _: Index => true
    case _        => false
  }

  // TODO#1.1: Review after deprecation cleanup (include all fields)
  override def equals(that: Any): Boolean = that match {
    case other: Index => tupled == other.tupled
    case _            => false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"Index${tupled.toString}"
}

object Index extends scala.runtime.AbstractFunction9[Seq[(String, IndexType)], Option[String], Boolean, Boolean, Boolean, Boolean, Option[Int], Option[BSONDocument], BSONDocument, Index] {
  import reactivemongo.api.BSONSerializationPack

  type Aux[P] = Index { type Pack = P }

  type Default = Aux[Serialization.Pack]

  @deprecated("Use constructor with pack parameter", "0.19.1")
  def apply(
    key: Seq[(String, IndexType)],
    name: Option[String] = None,
    unique: Boolean = false,
    background: Boolean = false,
    @deprecated("Since MongoDB 2.6", "0.19.1") dropDups: Boolean = false,
    sparse: Boolean = false,
    version: Option[Int] = None, // let MongoDB decide
    @deprecated("Internal: will be made private", "0.19.0") partialFilter: Option[BSONDocument] = None,
    @deprecated("Internal: will be made private", "0.19.0") options: BSONDocument = BSONDocument.empty): Index = apply(BSONSerializationPack)(key, name, unique, background, dropDups, sparse, None, None, None, None, None, None, None, None, None, None, None, None, None, version, partialFilter, options)

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
   *   dropDups = false,
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
    @deprecated("Since MongoDB 2.6", "0.19.1") dropDups: Boolean,
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
    @silent def d = dropDups
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
      override val dropDups = d
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
      val partialFilterDocument = pf
      val optionDocument = o
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
      dropDups = false,
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

  @deprecated("No longer a ReactiveMongo case class", "0.19.1")
  def unapply(index: Index): Option[Tuple9[Seq[(String, IndexType)], Option[String], Boolean, Boolean, Boolean, Boolean, Option[Int], Option[BSONDocument], BSONDocument]] = Option(index).map { i =>
    Tuple9(i.key, i.name, i.unique, i.background, i.dropDups,
      i.sparse, i.version, i.partialFilter, i.options)
  }

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
class NSIndex extends Product2[String, Index] with Serializable {
  type Pack <: SerializationPack

  def namespace: String = ???

  private[api] def idx: Index.Aux[Pack] = ??? // TODO: Remove

  // TODO: ?
  def index: Index = idx

  lazy val (dbName: String, collectionName: String) = {
    val spanned = namespace.span(_ != '.')
    spanned._1 -> spanned._2.drop(1)
  }

  @inline def _1 = namespace

  @inline def _2 = index

  def canEqual(that: Any): Boolean = that match {
    case _: NSIndex =>
      true

    case _ =>
      false
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

// TODO: Remove inheritance
object NSIndex extends scala.runtime.AbstractFunction2[String, Index.Aux[Serialization.Pack], NSIndex { type Pack = Serialization.Pack }] {
  type Aux[P <: SerializationPack] = NSIndex { type Pack = P }

  type Default = NSIndex.Aux[Serialization.Pack]

  @deprecated("Will be removed", "0.20.3")
  def apply(namespace: String, index: Index.Aux[Serialization.Pack]): NSIndex.Aux[Serialization.Pack] = at[Serialization.Pack](namespace, index)

  @deprecated("Will be renamed", "0.20.3")
  def at[P <: SerializationPack](
    namespace: String, index: Index.Aux[P]): NSIndex.Aux[P] = {
    @inline def nsp = namespace
    @inline def i = index
    new NSIndex {
      type Pack = P
      override val namespace = nsp
      override val idx = i
    }
  }

  def unapply(nsIndex: NSIndex): Option[(String, Index)] =
    Option(nsIndex).map(_.tupled)
}
