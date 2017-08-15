package reactivemongo.api.indexes

import reactivemongo.bson.BSONDocument

/**
 * A MongoDB index (excluding the namespace).
 *
 * Consider reading [[http://www.mongodb.org/display/DOCS/Indexes the documentation about indexes in MongoDB]].
 *
 * @param key The index key (it can be composed of multiple fields). This list should not be empty!
 * @param name The name of this index. If you provide none, a name will be computed for you.
 * @param unique Enforces uniqueness.
 * @param background States if this index should be built in background. You should read [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-background%3Atrue the documentation about background indexing]] before using it.
 * @param dropDups States if duplicates should be discarded (if unique = true). Warning: you should read [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-dropDups%3Atrue the documentation]].
 * @param sparse States if the index to build should only consider the documents that have the indexed fields. See [[http://www.mongodb.org/display/DOCS/Indexes#Indexes-sparse%3Atrue the documentation]] on the consequences of such an index.
 * @param version Indicates the [[http://www.mongodb.org/display/DOCS/Index+Versions version]] of the index (1 for >= 2.0, else 0). You should let MongoDB decide.
 * @param options Optional parameters for this index (typically specific to an IndexType like Geo2D).
 * @param partialFilter Optional [[https://docs.mongodb.com/manual/core/index-partial/#partial-index-with-unique-constraints partial filter]] (since MongoDB 3.2)
 */
case class Index(
  key: Seq[(String, IndexType)],
  name: Option[String] = None,
  unique: Boolean = false,
  background: Boolean = false,
  dropDups: Boolean = false, // Deprecated since 2.6, TODO: Remove
  sparse: Boolean = false,
  version: Option[Int] = None, // let MongoDB decide
  // TODO: storageEngine (new for Mongo3)
  partialFilter: Option[BSONDocument] = None,
  options: BSONDocument = BSONDocument()) {

  /** The name of the index (a default one is computed if none). */
  lazy val eventualName: String = name.getOrElse(key.foldLeft("") { (name, kv) =>
    name + (if (name.length > 0) "_" else "") + kv._1 + "_" + kv._2.valueStr
  })
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
case class NSIndex(namespace: String, index: Index) {
  val (dbName: String, collectionName: String) = {
    val spanned = namespace.span(_ != '.')
    spanned._1 -> spanned._2.drop(1)
  }
}
