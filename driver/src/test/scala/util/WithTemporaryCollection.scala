package util

import scala.util.Random

import reactivemongo.api.{ Collection, CollectionMetaCommands, CollectionProducer }
import tests.Common.timeout

trait WithTemporaryCollection extends WithTemporaryDb { this: org.specs2.mutable.Specification =>

  protected final def withTmpCollection[C <: Collection with CollectionMetaCommands, A](f: C => A)(implicit producer: CollectionProducer[C]): A = {
    val collectionName = s"tmp-${System identityHashCode this}-${Random.alphanumeric.take(10).mkString("")}"
    val collection = db[C](collectionName)
    scala.concurrent.Await.result(collection.create(), timeout)
    f(collection)
    // no need to drop the collection, as the DB will itself be dropped after the test suite
  }

}
