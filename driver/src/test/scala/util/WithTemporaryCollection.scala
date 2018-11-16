package util

import scala.concurrent.Await
import scala.util.Random

import org.specs2.concurrent.ExecutionEnv
import reactivemongo.api.{ Collection, CollectionMetaCommands, CollectionProducer, DefaultDB }
import tests.Common.timeout

trait WithTemporaryCollection { this: org.specs2.mutable.Specification =>

  protected implicit def ee: ExecutionEnv

  protected final def withTmpCollection[C <: Collection with CollectionMetaCommands, A](
    db: DefaultDB)(
    f: C => A)(implicit producer: CollectionProducer[C]): A = {
    val collectionName = s"tmp-${System identityHashCode this}-${Random.alphanumeric.take(10).mkString("")}"
    val collection = db[C](collectionName)
    // we won't drop the collection in case of exceptions, so that it can be debugged
    Await.ready(collection.create(), timeout)
    val result = f(collection)
    Await.ready(collection.drop(failIfNotFound = false), timeout)
    result
  }

}
