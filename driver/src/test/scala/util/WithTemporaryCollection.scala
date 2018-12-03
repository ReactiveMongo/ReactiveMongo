package util

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration.FiniteDuration

import scala.util.Random

import reactivemongo.api.{ Collection, CollectionMetaCommands, CollectionProducer, DefaultDB }
import tests.Common.timeout

object WithTemporaryCollection {

  def withTmpCollection[C <: Collection with CollectionMetaCommands, A](
    db: DefaultDB,
    timeout: FiniteDuration = tests.Common.timeout)(f: C => A)(implicit producer: CollectionProducer[C], ec: ExecutionContext): A = {
    val collectionName = s"tmp-${System identityHashCode this}-${Random.alphanumeric.take(10).mkString("")}"
    val collection = db[C](collectionName)
    // we won't drop the collection in case of exceptions, so that it can be debugged
    Await.ready(collection.create(), timeout)
    val result = f(collection)
    Await.ready(collection.drop(failIfNotFound = false), timeout)
    result
  }

}
