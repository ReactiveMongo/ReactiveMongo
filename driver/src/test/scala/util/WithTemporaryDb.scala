package util

import org.specs2.concurrent.ExecutionEnv

import tests.Common

trait WithTemporaryDb extends org.specs2.specification.AfterAll {
  this: org.specs2.mutable.Specification =>

  protected implicit def ee: ExecutionEnv

  protected final lazy val (db, slowDb) = Common.databases(
    s"reactivemongo-tmp-${System identityHashCode this}",
    Common.connection,
    Common.slowConnection
  )

  def afterAll(): Unit = {
    db.drop()
    ()
  }

}
