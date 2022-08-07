package util

import reactivemongo.api.DB
import reactivemongo.core.protocol.MongoWireVersion

import org.specs2.execute.{ AsResult, Result }

import tests.Common

trait MongoSkips { this: org.specs2.mutable.Specification =>

  final def skippedIf[R](
      predicates: Option[String]*
    )(r: => R
    )(implicit
      R: AsResult[R]
    ): Result = {
    predicates.flatten.headOption match {
      case None         => R.asResult(r)
      case Some(reason) => skipped(reason)
    }
  }

  final def isNotReplicaSet: Option[String] = {
    if (Common.replSetOn) None
    else
      Some(
        "untestable because the target mongo server is not within a Replica Set"
      )
  }

  final def isNotAtLeast(db: DB, version: MongoWireVersion): Option[String] = {
    val mongoVersion = reactivemongo.api.tests.maxWireVersion(db)

    if (mongoVersion >= version) None
    else
      Some(s"untestable because the target mongo server has version $mongoVersion, which is smaller than $version required for this test")
  }
}
