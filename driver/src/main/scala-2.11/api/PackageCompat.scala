package reactivemongo.api

import scala.concurrent.ExecutionContext

import scala.language.implicitConversions

import reactivemongo.api.bson.{ compat, BSONDocument }

import reactivemongo.bson.{ BSONDocument => LegacyDocument }

import play.api.libs.iteratee.{ Enumeratee, Iteratee }

private[api] trait PackageCompat {
  @deprecated("Use reactivemongo-bson-api", "0.19.0") /*implicit */
  implicit def toDocumentIteratorIteratee[T](it: Iteratee[Iterator[LegacyDocument], T])(implicit ec: ExecutionContext): Iteratee[Iterator[BSONDocument], T] =
    Enumeratee.map[Iterator[BSONDocument]](
      _.map(compat.fromDocument)).transform(it)

  @deprecated("Use reactivemongo-bson-api", "0.19.0") /*implicit */
  implicit def toDocumentIteratee[T](it: Iteratee[LegacyDocument, T])(implicit ec: ExecutionContext): Iteratee[BSONDocument, T] =
    Enumeratee.map[BSONDocument](compat.fromDocument).transform(it)

}
