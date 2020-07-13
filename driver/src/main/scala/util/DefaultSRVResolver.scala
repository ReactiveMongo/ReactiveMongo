/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.util

import scala.concurrent.{ ExecutionContext, Future }

import org.xbill.DNS.{ Name, Record, SRVRecord }

import reactivemongo.core.errors.GenericDriverException

/**
 * @param srvPrefix the SRV prefix (default: `_mongodb._tcp`)
 */
private[util] final class DefaultSRVResolver(
  resolve: SRVRecordResolver)(
  implicit
  ec: ExecutionContext) extends (String => Future[List[SRV]]) {

  def apply(name: String): Future[List[SRV]] = {
    val baseName = Name.fromString(
      name.dropWhile(_ != '.').drop(1), Name.root)

    @annotation.tailrec
    def go(records: Array[Record], names: List[SRV]): Future[List[SRV]] = {
      // Common checks and transformation whatever is the `resolve`
      records.headOption match {
        case Some(rec: SRVRecord) => {
          val nme = rec.getAdditionalName

          if (nme.isAbsolute) {
            if (!nme.subdomain(baseName)) {
              Future.failed[List[SRV]](new GenericDriverException(
                s"$nme is not subdomain of $baseName"))

            } else {
              go(records.tail, (nme.toString(true) -> rec.getPort) :: names)
            }
          } else {
            go(
              records.tail,
              (Name.concatenate(
                nme, baseName).toString(true) -> rec.getPort) :: names)
          }
        }

        case Some(rec) => Future.failed[List[SRV]](
          new GenericDriverException(s"Unexpected record: $rec"))

        case _ => Future.successful(names.reverse)
      }
    }

    // ---

    resolve(ec)(name).flatMap {
      case null =>
        Future.successful(List.empty[SRV])

      case records =>
        go(records, List.empty)
    }
  }
}
