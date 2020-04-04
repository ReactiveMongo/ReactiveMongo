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
package reactivemongo.api.collections

import reactivemongo.api.{ ReadPreference, SerializationPack }

private[reactivemongo] object QueryCodecs {
  @inline def writeReadPref[P <: SerializationPack](pack: P): ReadPreference => pack.Document = writeReadPref[pack.type](pack.newBuilder)

  def writeReadPref[P <: SerializationPack](builder: SerializationPack.Builder[P]): ReadPreference => builder.pack.Document =
    { readPreference: ReadPreference =>
      import builder.{ elementProducer => element, document, string }

      val mode = readPreference match {
        case ReadPreference.Primary               => "primary"
        case ReadPreference.PrimaryPreferred(_)   => "primaryPreferred"
        case ReadPreference.Secondary(_)          => "secondary"
        case ReadPreference.SecondaryPreferred(_) => "secondaryPreferred"
        case ReadPreference.Nearest(_)            => "nearest"
      }
      val elements = Seq.newBuilder[builder.pack.ElementProducer]

      elements += element("mode", string(mode))

      readPreference match {
        case ReadPreference.Taggable(first :: tagSet) if tagSet.nonEmpty => {
          val head = document(first.toSeq.map {
            case (k, v) => element(k, string(v))
          })

          elements += element("tags", builder.array(
            head,
            tagSet.toSeq.map { tags =>
              document(tags.toSeq.map {
                case (k, v) => element(k, string(v))
              })
            }))
        }

        case _ => ()
      }

      document(elements.result())
    }
}
