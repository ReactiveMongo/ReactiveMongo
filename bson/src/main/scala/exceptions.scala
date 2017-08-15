/*
 * Copyright 2013 Stephane Godbillon (@sgodbillon)
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
package reactivemongo.bson.exceptions

import scala.util.control.NoStackTrace

case class DocumentKeyNotFound(name: String) extends Exception {
  override def getMessage = s"The key '$name' could not be found in this document or array"
}

case class TypeDoesNotMatch(message: String)
  extends Exception with NoStackTrace {
  override val getMessage = message
}
