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
package reactivemongo.bson.utils

/** Common functions */
object Converters {
  private val HEX_CHARS: Array[Char] = "0123456789abcdef".toCharArray

  /** Turns an array of Byte into a String representation in hexadecimal. */
  def hex2Str(bytes: Array[Byte]): String = {
    val hex = new Array[Char](2 * bytes.length)
    var i = 0
    while (i < bytes.length) {
      hex(2 * i) = HEX_CHARS((bytes(i) & 0xF0) >>> 4)
      hex(2 * i + 1) = HEX_CHARS(bytes(i) & 0x0F)
      i = i + 1
    }
    new String(hex)
  }

  /** Turns a hexadecimal String into an array of Byte. */
  def str2Hex(str: String): Array[Byte] = {
    val bytes = new Array[Byte](str.length / 2)
    var i = 0
    while (i < bytes.length) {
      bytes(i) = Integer.parseInt(str.substring(2 * i, 2 * i + 2), 16).toByte
      i += 1
    }
    bytes
  }

  /**
   * Returns the MD5 hash for the given UTF-8 `string`,
   * and turns it into a hexadecimal String representation.
   */
  @deprecated("Use `md5Hex` with explicit encoding", "0.12.0")
  def md5Hex(string: String): String = md5Hex(string, "UTF-8")

  /**
   * Returns the MD5 hash for the given `string`,
   * and turns it into a hexadecimal String representation.
   *
   * @param string the string to be hashed
   * @param encoding the string encoding/charset
   */
  def md5Hex(string: String, encoding: String): String =
    hex2Str(md5(string, encoding))

  /** Returns the MD5 hash of the given UTF-8 `string`. */
  @deprecated("Use `md5` with explicit encoding", "0.12.0")
  def md5(string: String): Array[Byte] = md5(string, "UTF-8")

  /**
   * Returns the MD5 hash of the given `string`.
   *
   * @param string the string to be hashed
   * @param encoding the string encoding/charset
   */
  def md5(string: String, encoding: String): Array[Byte] =
    md5(string.getBytes(encoding))

  /** Computes the MD5 hash of the given `bytes`. */
  def md5(bytes: Array[Byte]): Array[Byte] =
    java.security.MessageDigest.getInstance("MD5").digest(bytes)

}
