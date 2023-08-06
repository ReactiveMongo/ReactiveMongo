package reactivemongo.actors

import org.apache.pekko.util.{
  ByteString => PekkoByteString,
  Timeout => PekkoTimeout
}

package object util {
  type Timeout = PekkoTimeout
  val Timeout = PekkoTimeout

  type ByteString = PekkoByteString
  val ByteString = PekkoByteString

}
