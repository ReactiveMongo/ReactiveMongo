package reactivemongo

import reactivemongo.core.errors.CommandException
import reactivemongo.core.protocol.{
  MessageHeader,
  Reply,
  Response,
  ResponseInfo
}

import reactivemongo.api.bson.collection.BSONSerializationPack

final class BSONSerializationPackSpec extends org.specs2.mutable.Specification {
  "BSON serialization pack".title

  "Response" should {
    "be read and deserialized" >> {
      def responseInfo = new ResponseInfo(
        reactivemongo.io.netty.channel.DefaultChannelId.newInstance()
      )

      "when CommandError" in {
        val msgHdr = new MessageHeader(
          messageLength = 0,
          requestID = 1,
          responseTo = 2,
          opCode = -1
        )

        val reply =
          Reply(flags = 0, cursorID = 0, startingFrom = 0, numberReturned = 0)

        val cmdErr = Response.CommandError(
          msgHdr,
          reply,
          responseInfo,
          CommandException(BSONSerializationPack)("Foo", None, None)
        )

        BSONSerializationPack.readAndDeserialize(
          response = cmdErr,
          reader = BSONSerializationPack.IdentityReader
        )

        ok
      }
    }
  }
}
