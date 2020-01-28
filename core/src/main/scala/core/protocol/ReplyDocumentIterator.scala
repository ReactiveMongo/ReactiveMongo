package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.ByteBuf

import reactivemongo.core.netty.ChannelBufferReadableBuffer

import reactivemongo.api.SerializationPack

object ReplyDocumentIterator {
  private[reactivemongo] def parse[P <: SerializationPack, A](pack: P)(response: Response)(implicit reader: pack.Reader[A]): Iterator[A] = response match {
    case Response.CommandError(_, _, _, cause) =>
      new Iterator[A] {
        val hasNext = false
        @inline def next: A = throw cause
        //throw ReplyDocumentIteratorExhaustedException(cause)
      }

    case Response.WithCursor(_, _, _, _, _, _, preloaded) => {
      val buf = response.documents

      if (buf.readableBytes == 0) {
        Iterator.empty
      } else {
        try {
          buf.skipBytes(buf.getIntLE(buf.readerIndex))

          def docs = parseDocuments[P, A](pack)(buf)

          @com.github.ghik.silencer.silent(".*SerializationPack\\ is\\ deprecated.*")
          val firstBatch = preloaded.iterator.map { bson =>
            pack.deserialize(pack.document(bson), reader)
          }

          firstBatch ++ docs
        } catch {
          case cause: Exception => new Iterator[A] {
            val hasNext = false
            @inline def next: A = throw cause
            //throw ReplyDocumentIteratorExhaustedException(cause)
          }
        }
      }
    }

    case _ => parseDocuments[P, A](pack)(response.documents)
  }

  @deprecated("Use `parseDocuments`", "0.14.0")
  def apply[P <: SerializationPack, A](pack: P)(reply: Reply, buffer: ByteBuf)(implicit reader: pack.Reader[A]): Iterator[A] = parseDocuments[P, A](pack)(buffer)

  private[core] def parseDocuments[P <: SerializationPack, A](pack: P)(buffer: ByteBuf)(implicit reader: pack.Reader[A]): Iterator[A] = new Iterator[A] {
    override val isTraversableAgain = false // TODO: Add test
    override def hasNext = buffer.isReadable()

    @com.github.ghik.silencer.silent(".*SerializationPack\\ is\\ deprecated.*")
    override def next = try {
      val sz = buffer.getIntLE(buffer.readerIndex)
      //val cbrb = ChannelBufferReadableBuffer(buffer readBytes sz)
      val cbrb = ChannelBufferReadableBuffer(buffer readSlice sz)

      pack.readAndDeserialize(cbrb, reader)
    } catch {
      case e: IndexOutOfBoundsException =>
        /*
         * If this happens, the buffer is exhausted, and there is probably a bug.
         * It may happen if an enumerator relying on it is concurrently applied to many iteratees – which should not be done!
         */
        throw ReplyDocumentIteratorExhaustedException(e)
    }
  }
}

@deprecated("Internal: will be made private", "0.20.3")
class ReplyDocumentIteratorExhaustedException private[core] (
  val cause: Exception) extends Exception(cause) with Product1[Exception] with Serializable {

  @inline def _1 = cause

  @deprecated("No longer case class", "0.20.3")
  def canEqual(that: Any): Boolean = that match {
    case _: ReplyDocumentIteratorExhaustedException =>
      true

    case _ =>
      false
  }

  override def equals(that: Any): Boolean = that match {
    case other: ReplyDocumentIteratorExhaustedException =>
      (this.cause == null && other.cause == null) || (
        this.cause != null && this.cause.equals(other.cause))

    case _ =>
      false
  }

  override def hashCode: Int = if (cause == null) -1 else cause.hashCode
}

@deprecated("Internal: will be made private", "0.20.3")
object ReplyDocumentIteratorExhaustedException extends scala.runtime.AbstractFunction1[Exception, ReplyDocumentIteratorExhaustedException] {
  def apply(cause: Exception): ReplyDocumentIteratorExhaustedException =
    new ReplyDocumentIteratorExhaustedException(cause)

  @deprecated("No longer case class", "0.20.3")
  def unapply(exception: ReplyDocumentIteratorExhaustedException): Option[Exception] = Option(exception).map(_.cause)
}
