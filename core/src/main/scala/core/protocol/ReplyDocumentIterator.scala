package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.ByteBuf

import reactivemongo.api.SerializationPack

import reactivemongo.api.bson.collection.BSONSerializationPack

private[reactivemongo] object ReplyDocumentIterator
  extends ReplyDocumentIteratorLowPriority {

  // BSON optimized parse alternative
  def parse[A](pack: BSONSerializationPack.type)(response: Response)(implicit reader: pack.Reader[A]): Iterator[A] = response match {
    case Response.CommandError(_, _, _, cause) =>
      new Iterator[A] { // TODO: Failing iterator
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

          def docs = parseDocuments[BSONSerializationPack.type, A](pack)(buf)

          //@com.github.ghik.silencer.silent(".*SerializationPack\\ is\\ deprecated.*")
          val firstBatch = preloaded.iterator.map { bson =>
            pack.deserialize(bson, reader)
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

    case _ => parseDocuments[BSONSerializationPack.type, A](
      pack)(response.documents)
  }

  private[core] def parseDocuments[P <: SerializationPack, A](pack: P)(buffer: ByteBuf)(implicit reader: pack.Reader[A]): Iterator[A] = new Iterator[A] {
    override val isTraversableAgain = false // TODO: Add test
    override def hasNext = buffer.isReadable()

    //@com.github.ghik.silencer.silent(".*SerializationPack\\ is\\ deprecated.*")
    override def next = try {
      val sz = buffer.getIntLE(buffer.readerIndex)
      val cbrb = reactivemongo.api.bson.buffer.
        ReadableBuffer(buffer readSlice sz)

      pack.readAndDeserialize(cbrb, reader)
    } catch {
      case e: IndexOutOfBoundsException =>
        /*
         * If this happens, the buffer is exhausted,
         * and there is probably a bug.
         *
         * It may happen if an enumerator relying on
         * it is concurrently applied to many iteratees
         * – which should not be done!
         */
        throw new ReplyDocumentIteratorExhaustedException(e)
    }
  }
}

private[reactivemongo] sealed trait ReplyDocumentIteratorLowPriority {
  _: ReplyDocumentIterator.type =>

  def parse[P <: SerializationPack, A](pack: P)(response: Response)(implicit reader: pack.Reader[A]): Iterator[A] = response match {
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

          val firstBatch = preloaded.iterator.map {
            case pack.IsDocument(bson) => pack.deserialize(bson, reader)
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
}

private[reactivemongo] final class ReplyDocumentIteratorExhaustedException(
  val cause: Exception) extends Exception(cause) {

  override def equals(that: Any): Boolean = that match {
    case other: ReplyDocumentIteratorExhaustedException =>
      (this.cause == null && other.cause == null) || (
        this.cause != null && this.cause.equals(other.cause))

    case _ =>
      false
  }

  override def hashCode: Int = if (cause == null) -1 else cause.hashCode
}
